import logging
import re
import sqlite3
import sys

from argparse import ArgumentParser
from rdflib import Graph
from .helpers import add_labels, get_connection, get_ids, get_terms

"""
Usage: python3 extract.py -d <sqlite-database> -t <curie> > <ttl-file>

Creates a TTL file containing the term, predicates, and ancestors. TTL is written to stdout.
You can include more than one `-t <curie>`/`--term <curie>`.

You may also specify multiple CURIEs to extract with `-T <file>`/`--terms <file>`
where the file contains a list of CURIEs to extract.

You may also specify which predicates you would like to include with
`-p <curie>`/`--predicate <curie>` or `-P <file>`/`--predicates <file>`
where the file contains a list of predicate CURIEs.

Finally, if you don't wish to include the ancestors of the term/terms,
include the `-n`/`--no-hierarchy` flag.

The sqlite-database must be created by RDFTab (https://github.com/ontodev/rdftab.rs)
and include 'statements' and 'prefixes' tables.

The CURIEs must use a prefix from the 'prefixes' table.
"""


def main():
    p = ArgumentParser()
    p.add_argument(
        "-d", "--database", required=True, help="Database file (.db) or configuration (.ini)"
    )
    p.add_argument("-t", "--term", action="append", help="CURIE or label of term to extract")
    p.add_argument(
        "-T", "--terms", help="File containing CURIES or labels of terms to extract",
    )
    p.add_argument(
        "-p", "--predicate", action="append", help="CURIE or label of predicate to include",
    )
    p.add_argument(
        "-P", "--predicates", help="File containing CURIEs or labels of predicates to include",
    )
    p.add_argument("-f", "--format", help="", default="ttl")
    p.add_argument(
        "-n",
        "--no-hierarchy",
        action="store_true",
        help="If provided, do not create any rdfs:subClassOf statements",
    )
    args = p.parse_args()
    sys.stdout.write(extract(args))


def extract(args):
    # Get required terms
    terms = get_terms(args.term, args.terms)
    if not terms:
        logging.critical("One or more term(s) must be specified with --term or --terms")
        sys.exit(1)

    # Maybe get predicates to include
    predicates = get_terms(args.predicate, args.predicates)
    conn = get_connection(args.database)
    try:
        return extract_terms(
            conn, terms, predicates, fmt=args.format, no_hierarchy=args.no_hierarchy
        )
    finally:
        clean(conn)


def clean(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tmp_labels")
    cur.execute("DROP TABLE IF EXISTS tmp_terms")
    cur.execute("DROP TABLE IF EXISTS tmp_predicates")
    cur.execute("DROP TABLE IF EXISTS tmp_extract")
    conn.commit()


def escape(curie):
    """Escape illegal characters in the local ID portion of a CURIE"""
    prefix = curie.split(":")[0]
    local_id = curie.split(":")[1]
    local_id_fixed = re.sub(r"(?<!\\)([~!$&'()*+,;=/?#@%])", r"\\\1", local_id)
    return f"{prefix}:{local_id_fixed}"


def escape_qnames(cur):
    """Update CURIEs with illegal QName characters in the local ID by escaping those characters."""
    for keyword in ["stanza", "subject", "predicate", "object"]:
        cur.execute(
            f"""SELECT DISTINCT {keyword} FROM tmp_extract
                WHERE {keyword} NOT LIKE '<%>' AND {keyword} NOT LIKE '_:%'"""
        )
        for row in cur.fetchall():
            curie = row[0]
            escaped = escape(curie)
            if curie != escaped:
                cur.execute(
                    f"UPDATE tmp_extract SET {keyword} = '{escaped}' WHERE {keyword} = '{curie}'"
                )


def extract_terms(conn, terms, predicates, fmt="ttl", no_hierarchy=False):
    """Extract terms from the ontology database and return the module as Turtle or JSON-LD."""
    if fmt.lower() not in ["ttl", "json-ld"]:
        raise Exception("Unknown format: " + fmt)

    # Create a new table (extract) and copy the triples we care about
    # Then write the triples from that table to the output file
    cur = conn.cursor()

    # Pre-clean up
    clean(conn)

    # Create a temp labels table
    add_labels(cur)

    term_ids = get_ids(cur, terms)

    predicate_ids = None
    if predicates:
        # Current predicates are IDs or labels - make sure we get all the IDs
        predicate_ids = get_ids(cur, predicates)

    # Create the terms table containing parent -> child relationships
    cur.execute("CREATE TABLE tmp_terms(parent TEXT NOT NULL, child TEXT)")
    for term_id in term_ids:
        cur.execute(f"INSERT INTO tmp_terms VALUES ('{term_id}', NULL)")

    # Create tmp predicates table
    cur.execute("CREATE TABLE tmp_predicates(predicate TEXT PRIMARY KEY NOT NULL)")
    if predicate_ids:
        for predicate_id in predicate_ids:
            if isinstance(cur, sqlite3.Cursor):
                cur.execute(f"INSERT OR IGNORE INTO tmp_predicates VALUES ('{predicate_id}')")
            else:
                cur.execute(
                    f"""INSERT INTO tmp_predicates VALUES ('{predicate_id}')
                        ON CONFLICT (predicate) DO NOTHING"""
                )
    else:
        # Insert all predicates
        if isinstance(cur, sqlite3.Cursor):
            cur.execute(
                """INSERT OR IGNORE INTO tmp_predicates
                SELECT DISTINCT predicate
                FROM statements WHERE predicate NOT IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')"""
            )
        else:
            cur.execute(
                """INSERT INTO tmp_predicates
                SELECT DISTINCT predicate
                FROM statements WHERE predicate NOT IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                ON CONFLICT (predicate) DO NOTHING"""
            )

    if not no_hierarchy:
        # Add subclasses & subproperties
        cur.execute(
            """WITH RECURSIVE ancestors(parent, child) AS (
                SELECT * FROM tmp_terms
                UNION
                SELECT object AS parent, subject AS child
                FROM statements, ancestors
                WHERE ancestors.parent = statements.stanza
                  AND statements.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                  AND statements.object NOT LIKE '_:%'
              )
              INSERT INTO tmp_terms
              SELECT parent, child FROM ancestors"""
        )

    cur.execute(
        """CREATE TABLE tmp_extract(
             stanza TEXT,
             subject TEXT,
             predicate TEXT,
             object TEXT,
             value TEXT,
             datatype TEXT,
             language TEXT
           )"""
    )

    # Insert rdf:type declarations
    cur.execute(
        """INSERT INTO tmp_extract
        SELECT * FROM statements
        WHERE subject IN (SELECT DISTINCT parent FROM tmp_terms) AND predicate = 'rdf:type'"""
    )

    # Insert subclass statements:
    # - parent is a class if it's used in subclass statement
    # - this allows us to get around undeclared classes, e.g. owl:Thing
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT t.child, t.child, 'rdfs:subClassOf', t.parent
        FROM tmp_terms t
        JOIN statements s ON t.parent = s.object
        WHERE t.child IS NOT NULL AND s.predicate = 'rdfs:subClassOf'"""
    )

    # Insert subproperty statements (same as above)
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT t.child, t.child, 'rdfs:subPropertyOf', t.parent
        FROM tmp_terms t
        JOIN statements s ON t.parent = s.object
        WHERE t.child IS NOT NULL AND s.predicate = 'rdfs:subPropertyOf'"""
    )

    # Insert literal annotations
    cur.execute(
        """INSERT INTO tmp_extract
        SELECT *
        FROM statements
        WHERE subject IN (SELECT DISTINCT parent FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND value IS NOT NULL"""
    )

    # Insert logical relationships (object must be in set of input terms)
    cur.execute(
        """INSERT INTO tmp_extract
        SELECT * FROM statements
        WHERE subject IN (SELECT DISTINCT parent FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND object IN (SELECT DISTINCT parent FROM tmp_terms)"""
    )

    # Insert IRI annotations (object does not have to be in input terms)
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT s1.stanza, s1.subject, s1.predicate, s1.object
        FROM statements s1
        JOIN statements s2 ON s1.predicate = s2.subject
        WHERE s1.subject IN (SELECT DISTINCT parent FROM tmp_terms)
          AND s1.predicate IN (SELECT predicate FROM tmp_predicates)
          AND s2.object = 'owl:AnnotationProperty'
          AND s1.object IS NOT NULL"""
    )

    # Escape QNames
    escape_qnames(cur)

    ttl = "\n".join(get_ttl(cur))
    if fmt.lower() == "ttl":
        return ttl

    # Otherwise the format is JSON
    graph = Graph()
    graph.parse(data=ttl, format="turtle")

    # Create the context with prefixes
    cur.execute("SELECT DISTINCT prefix, base FROM prefix;")
    context = {}
    for row in cur.fetchall():
        context[row[0]] = {"@id": row[1], "@type": "@id"}
    return graph.serialize(format="json-ld", context=context, indent=4).decode("utf-8")


def get_ttl(cur):
    """Get the 'extract' table as lines of Turtle (the lines are returned as a list)."""
    # Get ttl lines
    cur.execute(
        """WITH literal(value, escaped) AS (
              SELECT DISTINCT
                value,
                replace(replace(replace(value, '\\', '\\\\'), '"', '\\"'), '
            ', '\\n') AS escaped
              FROM tmp_extract
            )
            SELECT
              '@prefix ' || prefix || ': <' || base || '> .'
            FROM prefix
            UNION ALL
            SELECT DISTINCT
               subject
            || ' '
            || predicate
            || ' '
            || coalesce(
                 object,
                 '"' || escaped || '"^^' || datatype,
                 '"' || escaped || '"@' || language,
                 '"' || escaped || '"'
               )
            || ' .'
            FROM tmp_extract LEFT JOIN literal ON tmp_extract.value = literal.value;"""
    )
    lines = []
    for row in cur.fetchall():
        line = row[0]
        if not line:
            continue
        # Replace newlines
        line = line.replace("\n", "\\n")
        lines.append(line)

    return lines


if __name__ == "__main__":
    main()
