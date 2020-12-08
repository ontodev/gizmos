import logging
import re
import sqlite3
import sys

from argparse import ArgumentParser
from rdflib import Graph
from .helpers import add_labels, dict_factory, get_ids, get_terms

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
    p.add_argument("-d", "--database", required=True, help="SQLite database")
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
    return extract_terms(
        args.database, terms, predicates, fmt=args.format, no_hierarchy=args.no_hierarchy
    )


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
            f"""
            UPDATE tmp.extract SET {keyword} = esc({keyword})
            WHERE {keyword} IN
              (SELECT DISTINCT {keyword}
               FROM tmp.extract WHERE {keyword} NOT LIKE '<%>' AND {keyword} NOT LIKE '_:%');"""
        )


def extract_terms(database, terms, predicates, fmt="ttl", no_hierarchy=False):
    """Extract terms from the ontology database and return the module as Turtle or JSON-LD."""
    if fmt.lower() not in ["ttl", "json-ld"]:
        raise Exception("Unknown format: " + fmt)

    # Create a new table (extract) and copy the triples we care about
    # Then write the triples from that table to the output file
    with sqlite3.connect(database) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()

        # Create a temp labels table
        cur.execute("ATTACH DATABASE '' AS tmp")
        add_labels(cur)

        term_ids = get_ids(cur, terms)

        predicate_ids = None
        if predicates:
            # Current predicates are IDs or labels - make sure we get all the IDs
            predicate_ids = get_ids(cur, predicates)

        # Create the terms table containing parent -> child relationships
        cur.execute("CREATE TABLE tmp.terms(parent TEXT NOT NULL, child TEXT)")
        cur.executemany("INSERT INTO tmp.terms VALUES (?, NULL)", [(x,) for x in term_ids])

        # Create tmp predicates table
        cur.execute("CREATE TABLE tmp.predicates(predicate TEXT PRIMARY KEY NOT NULL)")
        if predicate_ids:
            cur.executemany(
                "INSERT OR IGNORE INTO tmp.predicates VALUES (?)", [(x,) for x in predicate_ids]
            )
        else:
            # Insert all predicates
            cur.execute(
                """INSERT OR IGNORE INTO tmp.predicates
                SELECT DISTINCT predicate
                FROM statements WHERE predicate NOT IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')"""
            )

        if not no_hierarchy:
            # Add subclasses & subproperties
            cur.execute(
                """WITH RECURSIVE ancestors(parent, child) AS (
                    SELECT * FROM terms
                    UNION
                    SELECT object AS parent, subject AS child
                    FROM statements, ancestors
                    WHERE ancestors.parent = statements.stanza
                      AND statements.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                      AND statements.object NOT LIKE '_:%'
                  )
                  INSERT INTO tmp.terms
                  SELECT parent, child FROM ancestors"""
            )

        cur.execute(
            """CREATE TABLE tmp.extract(
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
            """INSERT INTO tmp.extract
            SELECT * FROM statements
            WHERE subject IN (SELECT DISTINCT parent FROM terms) AND predicate = 'rdf:type'"""
        )

        # Insert subclass statements:
        # - parent is a class if it's used in subclass statement
        # - this allows us to get around undeclared classes, e.g. owl:Thing
        cur.execute(
            """INSERT INTO tmp.extract (stanza, subject, predicate, object)
            SELECT DISTINCT t.child, t.child, 'rdfs:subClassOf', t.parent
            FROM terms t
            JOIN statements s ON t.parent = s.object
            WHERE t.child IS NOT NULL AND s.predicate = 'rdfs:subClassOf'"""
        )

        # Insert subproperty statements (same as above)
        cur.execute(
            """INSERT INTO tmp.extract (stanza, subject, predicate, object)
            SELECT DISTINCT t.child, t.child, 'rdfs:subPropertyOf', t.parent
            FROM terms t
            JOIN statements s ON t.parent = s.object
            WHERE t.child IS NOT NULL AND s.predicate = 'rdfs:subPropertyOf'"""
        )

        # Insert literal annotations
        cur.execute(
            """INSERT INTO tmp.extract
            SELECT *
            FROM statements
            WHERE subject IN (SELECT DISTINCT parent FROM terms)
              AND predicate IN (SELECT predicate FROM predicates)
              AND value IS NOT NULL"""
        )

        # Insert logical relationships (object must be in set of input terms)
        cur.execute(
            """INSERT INTO tmp.extract
            SELECT * FROM statements
            WHERE subject IN (SELECT DISTINCT parent FROM terms)
              AND predicate IN (SELECT predicate FROM predicates)
              AND object IN (SELECT DISTINCT parent FROM terms)"""
        )

        # Insert IRI annotations (object does not have to be in input terms)
        cur.execute(
            """INSERT INTO tmp.extract (stanza, subject, predicate, object)
            SELECT s1.stanza, s1.subject, s1.predicate, s1.object
            FROM statements s1
            JOIN statements s2 ON s1.predicate = s2.subject
            WHERE s1.subject IN (SELECT DISTINCT parent FROM terms)
              AND s1.predicate IN (SELECT predicate FROM predicates)
              AND s2.object = 'owl:AnnotationProperty'
              AND s1.object NOT NULL"""
        )

        # Create esc function
        conn.create_function("esc", 1, escape)
        escape_qnames(cur)

        # Reset row factory
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
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
            context[row["prefix"]] = {"@id": row["base"], "@type": "@id"}
        return graph.serialize(format="json-ld", context=context, indent=4).decode("utf-8")


def get_ttl(cur):
    """Get the 'extract' table as lines of Turtle (the lines are returned as a list)."""
    # Get ttl lines
    cur.execute(
        '''WITH literal(value, escaped) AS (
              SELECT DISTINCT
                value,
                replace(replace(replace(value, '\\', '\\\\'), '"', '\\"'), '
            ', '\\n') AS escaped
              FROM extract
            )
            SELECT
              "@prefix " || prefix || ": <" || base || "> ."
            FROM prefix
            UNION ALL
            SELECT DISTINCT
               subject
            || " "
            || predicate
            || " "
            || coalesce(
                 object,
                 """" || escaped || """^^" || datatype,
                 """" || escaped || """@" || language,
                 """" || escaped || """"
               )
            || " ."
            FROM extract LEFT JOIN literal ON extract.value = literal.value;'''
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
