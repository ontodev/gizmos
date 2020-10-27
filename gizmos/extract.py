import logging
import re
import sqlite3
import sys

from argparse import ArgumentParser

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
    p.add_argument("-t", "--term", action="append", help="CURIE of term to extract")
    p.add_argument(
        "-T", "--terms", help="File containing CURIES of terms to extract",
    )
    p.add_argument(
        "-p", "--predicate", action="append", help="CURIE of predicate to include",
    )
    p.add_argument(
        "-P", "--predicates", help="File containing CURIEs of predicates to include",
    )
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
    terms = []
    if args.term:
        terms = args.term
    if args.terms:
        with open(args.terms, "r") as f:
            terms_from_file = [x.strip() for x in f.readlines()]
            terms.extend(terms_from_file)

    if not terms:
        logging.critical("One or more term(s) must be specified with --term or --terms")
        sys.exit(1)

    # Maybe get predicates to include
    predicate_ids = args.predicate or []
    if args.predicates:
        with open(args.predicates, "r") as f:
            predicate_ids.extend([x.strip() for x in f.readlines()])

    ttl = "\n".join(
        extract_terms(args.database, terms, predicate_ids, no_hierarchy=args.no_hierarchy)
    )
    return ttl


def dict_factory(cursor, row):
    """Create a dict factory for sqlite cursor"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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


def extract_terms(database, terms, predicate_ids, no_hierarchy=False):
    """Extract terms from the ontology database and return the module as lines of Turtle."""

    # Create a new table (extract) and copy the triples we care about
    # Then write the triples from that table to the output file
    with sqlite3.connect(database) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()

        cur.execute("ATTACH DATABASE '' AS tmp")

        # Create the extract table
        cur.execute("CREATE TABLE tmp.terms(parent TEXT NOT NULL, child TEXT)")
        cur.executemany("INSERT INTO tmp.terms VALUES (?, NULL)", [(x,) for x in terms])

        cur.execute("CREATE TABLE tmp.predicates(predicate TEXT PRIMARY KEY NOT NULL)")
        cur.execute("INSERT INTO tmp.predicates VALUES ('rdf:type')")
        if predicate_ids:
            cur.executemany(
                "INSERT OR IGNORE INTO tmp.predicates VALUES (?)", [(x,) for x in predicate_ids]
            )
        else:
            # Insert all predicates
            cur.execute(
                """
                    INSERT OR IGNORE INTO tmp.predicates
                    SELECT DISTINCT predicate
                    FROM statements"""
            )
        if not no_hierarchy:
            cur.execute(
                """
                  WITH RECURSIVE ancestors(parent, child) AS (
                    SELECT * FROM terms
                    UNION
                    SELECT object AS parent, subject AS child
                    FROM statements, ancestors
                    WHERE ancestors.parent = statements.stanza
                      AND statements.predicate = 'rdfs:subClassOf'
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
        cur.execute(
            """
                INSERT INTO tmp.extract (stanza, subject, predicate, object)
                SELECT DISTINCT child, child, 'rdfs:subClassOf', parent
                FROM terms WHERE child IS NOT NULL"""
        )
        cur.execute(
            """
                INSERT INTO tmp.extract
                SELECT *
                FROM statements
                WHERE subject IN (SELECT DISTINCT parent FROM terms)
                  AND predicate IN (SELECT predicate FROM predicates)"""
        )

        # Create esc function
        conn.create_function("esc", 1, escape)
        escape_qnames(cur)

        # Reset row factory
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        return get_ttl(cur)


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
