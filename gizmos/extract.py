import logging
import sqlite3
import sys

from argparse import ArgumentParser

"""
Usage: python3 extract.py -d <sqlite-database> -t <curie> > <ttl-file>

Creates a TTL file containing the term, annotations, and ancestors. TTL is written to stdout.
You can include more than one `-t <curie>`/`--term <curie>`.

You may also specify multiple CURIEs to extract with `-T <file>`/`--terms <file>`
where the file contains a list of CURIEs to extract.

You may also specify which annotations you would like to include with
`-a <curie>`/`--annotation <curie>` or `-A <file>`/`--annotations <file>`
where the file contains a list of annotation property CURIEs.

Finally, if you don't wish to include the ancestors of the term/terms,
include the `-n`/`--no-hierarchy` flag.

The sqlite-database must be created by RDFTab (https://github.com/ontodev/rdftab.rs)
and include 'statements' and 'prefixes' tables.

The CURIEs must use a prefix from the 'prefixes' table.
"""

# Track terms already added to database
added = []


def main():
    global added
    p = ArgumentParser()
    p.add_argument("-d", "--database", required=True, help="SQLite database")
    p.add_argument("-t", "--term", action="append", help="CURIE of term to extract")
    p.add_argument(
        "-T", "--terms", help="File containing CURIES of terms to extract",
    )
    p.add_argument(
        "-a",
        "--annotation",
        action="append",
        help="CURIE of annotation property to include",
    )
    p.add_argument(
        "-A",
        "--annotations",
        help="File containing CURIEs of annotation properties to include",
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

    # Get optional annotations (otherwise, all annotations are included)
    annotations = None
    if args.annotation:
        # One or more annotations to add
        annotations = args.annotation
    if args.annotations:
        with open(args.annotations, "r") as f:
            annotations = [x.strip() for x in f.readlines()]

    ttl = "\n".join(
        extract_terms(args.database, terms, annotations, no_hierarchy=args.no_hierarchy)
    )
    return ttl


def add_annotations(cur, annotations=None):
    """Add annotations from the 'statements' table on all subjects in the 'extract' table."""
    annotation_str = None
    if annotations:
        annotation_str = ["'" + x.replace("'", "''") + "'" for x in annotations]
        annotation_str = ", ".join(annotation_str)
    cur.execute("SELECT DISTINCT subject FROM extract;")
    for row in cur.fetchall():
        subject = row["subject"]
        query = f"""INSERT INTO extract (stanza, subject, predicate, value, language, datatype)
                    SELECT DISTINCT
                      subject AS stanza,
                      subject,
                      predicate,
                      value,
                      language,
                      datatype
                    FROM statements WHERE subject = '{subject}' AND value NOT NULL"""
        if annotation_str:
            query += f" AND predicate IN ({annotation_str})"
        cur.execute(query)


def add_ancestors(cur, term_id):
    """Add the hierarchy for a term ID starting with that term up to the top-level, assuming that
    term ID exists in the database."""
    global added
    cur.execute(
        f"""
          WITH RECURSIVE ancestors(parent, child) AS (
            VALUES ('{term_id}', NULL)
            UNION
            SELECT object AS parent, subject AS child
            FROM statements
            WHERE predicate = 'rdfs:subClassOf'
              AND object = '{term_id}'
            UNION
            SELECT object AS parent, subject AS child
            FROM statements, ancestors
            WHERE ancestors.parent = statements.stanza
              AND statements.predicate = 'rdfs:subClassOf'
              AND statements.object NOT LIKE '_:%'
          )
          SELECT * FROM ancestors;"""
    )

    for row in cur.fetchall():
        parent = row["parent"]
        if parent and parent not in added:
            # Only add rdf:type if it hasn't been added
            added.append(parent)
            cur.execute(
                f"""INSERT INTO extract (stanza, subject, predicate, object)
                        VALUES ('{parent}', '{parent}', 'rdf:type', 'owl:Class');"""
            )

        child = row["child"]
        if child and child not in added:
            # Only add rdf:type if it hasn't been added
            added.append(child)
            cur.execute(
                f"""INSERT INTO extract (stanza, subject, predicate, object)
                        VALUES ('{child}', '{child}', 'rdf:type', 'owl:Class');"""
            )

        if child and parent:
            # Row has child & parent, add subclass statement
            cur.execute(
                f"""INSERT INTO extract (stanza, subject, predicate, object)
                        VALUES ('{child}', '{child}', 'rdfs:subClassOf', '{parent}');"""
            )


def add_term(cur, term_id):
    """Add the class assertion for a term ID, assuming that term ID exists in the database."""
    cur.execute(f"SELECT * FROM statements WHERE subject = '{term_id}';")
    res = cur.fetchone()
    if res:
        cur.execute(
            f"""INSERT INTO extract (stanza, subject, predicate, object)
                    VALUES ('{term_id}', '{term_id}', 'rdf:type', 'owl:Class');"""
        )


def dict_factory(cursor, row):
    """Create a dict factory for sqlite cursor"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def extract_terms(database, terms, annotations, no_hierarchy=False):
    """Extract terms from the ontology database and return the module as lines of Turtle."""
    # Create a new table (extract) and copy the triples we care about
    # Then write the triples from that table to the output file
    with sqlite3.connect(database) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        try:
            # Create the extract table
            cur.execute("DROP TABLE IF EXISTS extract;")
            cur.execute(
                """CREATE TABLE extract(stanza TEXT,
                                  subject TEXT,
                                  predicate TEXT,
                                  object TEXT,
                                  value TEXT,
                                  datatype TEXT,
                                  language TEXT);"""
            )

            # Get each term up to the top-level (unless no_hierarchy)
            if not no_hierarchy:
                for t in terms:
                    add_ancestors(cur, t)
            else:
                # Only add the terms themselves (as long as they exist)
                for t in terms:
                    add_term(cur, t)

            # Add declarations for any annotations used in 'extract'
            cur.execute(
                """INSERT INTO extract (stanza, subject, predicate, object)
                    SELECT DISTINCT
                      predicate AS stanza,
                      predicate AS subject,
                      'rdf:type',
                      'owl:AnnotationProperty'
                    FROM extract WHERE value NOT NULL;"""
            )

            # Add annotations for all subjects
            add_annotations(cur, annotations=annotations)

            # Reset row factory
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            return get_ttl(cur)
        finally:
            # Always drop the extract table
            cur.execute("DROP TABLE IF EXISTS extract;")


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
