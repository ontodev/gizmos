import csv
import logging
import sqlite3
import sys

from argparse import ArgumentParser
from .helpers import add_labels, escape_qnames, get_ancestors, get_children, get_connection, get_descendants, get_ids, get_parents, get_terms, get_ttl, ttl_to_json

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
    p.add_argument("-i", "--imports", help="TSV or CSV file containing import module details")
    p.add_argument("-s", "--source", help="Ontology source to filter imports file")
    p.add_argument("-f", "--format", help="Output format (ttl or json)", default="ttl")
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
    terms_list = get_terms(args.term, args.terms)
    terms = {}
    if args.imports:
        terms = get_import_terms(args.imports, source=args.source)

    if not terms_list and not terms:
        logging.critical("One or more term(s) must be specified with --term, --terms, or --imports")
        sys.exit(1)

    for t in terms_list:
        if not args.no_hierarchy:
            terms[t] = {"Related": "ancestors"}
        else:
            terms[t] = {}

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

    # First pass, get all related entities
    ignore = []
    more_terms = set()
    for term_id, details in terms.items():
        # Confirm that this term exists
        cur.execute(f"SELECT * FROM statements WHERE stanza = '{term_id}' LIMIT 1")
        res = cur.fetchone()
        if not res:
            logging.warning(f"'{term_id}' does not exist in database")
            ignore.append(term_id)
            continue

        # Check for related entities
        related = details.get("Related")
        if not related:
            continue
        related = related.strip().lower()
        if related == "ancestors":
            more_terms.update(get_ancestors(cur, term_id))
        elif related == "children":
            more_terms.update(get_children(cur, term_id))
        elif related == "descendants":
            more_terms.update(get_descendants(cur, term_id))
        elif related == "parents":
            more_terms.update(get_parents(cur, term_id))
        else:
            # TODO: should this just warn and continue?
            logging.error(f"unknown 'Related' keyword for '{term_id}': " + related)
            sys.exit(1)

    # Add those extra terms to our terms dict
    for mt in more_terms:
        if mt not in terms:
            # Don't worry about the parent ID because relationship will be maintained ...
            # ... as long as the parent is in our terms
            terms[mt] = {}

    predicate_ids = None
    if predicates:
        # Current predicates are IDs or labels - make sure we get all the IDs
        predicate_ids = get_ids(cur, predicates)

    # Create the terms table containing parent -> child relationships
    cur.execute("CREATE TABLE tmp_terms(child TEXT, parent TEXT)")
    for term_id in terms.keys():
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
        for term_id, details in terms.items():
            override_parent = details.get("Parent ID")
            if override_parent:
                # Just assert this as parent and don't worry about existing parent(s)
                cur.execute(
                    f"INSERT INTO tmp_terms VALUES ('{term_id}', '{override_parent}')"
                )
            else:
                # Get the parent(s) from statements and see which are in our input terms
                parents = get_parents(cur, term_id)
                included_parents = parents.intersection(set(terms.keys()))
                if included_parents:
                    # Maintain these relationships in the import module
                    for ip in included_parents:
                        cur.execute(
                            f"INSERT INTO tmp_terms VALUES ('{term_id}', '{ip}')"
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
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms) AND predicate = 'rdf:type'"""
    )

    # Insert subproperty statements for any property types
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT child, child, 'rdfs:subPropertyOf', parent
        FROM tmp_terms WHERE parent IS NOT NULL AND child IN
          (SELECT stanza FROM statements WHERE predicate = 'rdf:type'
           AND object IN ('owl:AnnotationProperty', 'owl:DataProperty', 'owl:ObjectProperty'))"""
    )

    # Insert subclass statements for any class types
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT child, child, 'rdfs:subClassOf', parent
        FROM tmp_terms WHERE parent IS NOT NULL AND child IN
          (SELECT stanza FROM statements WHERE predicate = 'rdf:type' AND object = 'owl:Class')"""
    )

    # Everything else is an instance
    # TODO: or datatype?
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT child, child, 'rdf:type', parent
        FROM tmp_terms WHERE parent IS NOT NULL AND child NOT IN
          (SELECT stanza from tmp_extract
           WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf'))"""
    )

    # Insert literal annotations
    cur.execute(
        """INSERT INTO tmp_extract
        SELECT *
        FROM statements
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND value IS NOT NULL"""
    )

    # Insert logical relationships (object must be in set of input terms)
    cur.execute(
        """INSERT INTO tmp_extract
        SELECT * FROM statements
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND object IN (SELECT DISTINCT child FROM tmp_terms)"""
    )

    # Insert IRI annotations (object does not have to be in input terms)
    cur.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT s1.stanza, s1.subject, s1.predicate, s1.object
        FROM statements s1
        JOIN statements s2 ON s1.predicate = s2.subject
        WHERE s1.subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND s1.predicate IN (SELECT predicate FROM tmp_predicates)
          AND s2.object = 'owl:AnnotationProperty'
          AND s1.object IS NOT NULL"""
    )

    # Escape QNames
    escape_qnames(cur, "tmp_extract")

    ttl = get_ttl(cur, "tmp_extract")
    if fmt.lower() == "ttl":
        return ttl

    # Otherwise the format is JSON
    return ttl_to_json(cur, ttl)


def get_import_terms(import_file, source=None):
    """Get the terms and their details from the input file.

    :param import_file: path to file containing import details
    :param source: source ontology ID for terms to include
                   (if None or if input has no 'source' column, all are included)
    """
    terms = {}
    sep = "\t"
    if import_file.endswith(".csv"):
        sep = "\t"
    with open(import_file, "r") as f:
        reader = csv.DictReader(f, delimiter=sep)
        for row in reader:
            term_id = row.get("ID")
            if not term_id:
                continue
            if source and row.get("Source") != source:
                # If we have a source and this is not from that source, skip
                continue
            terms[term_id] = {"Parent ID": row.get("Parent ID"), "Related": row.get("Related")}
    return terms


if __name__ == "__main__":
    main()
