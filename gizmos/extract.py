import csv
import logging
import sqlite3
import sys

from argparse import ArgumentParser
from .helpers import (
    add_labels,
    escape_qnames,
    get_ancestors_capped,
    get_bottom_descendants,
    get_children,
    get_connection,
    get_descendants,
    get_ids,
    get_parents,
    get_terms,
    get_top_ancestors,
    get_ttl,
    ttl_to_json,
)

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
    p.add_argument("-c", "--config", help="Source configuration for imports")
    p.add_argument(
        "-I",
        "--intermediates",
        help="Included ancestor/descendant intermediates (default: all)",
        default="all",
    )
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
    source = args.source
    if args.imports:
        terms = get_import_terms(args.imports, source=source)

    if not terms_list and not terms:
        logging.critical("One or more term(s) must be specified with --term, --terms, or --imports")
        sys.exit(1)

    for t in terms_list:
        if not args.no_hierarchy:
            terms[t] = {"Related": "ancestors"}
        else:
            terms[t] = {}

    predicates = get_terms(args.predicate, args.predicates)
    intermediates = args.intermediates

    if args.config:
        # Get options from the config file based on the source
        if not source:
            logging.critical("A --source is required when using the --config option")
            sys.exit(1)
        config_path = args.config
        sep = "\t"
        if config_path.endswith(".csv"):
            sep = ","
        # Search for the source in the file and read in option
        found_source = False
        with open(config_path, "r") as f:
            reader = csv.DictReader(f, delimiter=sep)
            for row in reader:
                if row["Source"] == source:
                    found_source = True
                    intermediates = row.get("Intermediates", "all")
                    predicates_str = row.get("Predicates")
                    if predicates_str:
                        # Extend any existing command-line predicates
                        predicates.extend(predicates_str.split(" "))
                    break
        if not found_source:
            # No source with provided name found
            logging.critical(f"Source '{source}' does not exist in config file: " + config_path)
            sys.exit(1)

    # Get the database connection & extract terms
    conn = get_connection(args.database)
    try:
        return extract_terms(
            conn,
            terms,
            predicates,
            fmt=args.format,
            intermediates=intermediates,
            no_hierarchy=args.no_hierarchy,
        )
    finally:
        # Always remove temp tables before exiting
        clean(conn)


def clean(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tmp_labels")
    cur.execute("DROP TABLE IF EXISTS tmp_terms")
    cur.execute("DROP TABLE IF EXISTS tmp_predicates")
    cur.execute("DROP TABLE IF EXISTS tmp_extract")
    conn.commit()


def extract_terms(conn, terms, predicates, fmt="ttl", intermediates="all", no_hierarchy=False):
    """Extract terms from the ontology database and return the module as Turtle or JSON-LD."""
    if fmt.lower() not in ["ttl", "json-ld"]:
        raise Exception("Unknown format: " + fmt)

    intermediates = intermediates.lower()
    if intermediates not in ["all", "none"]:
        raise Exception("Unknown 'intermediates' option: " + intermediates)

    # Create a new table (extract) and copy the triples we care about
    # Then write the triples from that table to the output file
    cur = conn.cursor()

    # Pre-clean up
    clean(conn)

    # Create a temp labels table
    add_labels(cur)

    # First pass on terms, get all related entities
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

        # Check for related entities & add them
        related = details.get("Related")
        if not related:
            continue
        related = related.strip().lower().split(" ")
        for r in related:
            if r == "ancestors":
                ancestors = set()
                if intermediates == "none":
                    # Find first ancestor/s that is/are either:
                    # - in the set of input terms
                    # - a top level term (below owl:Thing)
                    get_top_ancestors(cur, ancestors, term_id, top_terms=terms.keys())
                else:
                    # Otherwise get a set of ancestors, stopping at terms that are either:
                    # - in the set of input terms
                    # - a top level term (below owl:Thing)
                    get_ancestors_capped(cur, terms.keys(), ancestors, term_id)
                more_terms.update(ancestors)
            elif r == "children":
                # Just add the direct children
                more_terms.update(get_children(cur, term_id))
            elif r == "descendants":
                if intermediates == "none":
                    # Find all bottom-level descendants (do not have children)
                    descendants = set()
                    get_bottom_descendants(cur, term_id, descendants)
                    more_terms.update(descendants)
                else:
                    # Get a set of all descendants, including intermediates
                    more_terms.update(get_descendants(cur, term_id))
            elif r == "parents":
                # Just add the direct parents
                more_terms.update(get_parents(cur, term_id))
            else:
                # TODO: should this just warn and continue?
                raise Exception(f"unknown 'Related' keyword for '{term_id}': " + r)

    # Add those extra terms from related entities to our terms dict
    for mt in more_terms:
        if mt not in terms:
            # Don't worry about the parent ID because hierarchy will be maintained ...
            # ... based on the first ancestor in the full set of terms
            terms[mt] = {}

    predicate_ids = None
    if predicates:
        # Current predicates are IDs or labels - make sure we get all the IDs
        predicate_ids = get_ids(cur, predicates)

    # Create the terms table containing parent -> child relationships
    cur.execute("CREATE TABLE tmp_terms(child TEXT, parent TEXT)")
    for term_id in terms.keys():
        cur.execute(f"INSERT INTO tmp_terms VALUES ('{term_id}', NULL)")

    # Create tmp predicates table containing all predicates to include
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
                FROM statements WHERE predicate NOT IN
                  ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')"""
            )
        else:
            cur.execute(
                """INSERT INTO tmp_predicates
                SELECT DISTINCT predicate
                FROM statements WHERE predicate NOT IN
                  ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')
                ON CONFLICT (predicate) DO NOTHING"""
            )

    # Add subclass/subproperty/type relationships to terms table
    for term_id, details in terms.items():
        # Check for overrides, regardless of no-hierarchy
        override_parent = details.get("Parent ID")
        if override_parent:
            # Just assert this as parent and don't worry about existing parent(s)
            cur.execute(f"INSERT INTO tmp_terms VALUES ('{term_id}', '{override_parent}')")
            continue
        if no_hierarchy:
            continue

        # Otherwise only add the parent if we want a hierarchy
        # Check for the first ancestor we can find with all terms considered "top level"
        # In many cases, this is just the direct parent
        parents = set()
        get_top_ancestors(cur, parents, term_id, top_terms=terms.keys())
        parents = parents.intersection(set(terms.keys()))
        if parents:
            # Maintain these relationships in the import module
            for p in parents:
                if p == term_id:
                    continue
                cur.execute(f"INSERT INTO tmp_terms VALUES ('{term_id}', '{p}')")

    # Create our extract table to hold the actual triples
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

    # Insert rdf:type declarations - only for OWL entities
    cur.execute(
        """INSERT INTO tmp_extract
        SELECT * FROM statements
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate = 'rdf:type'
          AND object IN
          ('owl:Class', 'owl:AnnotationProperty', 'owl:DataProperty', 'owl:ObjectProperty', 'owl:NamedIndividual')"""
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
