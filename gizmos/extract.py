import csv
import logging
import sys

from argparse import ArgumentParser, Namespace
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .helpers import (
    add_labels,
    escape_qnames,
    get_ancestors,
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
    p.add_argument(
        "-m", "--imported-from", help="IRI of source import ontology to annotate terms with"
    )
    p.add_argument(
        "-M",
        "--imported-from-property",
        help="ID of property to use for 'imported from' annotation (default: IAO:0000412)",
        default="IAO:0000412",
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
    sys.stdout.write(run_extract(args))


def run_extract(args: Namespace) -> str:
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
    imported_from = args.imported_from

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
                    imported_from = row.get("IRI")
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
        return extract(
            conn,
            terms,
            predicates,
            fmt=args.format,
            imported_from=imported_from,
            imported_from_property=args.imported_from_property,
            intermediates=intermediates,
            no_hierarchy=args.no_hierarchy,
        )
    finally:
        # Always remove temp tables before exiting
        clean(conn)


def clean(conn: Connection):
    with conn.begin():
        conn.execute("DROP TABLE IF EXISTS tmp_labels")
        conn.execute("DROP TABLE IF EXISTS tmp_terms")
        conn.execute("DROP TABLE IF EXISTS tmp_predicates")
        conn.execute("DROP TABLE IF EXISTS tmp_extract")


def extract(
    conn: Connection,
    terms: dict,
    predicates: list,
    fmt: str = "ttl",
    imported_from: str = None,
    imported_from_property: str = "IAO:0000412",
    intermediates: str = "all",
    no_hierarchy: bool = False,
    statements: str = "statements",
) -> str:
    """Extract terms from the ontology database and return the module as Turtle or JSON-LD."""
    if fmt.lower() not in ["ttl", "json-ld"]:
        raise Exception("Unknown format: " + fmt)

    intermediates = intermediates.lower()
    if intermediates not in ["all", "none"]:
        raise Exception("Unknown 'intermediates' option: " + intermediates)

    # Pre-clean up
    clean(conn)

    # Create a temp labels table
    add_labels(conn, statements=statements)

    # First pass on terms, get all related entities
    ignore = []
    more_terms = set()
    for term_id, details in terms.items():
        # Confirm that this term exists
        query = sql_text(f"SELECT * FROM {statements} WHERE stanza = :term_id LIMIT 1")
        res = conn.execute(query, term_id=term_id).fetchone()
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
                more_terms.update(get_ancestors(conn, term_id, set(terms.keys()), intermediates))
            elif r == "children":
                # Just add the direct children
                more_terms.update(get_children(conn, term_id, statements=statements))
            elif r == "descendants":
                more_terms.update(get_descendants(conn, term_id, intermediates))
            elif r == "parents":
                # Just add the direct parents
                more_terms.update(get_parents(conn, term_id, statements=statements))
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
        predicate_ids = get_ids(conn, predicates)

    # Create the terms table containing parent -> child relationships
    conn.execute("CREATE TABLE tmp_terms(child TEXT, parent TEXT)")
    for term_id in terms.keys():
        query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, NULL)")
        conn.execute(query, term_id=term_id)

    # Create tmp predicates table containing all predicates to include
    conn.execute("CREATE TABLE tmp_predicates(predicate TEXT PRIMARY KEY NOT NULL)")
    if predicate_ids:
        for predicate_id in predicate_ids:
            if str(conn.engine.url).startswith("sqlite"):
                query = sql_text("INSERT OR IGNORE INTO tmp_predicates VALUES (:predicate_id)")
                conn.execute(query, predicate_id=predicate_id)
            else:
                query = sql_text(
                    """INSERT INTO tmp_predicates VALUES (:predicate_id)
                    ON CONFLICT (predicate) DO NOTHING"""
                )
                conn.execute(query, predicate_id=predicate_id)
    else:
        # Insert all predicates
        if str(conn.engine.url).startswith("sqlite"):
            conn.execute(
                f"""INSERT OR IGNORE INTO tmp_predicates
                 SELECT DISTINCT predicate
                 FROM {statements} WHERE predicate NOT IN
                   ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')"""
            )
        else:
            conn.execute(
                f"""INSERT INTO tmp_predicates
                 SELECT DISTINCT predicate
                 FROM {statements} WHERE predicate NOT IN
                   ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')
                 ON CONFLICT (predicate) DO NOTHING"""
            )

    # Add subclass/subproperty/type relationships to terms table
    for term_id, details in terms.items():
        # Check for overrides, regardless of no-hierarchy
        override_parent = details.get("Parent ID")
        if override_parent:
            # Just assert this as parent and don't worry about existing parent(s)
            query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, :override_parent)")
            conn.execute(query, term_id=term_id, override_parent=override_parent)
            continue
        if no_hierarchy:
            continue

        # Otherwise only add the parent if we want a hierarchy
        # Check for the first ancestor we can find with all terms considered "top level"
        # In many cases, this is just the direct parent
        parents = get_top_ancestors(
            conn, term_id, statements=statements, top_terms=set(terms.keys())
        )
        parents = parents.intersection(set(terms.keys()))
        if parents:
            # Maintain these relationships in the import module
            for p in parents:
                if p == term_id:
                    continue
                query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, :p)")
                conn.execute(query, term_id=term_id, p=p)

    # Create our extract table to hold the actual triples
    conn.execute(
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
    conn.execute(
        f"""INSERT INTO tmp_extract
         SELECT * FROM {statements}
         WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
           AND predicate = 'rdf:type'
           AND object IN
           ('owl:Class',
            'owl:AnnotationProperty',
            'owl:DataProperty',
            'owl:ObjectProperty',
            'owl:NamedIndividual')"""
    )

    # Insert subproperty statements for any property types
    conn.execute(
        f"""INSERT INTO tmp_extract (stanza, subject, predicate, object)
         SELECT DISTINCT child, child, 'rdfs:subPropertyOf', parent
         FROM tmp_terms WHERE parent IS NOT NULL AND child IN
           (SELECT subject FROM {statements} WHERE predicate = 'rdf:type'
            AND object IN ('owl:AnnotationProperty', 'owl:DataProperty', 'owl:ObjectProperty')
            AND subject NOT LIKE '_:%%')"""
    )

    # Insert subclass statements for any class types
    conn.execute(
        f"""INSERT INTO tmp_extract (stanza, subject, predicate, object)
         SELECT DISTINCT child, child, 'rdfs:subClassOf', parent
         FROM tmp_terms WHERE parent IS NOT NULL AND child IN
           (SELECT subject FROM {statements} WHERE predicate = 'rdf:type'
            AND object = 'owl:Class' AND subject NOT LIKE '_:%%')"""
    )

    # Everything else is an instance
    # TODO: or datatype?
    conn.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT child, child, 'rdf:type', parent
        FROM tmp_terms WHERE parent IS NOT NULL AND child NOT IN
          (SELECT stanza from tmp_extract
           WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf'))"""
    )

    # Insert literal annotations
    conn.execute(
        f"""INSERT INTO tmp_extract
            SELECT * FROM {statements}
            WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
              AND predicate IN (SELECT predicate FROM tmp_predicates)
              AND value IS NOT NULL"""
    )

    # Insert logical relationships (object must be in set of input terms)
    conn.execute(
        f"""INSERT INTO tmp_extract
            SELECT * FROM {statements}
            WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
              AND predicate IN (SELECT predicate FROM tmp_predicates)
              AND object IN (SELECT DISTINCT child FROM tmp_terms)"""
    )

    # Insert IRI annotations (object does not have to be in input terms)
    conn.execute(
        f"""INSERT INTO tmp_extract (stanza, subject, predicate, object)
            SELECT s1.stanza, s1.subject, s1.predicate, s1.object FROM {statements} s1
            JOIN {statements} s2 ON s1.predicate = s2.subject
            WHERE s1.subject IN (SELECT DISTINCT child FROM tmp_terms)
              AND s1.predicate IN (SELECT predicate FROM tmp_predicates)
              AND s2.object = 'owl:AnnotationProperty'
              AND s1.object IS NOT NULL"""
    )

    # Finally, if imported_from IRI is included, add this to add terms
    if imported_from:
        query = sql_text(
            """INSERT INTO tmp_extract (stanza, subject, predicate, object)
            SELECT DISTINCT child, child, :imported_from_property, :imported_from FROM tmp_terms"""
        )
        conn.execute(
            query, imported_from_property=imported_from_property, imported_from=f"<{imported_from}>"
        )

    # Escape QNames
    escape_qnames(conn, "tmp_extract")

    ttl = get_ttl(conn, "tmp_extract")
    if fmt.lower() == "ttl":
        return ttl

    # Otherwise the format is JSON
    return ttl_to_json(conn, ttl)


def get_import_terms(import_file: str, source: str = None) -> dict:
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
