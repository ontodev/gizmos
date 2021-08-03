import csv
import logging
import sys

from argparse import ArgumentParser, Namespace
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .helpers import (
    add_labels,
    escape_qnames,
    get_connection,
    get_ids,
    get_terms,
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
    add_labels(conn)

    # First pass on terms, get all related entities
    ignore = []
    more_terms = set()
    for term_id, details in terms.items():
        # Confirm that this term exists
        query = sql_text("SELECT * FROM statements WHERE stanza = :term_id LIMIT 1")
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
                ancestors = set()
                if intermediates == "none":
                    # Find first ancestor/s that is/are either:
                    # - in the set of input terms
                    # - a top level term (below owl:Thing)
                    get_top_ancestors(conn, ancestors, term_id, top_terms=list(terms.keys()))
                else:
                    # Otherwise get a set of ancestors, stopping at terms that are either:
                    # - in the set of input terms
                    # - a top level term (below owl:Thing)
                    get_ancestors_capped(conn, set(terms.keys()), ancestors, term_id)
                more_terms.update(ancestors)
            elif r == "children":
                # Just add the direct children
                more_terms.update(get_children(conn, term_id))
            elif r == "descendants":
                if intermediates == "none":
                    # Find all bottom-level descendants (do not have children)
                    descendants = set()
                    get_bottom_descendants(conn, descendants, term_id)
                    more_terms.update(descendants)
                else:
                    # Get a set of all descendants, including intermediates
                    more_terms.update(get_descendants(conn, term_id))
            elif r == "parents":
                # Just add the direct parents
                more_terms.update(get_parents(conn, term_id))
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
                """INSERT OR IGNORE INTO tmp_predicates
                SELECT DISTINCT predicate
                FROM statements WHERE predicate NOT IN
                  ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')"""
            )
        else:
            conn.execute(
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
            query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, :override_parent)")
            conn.execute(query, term_id=term_id, override_parent=override_parent)
            continue
        if no_hierarchy:
            continue

        # Otherwise only add the parent if we want a hierarchy
        # Check for the first ancestor we can find with all terms considered "top level"
        # In many cases, this is just the direct parent
        parents = set()
        get_top_ancestors(conn, parents, term_id, top_terms=list(terms.keys()))
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
        """INSERT INTO tmp_extract
        SELECT * FROM statements
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate = 'rdf:type'
          AND object IN
          ('owl:Class', 'owl:AnnotationProperty', 'owl:DataProperty', 'owl:ObjectProperty', 'owl:NamedIndividual')"""
    )

    # Insert subproperty statements for any property types
    conn.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT child, child, 'rdfs:subPropertyOf', parent
        FROM tmp_terms WHERE parent IS NOT NULL AND child IN
          (SELECT subject FROM statements WHERE predicate = 'rdf:type'
           AND object IN ('owl:AnnotationProperty', 'owl:DataProperty', 'owl:ObjectProperty')
           AND subject NOT LIKE '_:%%')"""
    )

    # Insert subclass statements for any class types
    conn.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT DISTINCT child, child, 'rdfs:subClassOf', parent
        FROM tmp_terms WHERE parent IS NOT NULL AND child IN
          (SELECT subject FROM statements WHERE predicate = 'rdf:type'
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
        """INSERT INTO tmp_extract
        SELECT *
        FROM statements
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND value IS NOT NULL"""
    )

    # Insert logical relationships (object must be in set of input terms)
    conn.execute(
        """INSERT INTO tmp_extract
        SELECT * FROM statements
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND object IN (SELECT DISTINCT child FROM tmp_terms)"""
    )

    # Insert IRI annotations (object does not have to be in input terms)
    conn.execute(
        """INSERT INTO tmp_extract (stanza, subject, predicate, object)
        SELECT s1.stanza, s1.subject, s1.predicate, s1.object
        FROM statements s1
        JOIN statements s2 ON s1.predicate = s2.subject
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


def get_ancestors_capped(conn: Connection, top_terms: set, ancestors: set, term_id: str):
    """Return a set of ancestors for a given term ID, until a term in the top_terms is reached,
    or a top-level term is reached (below owl:Thing).

    :param conn: database connection
    :param top_terms: set of top terms to stop at
    :param ancestors: set to collect ancestors in
    :param term_id: term ID to get the ancestors of"""
    query = sql_text(
        """SELECT DISTINCT object FROM statements WHERE stanza = :term_id
        AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf') AND object NOT LIKE '_:%%'"""
    )
    results = conn.execute(query, term_id=term_id)
    ancestors.add(term_id)
    for res in results:
        o = res["object"]
        if o == "owl:Thing" or (top_terms and o in top_terms):
            continue
        ancestors.add(o)
        get_ancestors_capped(conn, top_terms, ancestors, o)


def get_bottom_descendants(conn: Connection, descendants: set, term_id: str):
    """Get all bottom-level descendants for a given term with no intermediates. The bottom-level
    terms are those that are not ever used as the object of an rdfs:subClassOf statement.

    :param conn: database connection
    :param descendants: a set to add descendants to
    :param term_id: term ID to get the bottom descendants of
    """
    query = sql_text(
        """SELECT DISTINCT stanza FROM statements
    WHERE object = :term_id AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')"""
    )
    results = conn.execute(query, term_id=term_id)
    descendants.add(term_id)
    for res in results:
        get_bottom_descendants(conn, descendants, res["stanza"])


def get_children(conn: Connection, term_id: str) -> set:
    """Return a set of children for a given term ID."""
    query = sql_text(
        """SELECT DISTINCT stanza FROM statements
        WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf') AND object = :term_id"""
    )
    results = conn.execute(query, term_id=term_id)
    return set([x["stanza"] for x in results])


def get_descendants(conn: Connection, term_id: str) -> set:
    """Return a set of descendants for a given term ID."""
    query = sql_text(
        """WITH RECURSIVE descendants(node) AS (
            VALUES (:term_id)
            UNION
             SELECT stanza AS node
            FROM statements
            WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND stanza = :term_id
            UNION
            SELECT stanza AS node
            FROM statements, descendants
            WHERE descendants.node = statements.object
              AND statements.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
        )
        SELECT * FROM descendants"""
    )
    results = conn.execute(query, term_id=term_id)
    return set([x[0] for x in results])


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


def get_parents(conn: Connection, term_id: str) -> set:
    """Return a set of parents for a given term ID."""
    query = sql_text(
        """SELECT DISTINCT object FROM statements WHERE stanza = :term_id
        AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf') AND object NOT LIKE '_:%%'"""
    )
    results = conn.execute(query, term_id=term_id)
    return set([x["object"] for x in results])


def get_top_ancestors(conn: Connection, ancestors: set, term_id: str, top_terms: list = None):
    """Get the top-level ancestor or ancestors for a given term with no intermediates. The top-level
    terms are those with no rdfs:subClassOf statement, or direct children of owl:Thing. If top_terms
    is included, they may also be those terms in that list.

    :param conn: database connection
    :param ancestors: a set to add ancestors to
    :param term_id: term ID to get the top ancestor of
    :param top_terms: a list of top-level terms to stop at
                      (if an ancestor is in this set, it will be added and recursion will stop)
    """
    query = sql_text(
        """SELECT DISTINCT object FROM statements WHERE stanza = :term_id
        AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf') AND object NOT LIKE '_:%%'"""
    )
    results = conn.execute(query, term_id=term_id)
    ancestors.add(term_id)
    for res in results:
        o = res["object"]
        if o == "owl:Thing":
            ancestors.add(term_id)
            break
        if top_terms and o in top_terms:
            ancestors.add(o)
        else:
            get_top_ancestors(conn, ancestors, o, top_terms=top_terms)


if __name__ == "__main__":
    main()
