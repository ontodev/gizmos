import gizmos.tree
import html5lib
import psycopg2
import sqlite3

from pyRdfa.parse import parse_one_node
from pyRdfa.state import ExecutionContext
from pyRdfa.options import Options
from rdflib import Graph
from util import test_conn, test_db, create_postgresql_db, create_sqlite_db, compare_graphs


def check_term(conn, term, predicates):
    cur = conn.cursor()
    html = gizmos.tree.build_tree(cur, "obi", term, predicate_ids=predicates)

    # Create the DOM document element
    parser = html5lib.HTMLParser(tree=html5lib.treebuilders.getTreeBuilder("dom"))
    dom = parser.parse(html)

    # get the DOM tree
    top = dom.documentElement

    # Create the initial state (from pyRdfa)
    actual = Graph()
    options = Options(
        output_default_graph=True,
        output_processor_graph=True,
        space_preserve=True,
        transformers=[],
        embedded_rdf=True,
        vocab_expansion=False,
        vocab_cache=True,
        vocab_cache_report=False,
        refresh_vocab_cache=False,
        check_lite=False,
        experimental_features=True,
    )
    state = ExecutionContext(
        top, actual, base="http://purl.obolibrary.org/obo/", options=options, rdfa_version="1.1",
    )

    # Add the RDFa to the RDFLib graph (recursive)
    parse_one_node(top, actual, None, state, [])

    expected = Graph()
    if predicates:
        expected.parse(f"tests/resources/obi-tree-{term}-predicates.ttl", format="turtle")
    else:
        expected.parse(f"tests/resources/obi-tree-{term}.ttl", format="turtle")

    compare_graphs(actual, expected)


def tree(conn):
    check_term(conn, "OBI:0000666", [])
    check_term(conn, "OBI:0000793", [])
    check_term(
        conn,
        "OBI:0000793",
        ["rdfs:label", "IAO:0000115", "rdfs:subClassOf", "owl:equivalentClass", "rdf:type"]
    )
    check_term(conn, "OBI:0100046", [])


def test_tree_postgresql(create_postgresql_db):
    with psycopg2.connect(test_conn) as conn:
        tree(conn)


def test_tree_sqlite(create_sqlite_db):
    with sqlite3.connect(test_db) as conn:
        tree(conn)
