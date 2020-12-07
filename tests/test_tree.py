import gizmos.tree
import html5lib
import sqlite3

from pyRdfa.parse import parse_one_node
from pyRdfa.state import ExecutionContext
from pyRdfa.options import Options
from rdflib import Graph
from util import test_db, create_db, compare_graphs


def check_term(term, predicates):
    with sqlite3.connect(test_db) as conn:
        conn.row_factory = gizmos.tree.dict_factory
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


def test_tree(create_db):
    check_term("OBI:0000666", [])
    check_term("OBI:0000793", [])
    check_term(
        "OBI:0000793",
        ["rdfs:label", "IAO:0000115", "rdfs:subClassOf", "owl:equivalentClass", "rdf:type"]
    )
    check_term("OBI:0100046", [])
