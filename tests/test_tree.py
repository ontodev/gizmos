import os
import gizmos.tree
import html5lib
import sqlite3
import sys

from pyRdfa.parse import parse_one_node
from pyRdfa.state import ExecutionContext
from pyRdfa.options import Options

from rdflib import Graph, Literal, URIRef


def test_tree():
    db = "tests/resources/obi.db"
    treename = os.path.splitext(os.path.basename(db))[0]

    with sqlite3.connect(db) as conn:
        conn.row_factory = gizmos.tree.dict_factory
        cur = conn.cursor()
        html = gizmos.tree.terms2rdfa(cur, treename, ["OBI:0100046"])

    # Create the DOM document element
    parser = html5lib.HTMLParser(tree=html5lib.treebuilders.getTreeBuilder("dom"))
    dom = parser.parse(html)

    # get the DOM tree
    top = dom.documentElement

    # Create the initial state (from pyRdfa)
    graph = Graph()
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
        top,
        graph,
        base="http://purl.obolibrary.org/obo/",
        options=options,
        rdfa_version="1.1",
    )

    # Add the RDFa to the RDFLib graph (recursive)
    parse_one_node(top, graph, None, state, [])

    # Read in the expected output to compare
    success = True
    expected_graph = Graph()
    expected_graph.parse("tests/resources/obi.ttl", format="turtle")
    subject = URIRef("http://purl.obolibrary.org/obo/OBI_0100046")

    # Check that no triples are missing
    for p, o in expected_graph.predicate_objects(subject):
        if (subject, URIRef(p), Literal(str(o))) not in graph and (
            subject,
            URIRef(p),
            URIRef(o),
        ) not in graph:
            success = False
            print(f"Missing {p}: {o}")

    # Check that no triples have been added
    for p, o in graph.predicate_objects(subject):
        if (subject, URIRef(p), Literal(str(o))) not in graph and (
            subject,
            URIRef(p),
            URIRef(o),
        ) not in graph:
            success = False
            print(f"Added {p}: {o}")

    if not success:
        sys.exit(1)
