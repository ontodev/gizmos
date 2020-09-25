import gizmos.extract

from rdflib import Graph
from rdflib.compare import to_isomorphic, graph_diff

def dump_ttl_sorted(graph):
    for line in sorted(graph.serialize(format="ttl").splitlines()):
        if line:
            print(line.decode("ascii"))

def test_extract():
    ttl = "\n".join(
        gizmos.extract.extract_terms(
            "tests/resources/obi.db",
            ["OBI:0100046"],
            ["rdfs:label"],
        )
    )

    graph = Graph()
    graph.parse(data=ttl, format="turtle")

    expected_graph = Graph()
    expected_graph.parse("tests/resources/obi-extract.ttl", format="turtle")

    graph_iso = to_isomorphic(graph)
    expected_graph_iso = to_isomorphic(expected_graph)

    if graph_iso != expected_graph_iso:
        _, in_first, in_second = graph_diff(graph_iso, expected_graph_iso)
        print("The expected and generated graphs differ.")
        print("----- Generated graph -----")
        for line in graph.serialize(format="ttl").splitlines():
            print(line)
        print("----- Contents of generated graph not in expected graph -----")
        dump_ttl_sorted(in_first)
        print("----- Contents of expected graph not in generated graph -----")
        dump_ttl_sorted(in_second)

    assert graph_iso == expected_graph_iso
