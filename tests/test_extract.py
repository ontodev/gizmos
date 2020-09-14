import gizmos.extract
import sys

from argparse import Namespace
from rdflib import Graph, Literal, URIRef


def test_extract():
    args = Namespace(
        database="tests/resources/obi.db",
        term=None,
        terms="tests/resources/obi-terms.txt",
        annotation=["rdfs:label"],
        annotations=None,
        no_hierarchy=False,
    )
    ttl = gizmos.extract.extract(args)

    graph = Graph()
    graph.parse(data=ttl, format="turtle")

    success = True
    expected_graph = Graph()
    expected_graph.parse("tests/resources/obi-extract.ttl", format="turtle")

    # Check that no triples are missing
    subjects = expected_graph.subjects()
    for subject in subjects:
        for p, o in expected_graph.predicate_objects(subject):
            if (subject, URIRef(p), Literal(str(o), lang="en")) not in graph and (
                subject,
                URIRef(p),
                URIRef(o),
            ) not in graph:
                success = False
                print(f"Missing '{subject} {p} {o}'")

    # Check that no triples have been added
    subjects = graph.subjects()
    for subject in subjects:
        if str(subject) == "http://www.w3.org/2002/07/owl#Thing":
            continue
        for p, o in graph.predicate_objects(subject):
            if (
                (subject, URIRef(p), Literal(str(o), lang="en")) not in expected_graph
                and (subject, URIRef(p), URIRef(o),) not in expected_graph
            ):
                success = False
                print(f"Added '{subject} {p} {o}'")

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    test_extract()
