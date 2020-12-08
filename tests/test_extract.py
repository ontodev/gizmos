from gizmos.extract import extract_terms
from rdflib import Graph
from util import test_db, create_db, compare_graphs


def test_extract(create_db):
    ttl = extract_terms(test_db, ["OBI:0100046"], ["rdfs:label", "IAO:0010000"])

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract.ttl", format="turtle")

    compare_graphs(actual, expected)
