import psycopg2
import sqlite3

from gizmos.extract import extract_terms
from rdflib import Graph
from util import test_conn, test_db, create_postgresql_db, create_sqlite_db, compare_graphs


def extract_no_hierarchy(conn):
    ttl = extract_terms(
        conn, {"OBI:0100046": {}, "BFO:0000040": {}}, ["rdfs:label", "IAO:0010000"], no_hierarchy=True
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-no-hierarchy.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_ancestors(conn):
    ttl = extract_terms(
        conn, {"OBI:0100046": {"Related": "ancestors"}}, ["rdfs:label", "IAO:0010000"]
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-ancestors.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_children(conn):
    ttl = extract_terms(
        conn, {"BFO:0000040": {"Related": "children"}}, ["rdfs:label", "IAO:0010000"]
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-children.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_descendants(conn):
    ttl = extract_terms(
        conn, {"BFO:0000040": {"Related": "descendants"}}, ["rdfs:label", "IAO:0010000"]
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-descendants.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_parents(conn):
    ttl = extract_terms(
        conn, {"OBI:0100046": {"Related": "parents"}}, ["rdfs:label", "IAO:0010000"]
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-parents.ttl", format="turtle")

    compare_graphs(actual, expected)


def test_extract_postgresql(create_postgresql_db):
    with psycopg2.connect(**test_conn) as conn:
        extract_with_ancestors(conn)
        extract_with_children(conn)
        extract_with_descendants(conn)
        extract_with_parents(conn)
        extract_no_hierarchy(conn)


def test_extract_sqlite(create_sqlite_db):
    with sqlite3.connect(test_db) as conn:
        extract_with_ancestors(conn)
        extract_with_children(conn)
        extract_with_descendants(conn)
        extract_with_parents(conn)
        extract_no_hierarchy(conn)
