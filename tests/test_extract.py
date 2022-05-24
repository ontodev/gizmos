from gizmos.extract import extract
from rdflib import Graph
from sqlalchemy import create_engine
from util import create_postgresql_db, create_sqlite_db, compare_graphs, postgres_url, sqlite_url


def extract_no_hierarchy(conn):
    ttl = extract(
        conn,
        {"OBI:0100046": {}, "BFO:0000040": {}},
        ["rdfs:label", "IAO:0010000"],
        no_hierarchy=True,
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-no-hierarchy.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_ancestors(conn):
    ttl = extract(conn, {"OBI:0100046": {"Related": "ancestors"}}, ["rdfs:label", "IAO:0010000"])

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-ancestors.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_ancestors_no_intermediates(conn):
    ttl = extract(
        conn,
        {"OBI:0100046": {"Related": "ancestors"}, "OBI:0000666": {"Related": "ancestors"}},
        ["rdfs:label"],
        intermediates="none",
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-ancestors-no-intermediates.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_children(conn):
    ttl = extract(conn, {"BFO:0000040": {"Related": "children"}}, ["rdfs:label", "IAO:0010000"])

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-children.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_descendants(conn):
    ttl = extract(conn, {"BFO:0000040": {"Related": "descendants"}}, ["rdfs:label", "IAO:0010000"])

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-descendants.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_descendants_no_intermediates(conn):
    ttl = extract(
        conn,
        {"BFO:0000040": {"Related": "descendants"}},
        ["rdfs:label", "IAO:0010000"],
        intermediates="none"
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-descendants-no-intermediates.ttl", format="turtle")

    compare_graphs(actual, expected)


def extract_with_parents(conn):
    ttl = extract(conn, {"OBI:0100046": {"Related": "parents"}}, ["rdfs:label", "IAO:0010000"])

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract-parents.ttl", format="turtle")

    compare_graphs(actual, expected)


def test_extract_postgresql(create_postgresql_db):
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        extract_with_ancestors(conn)
        extract_with_ancestors_no_intermediates(conn)
        extract_with_children(conn)
        extract_with_descendants(conn)
        extract_with_descendants_no_intermediates(conn)
        extract_with_parents(conn)
        extract_no_hierarchy(conn)


def test_extract_sqlite(create_sqlite_db):
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        extract_with_ancestors(conn)
        extract_with_ancestors_no_intermediates(conn)
        extract_with_children(conn)
        extract_with_descendants(conn)
        extract_with_descendants_no_intermediates(conn)
        extract_with_parents(conn)
        extract_no_hierarchy(conn)
