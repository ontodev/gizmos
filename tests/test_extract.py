import psycopg2
import sqlite3

from gizmos.extract import extract_terms
from rdflib import Graph
from util import test_conn, test_db, create_postgresql_db, create_sqlite_db, compare_graphs


def extract(conn):
    ttl = extract_terms(
        conn, {"OBI:0100046": {"Related": "ancestors"}}, ["rdfs:label", "IAO:0010000"]
    )

    actual = Graph()
    actual.parse(data=ttl, format="turtle")

    expected = Graph()
    expected.parse("tests/resources/obi-extract.ttl", format="turtle")

    compare_graphs(actual, expected)


def test_extract_postgresql(create_postgresql_db):
    with psycopg2.connect(**test_conn) as conn:
        extract(conn)


def test_extract_sqlite(create_sqlite_db):
    with sqlite3.connect(test_db) as conn:
        extract(conn)
