import psycopg2
import sqlite3

from gizmos.search import search
from util import test_conn, test_db, create_postgresql_db, create_sqlite_db


def search_text(conn):
    res = search(conn, "buffer").strip()
    with open("tests/resources/obi-search.json", "r") as f:
        expected = f.read().strip()
    assert res == expected


def search_text_with_options(conn):
    res = search(conn, "buffer", short_label="ID", synonyms=["IAO:0000118"]).strip()
    with open("tests/resources/obi-search-options.json", "r") as f:
        expected = f.read().strip()
    assert res == expected


def test_search_postgresql(create_postgresql_db):
    with psycopg2.connect(**test_conn) as conn:
        search_text(conn)


def test_search_sqlite(create_sqlite_db):
    with sqlite3.connect(test_db) as conn:
        search_text(conn)
