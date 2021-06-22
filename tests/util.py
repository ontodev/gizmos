import csv
import logging
import os
import psycopg2
import pytest
import sqlite3
import sys

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from rdflib.compare import to_isomorphic, graph_diff

test_db = "build/obi.db"

POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PW = os.environ.get("POSTGRES_PW", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
test_conn = {"host": POSTGRES_HOST, "database": "gizmos_test", "user": POSTGRES_USER, "password": POSTGRES_PW, "port": POSTGRES_PORT}


def dump_ttl(graph, sort):
    lines = graph.serialize(format="ttl").splitlines()
    if sort:
        lines.sort()
    for line in lines:
        if line:
            try:
                print(line.decode("ascii"))
            except UnicodeDecodeError:
                print(line)

def compare_graphs(actual, expected, show_diff=False, sort=False):
    actual_iso = to_isomorphic(actual)
    expected_iso = to_isomorphic(expected)

    if actual_iso != expected_iso:
        print("The actual and expected graphs differ")
        if show_diff:
            _, in_first, in_second = graph_diff(actual_iso, expected_iso)
            print("----- Contents of actual graph not in expected graph -----")
            dump_ttl(in_first, sort)
            print("----- Contents of expected graph not in actual graph -----")
            dump_ttl(in_second, sort)

    assert actual_iso == expected_iso


def create_db(conn):
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS prefix")
    cur.execute(
        "CREATE TABLE prefix (" "  prefix TEXT PRIMARY KEY NOT NULL," "  base TEXT NOT NULL" ")"
    )
    with open("tests/resources/prefix.tsv") as f:
        rows = list(csv.reader(f, delimiter="\t"))
        for r in rows:
            cur.execute(f"INSERT INTO prefix VALUES ('{r[0]}', '{r[1]}')")

    cur.execute("DROP TABLE IF EXISTS statements")
    cur.execute(
        "CREATE TABLE statements ("
        "  stanza TEXT,"
        "  subject TEXT,"
        "  predicate TEXT,"
        "  object TEXT,"
        "  value TEXT,"
        "  datatype TEXT,"
        "  language TEXT"
        ")"
    )
    with open("tests/resources/statements.tsv") as f:
        rows = []
        for row in csv.reader(f, delimiter="\t"):
            rows.append([None if not x else x for x in row])
        for r in rows:
            query = []
            for itm in r:
                if not itm:
                    query.append("NULL")
                    continue
                query.append("'" + itm.replace("'", "''") + "'")
            query = ", ".join(query)
            cur.execute(f"INSERT INTO statements VALUES ({query})")


@pytest.fixture
def create_postgresql_db():
    with psycopg2.connect(host=POSTGRES_HOST, user=POSTGRES_USER, password=POSTGRES_PW, port=POSTGRES_PORT) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datname = 'gizmos_test';")
        res = cur.fetchone()
        if not res:
            cur.execute("CREATE DATABASE gizmos_test")
    with psycopg2.connect(**test_conn) as conn:
        create_db(conn)


@pytest.fixture
def create_sqlite_db():
    build = os.path.dirname(test_db)
    if not os.path.isdir(build):
        os.mkdir(build)

    with sqlite3.connect(test_db) as conn:
        create_db(conn)


if __name__ == "__main__":
    create_sqlite_db()
