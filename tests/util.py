import csv
import os
import pytest

from rdflib.compare import to_isomorphic, graph_diff
from sqlalchemy import create_engine

POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PW = os.environ.get("POSTGRES_PW", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
postgres_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}/gizmos_test"

sqlite_url = "sqlite:///" + os.path.abspath("build/obi.db")


def dump_ttl_sorted(graph):
    for line in sorted(graph.serialize(format="ttl").splitlines()):
        if line:
            print(line.decode("ascii"))


def compare_graphs(actual, expected):
    actual_iso = to_isomorphic(actual)
    expected_iso = to_isomorphic(expected)

    if actual_iso != expected_iso:
        _, in_first, in_second = graph_diff(actual_iso, expected_iso)
        print("The actual and expected graphs differ")
        print("----- Contents of actual graph not in expected graph -----")
        dump_ttl_sorted(in_first)
        print("----- Contents of expected graph not in actual graph -----")
        dump_ttl_sorted(in_second)

    assert actual_iso == expected_iso


def add_tables(conn):
    with conn.begin():
        conn.execute("DROP TABLE IF EXISTS prefix")
        conn.execute(
            "CREATE TABLE prefix (" "  prefix TEXT PRIMARY KEY NOT NULL," "  base TEXT NOT NULL" ")"
        )
        with open("tests/resources/prefix.tsv") as f:
            rows = list(csv.reader(f, delimiter="\t"))
            for r in rows:
                conn.execute(f"INSERT INTO prefix VALUES ('{r[0]}', '{r[1]}')")

        conn.execute("DROP TABLE IF EXISTS statements")
        conn.execute(
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
                    query.append("'" + itm.replace("'", "''").replace("%", "%%") + "'")
                query = ", ".join(query)
                conn.execute(f"INSERT INTO statements VALUES ({query})")


@pytest.fixture
def create_postgresql_db():
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}",
        isolation_level="AUTOCOMMIT",
    )
    with engine.connect() as conn:
        res = conn.execute(
            "SELECT datname FROM pg_database WHERE datname = 'gizmos_test';"
        ).fetchone()
        if not res:
            with conn.begin():
                conn.execute("CREATE DATABASE gizmos_test")
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        add_tables(conn)


@pytest.fixture
def create_sqlite_db():
    if not os.path.isdir("build"):
        os.mkdir("build")
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        add_tables(conn)


if __name__ == "__main__":
    create_sqlite_db()
