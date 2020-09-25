import csv
import os
import pytest
import sqlite3

from rdflib.compare import to_isomorphic, graph_diff


test_db = "build/obi.db"


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


@pytest.fixture
def create_db():
    build = os.path.dirname(test_db)
    if not os.path.isdir(build):
        os.mkdir(build)

    with sqlite3.connect(test_db) as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS prefix")
        cur.execute(
            "CREATE TABLE prefix (" "  prefix TEXT PRIMARY KEY NOT NULL," "  base TEXT NOT NULL" ")"
        )
        with open("tests/resources/prefix.tsv") as f:
            rows = list(csv.reader(f, delimiter="\t"))
            cur.executemany("INSERT INTO prefix VALUES (?, ?)", rows[1:])

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
            cur.executemany("INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?)", rows[1:])


if __name__ == "__main__":
    create_db()
