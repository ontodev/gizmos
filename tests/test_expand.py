from io import StringIO
import csv

from gizmos.expand import expand
from sqlalchemy import create_engine
from util import create_postgresql_db, create_sqlite_db, postgres_url, sqlite_url


def expand_import(conn):
    rows = expand(
        conn,
        [
            {"ID": "OBI:0000666", "Related": "ancestors"},
            {"ID": "BFO:0000001", "Related": "descendants"},
        ],
    )
    actual = StringIO()
    writer = csv.DictWriter(
        actual,
        fieldnames=["ID", "Reason"],
        delimiter="\t",
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    with open("tests/resources/obi-expand.tsv", "r") as f:
        assert actual.getvalue() == f.read()


def expand_import_with_labels(conn):
    rows = expand(
        conn,
        [
            {
                "ID": "OBI:0000666",
                "Label": "background correction data transformation",
                "Related": "ancestors",
            },
            {"ID": "BFO:0000001", "Label": "entity", "Related": "descendants"},
        ],
    )
    actual = StringIO()
    writer = csv.DictWriter(
        actual,
        fieldnames=["ID", "Label", "Reason"],
        delimiter="\t",
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    with open("tests/resources/obi-expand-labels.tsv", "r") as f:
        assert actual.getvalue() == f.read()


def test_expand_postgresql(create_postgresql_db):
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        expand_import(conn)
        expand_import_with_labels(conn)


def test_expand_sqlite(create_sqlite_db):
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        expand_import(conn)
        expand_import_with_labels(conn)
