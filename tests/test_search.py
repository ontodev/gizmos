from gizmos.search import search
from sqlalchemy import create_engine
from util import create_postgresql_db, create_sqlite_db, postgres_url, sqlite_url


def search_text(conn):
    res = search(conn, "buffer").strip()
    with open("tests/resources/obi-search.json", "r") as f:
        expected = f.read().strip()
    assert res == expected


def search_text_mixed_case(conn):
    res = search(conn, "buFFeR").strip()
    with open("tests/resources/obi-search.json", "r") as f:
        expected = f.read().strip()
    assert res == expected


def search_text_with_options(conn):
    res = search(conn, "buffer", short_label="ID", synonyms=["IAO:0000118"]).strip()
    with open("tests/resources/obi-search-options.json", "r") as f:
        expected = f.read().strip()
    assert res == expected


def test_search_postgresql(create_postgresql_db):
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        search_text(conn)
        search_text_mixed_case(conn)
        search_text_with_options(conn)


def test_search_sqlite(create_sqlite_db):
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        search_text(conn)
        search_text_mixed_case(conn)
        search_text_with_options(conn)
