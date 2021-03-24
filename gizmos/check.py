import logging
import psycopg2
import sqlite3
import sys

from argparse import ArgumentParser
from .helpers import get_connection


def main():
    p = ArgumentParser()
    p.add_argument("db", help="Database file (.db) or configuration (.ini)")
    p.add_argument("-l", "--limit", help="Max number of messages to log about rows", default="10")
    args = p.parse_args()

    # Set up logger
    logger = logging.getLogger()
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    logger.setLevel(logging.WARNING)
    ch.setLevel(logging.WARNING)
    logger.addHandler(ch)

    # Parse limit
    lim = args.limit
    try:
        limit = int(lim)
    except ValueError:
        if lim.lower() == "none":
            limit = None
        else:
            logger.error("Invalid --limit option: " + lim)
            sys.exit(1)

    conn = get_connection(args.db)
    check(conn, limit=limit)


def check(conn, limit=10):
    logger = logging.getLogger()
    cur = conn.cursor()
    if isinstance(conn, sqlite3.Connection):
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    else:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
    tables = [x[0] for x in cur.fetchall()]

    if "prefix" not in tables:
        logger.error("missing 'prefix' table")
        prefix_ok = False
    else:
        prefix_ok = check_prefix(cur)

    statements_ok = True
    if "statements" not in tables:
        logger.error("missing 'statements' table")
        statements_ok = False
    elif prefix_ok:
        statements_ok = check_statements(cur, limit=limit)

    if not statements_ok or not prefix_ok:
        sys.exit(1)


def check_prefix(cur):
    """Check the structure of the prefix table. It must have the columns 'prefix' and 'base'."""
    logger = logging.getLogger()

    # Check for required columns
    if isinstance(cur, sqlite3.Cursor):
        cur.execute("PRAGMA table_info(prefix)")
        columns = {x[1]: x[2] for x in cur.fetchall()}
    else:
        columns = get_postgres_columns(cur, "prefix")
    missing = []
    bad_type = []
    for col in ["prefix", "base"]:
        coltype = columns.get(col)
        if not coltype:
            missing.append(col)
        elif coltype != "TEXT":
            bad_type.append(col)
    if missing:
        logger.error("'prefix' is missing column(s): " + ", ".join(missing))
        return False
    if bad_type:
        logger.error("'prefix' column(s) do not have type 'TEXT': " + ", ".join(bad_type))
        return False

    # Check for required prefixes
    missing_prefixes = []
    for prefix in ["owl", "rdf", "rdfs"]:
        cur.execute(f"SELECT * FROM prefix WHERE prefix = '{prefix}'")
        res = cur.fetchone()
        if not res:
            missing_prefixes.append(prefix)
    if missing_prefixes:
        logger.error("'prefix' is missing required prefixes: " + ", ".join(missing_prefixes))

    return True


def check_statements(cur, limit=10):
    """Check the structure of the statements table then check the values of the columns."""
    logger = logging.getLogger()
    statements_ok = True

    # First check the structure
    if isinstance(cur, sqlite3.Cursor):
        cur.execute("PRAGMA table_info(statements)")
        columns = {x[1]: x[2] for x in cur.fetchall()}
    else:
        columns = get_postgres_columns(cur, "statements")
    missing = []
    bad_type = []
    for col in [
        "stanza",
        "subject",
        "predicate",
        "object",
        "value",
        "datatype",
        "language",
    ]:
        coltype = columns.get(col)
        if not coltype:
            missing.append(col)
        elif coltype != "TEXT":
            bad_type.append(col)
    if missing:
        logger.error("'statements' is missing column(s): " + ", ".join(missing))
        return False
    if bad_type:
        logger.error("'statements' column(s) do not have type 'TEXT': " + ", ".join(bad_type))
        return False

    # Check for an index on the stanza column, warn if missing (do not fail)
    has_stanza_idx = False
    if isinstance(cur, sqlite3.Cursor):
        cur.execute("PRAGMA index_list(statements)")
        for row in cur.fetchall():
            index = row[1]
            cur.execute(f"PRAGMA index_info({index})")
            col = cur.fetchone()[2]
            if col == "stanza":
                has_stanza_idx = True
                break
    else:
        cur.execute(
            """SELECT a.attname AS column_name
               FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
               WHERE
                t.oid = ix.indrelid
                and i.oid = ix.indexrelid
                and a.attrelid = t.oid
                and a.attnum = ANY(ix.indkey)
                and t.relkind = 'r'
                and t.relname = 'statements';"""
        )
        for row in cur.fetchall():
            if row[0] == "stanza":
                has_stanza_idx = True
                break
    if not has_stanza_idx:
        logger.warning("missing index on 'stanza' column")

    # Get prefixes to check against
    cur.execute("SELECT prefix, base FROM prefix")
    prefixes = {x[0]: x[1] for x in cur.fetchall()}

    # Check subjects, stanzas, predicates, and objects
    message_count = 0
    for col in ["stanza", "subject", "predicate", "object"]:
        if limit and message_count >= limit:
            # Do not exceed the limit of messages
            break
        cur.execute(f"SELECT {col} FROM statements")
        for row in cur.fetchall():
            value = row[0]
            if value is None:
                if col != "object":
                    # Object can be null when there is a value
                    logger.error(f"{col} cannot be NULL")
                    message_count += 1
                    statements_ok = False
                continue
            if value.startswith("<") and value.endswith(">"):
                iri = value.lstrip("<").rstrip(">")
                for prefix, base in prefixes.items():
                    if iri.startswith(base):
                        logger.warning(f"{col} '{value}' can use prefix '{prefix}'")
                        message_count += 1
                continue
            if ":" not in value:
                logger.error(f"{col} '{value}' is not a valid CURIE")
                message_count += 1
                statements_ok = False
                continue
            if value.startswith("_:"):
                if col == "predicate":
                    # Predicate should never be blank node, everything else is OK
                    logger.error(f"{col} '{value}' should be a named entity")
                    message_count += 1
                    statements_ok = False
                continue
            prefix = value.split(":")[0]
            if prefix not in prefixes:
                logger.error(f"{col} '{value}' does not have a valid prefix")
                message_count += 1
                statements_ok = False
    return statements_ok


def get_postgres_columns(cur, table):
    """Get a dictionary of column name to its type from a PostgreSQL database."""
    cur.execute(f"SELECT * FROM {table} LIMIT 0")
    columns = {}
    for desc in cur.description:
        type_oid = desc[1]
        cur.execute(f"SELECT typname FROM pg_type WHERE oid = {type_oid}")
        res = cur.fetchone()
        if res:
            typename = res[0].upper()
        else:
            typename = None
        columns[desc[0]] = typename
    return columns


if __name__ == "__main__":
    main()
