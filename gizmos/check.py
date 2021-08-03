import logging
import sys

from argparse import ArgumentParser
from sqlalchemy.engine.base import Connection
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

    with get_connection(args.db) as conn:
        if not check(conn, limit=limit):
            sys.exit(1)


def check(conn: Connection, limit: int = 10) -> bool:
    """Check for a 'prefix' and 'statements' table in the database, then check the contents.

    :param conn: sqlalchemy database connection
    :param limit: max number of messages to log
    :return: True on success
    """
    logger = logging.getLogger()
    if str(conn.engine.url).startswith("sqlite"):
        res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    else:
        res = conn.execute(
            "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public'"
        )
    tables = [x["name"] for x in res]

    if "prefix" not in tables:
        logger.error("missing 'prefix' table")
        prefix_ok = False
    else:
        prefix_ok = check_prefix(conn)

    statements_ok = True
    if "statements" not in tables:
        logger.error("missing 'statements' table")
        statements_ok = False
    elif prefix_ok:
        statements_ok = check_statements(conn, limit=limit)

    if not statements_ok or not prefix_ok:
        return False
    return True


def check_prefix(conn: Connection) -> bool:
    """Check the structure of the prefix table. It must have the columns 'prefix' and 'base'.

    :param conn: sqlalchemy database connection
    :return: True on success"""
    logger = logging.getLogger()

    # Check for required columns
    if str(conn.engine.url).startswith("sqlite"):
        res = conn.execute("PRAGMA table_info(prefix)")
    else:
        res = conn.execute(
            """SELECT column_name AS name, data_type AS type FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_NAME = 'prefix';"""
        )
    columns = {x["name"]: x["type"] for x in res}
    missing = []
    bad_type = []
    for col in ["prefix", "base"]:
        coltype = columns.get(col).lower()
        if not coltype:
            missing.append(col)
        elif coltype != "text":
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
        res = conn.execute(f"SELECT * FROM prefix WHERE prefix = '{prefix}'").fetchone()
        if not res:
            missing_prefixes.append(prefix)
    if missing_prefixes:
        logger.error("'prefix' is missing required prefixes: " + ", ".join(missing_prefixes))

    return True


def check_statements(conn: Connection, limit: int = 10) -> bool:
    """Check the structure of the statements table then check the values of the columns.

    :param conn: sqlalchemy database connection
    :param limit: max number of messages to log
    :return: True on success"""
    logger = logging.getLogger()
    statements_ok = True

    # First check the structure
    if str(conn.engine.url).startswith("sqlite"):
        res = conn.execute("PRAGMA table_info(statements)")
    else:
        res = conn.execute(
            """SELECT column_name AS name, data_type AS type FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'statements';"""
        )
    columns = {x["name"]: x["type"] for x in res}
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
        coltype = columns.get(col).lower()
        if not coltype:
            missing.append(col)
        elif coltype != "text":
            bad_type.append(col)
    if missing:
        logger.error("'statements' is missing column(s): " + ", ".join(missing))
        return False
    if bad_type:
        logger.error("'statements' column(s) do not have type 'TEXT': " + ", ".join(bad_type))
        return False

    # Check for an index on the stanza column, warn if missing (do not fail)
    has_stanza_idx = False
    if str(conn.engine.url).startswith("sqlite"):
        for res in conn.execute("PRAGMA index_list(statements)"):
            index = res["name"]
            col_res = conn.execute(f"PRAGMA index_info({index})").fetchone()
            col = col_res.fetchone()["name"]
            if col == "stanza":
                has_stanza_idx = True
                break
    else:
        results = conn.execute(
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
        for res in results:
            if res["column_name"] == "stanza":
                has_stanza_idx = True
                break
    if not has_stanza_idx:
        logger.warning("missing index on 'stanza' column")

    # Check that no row has both an object and a value
    message_count = 0
    res = conn.execute("SELECT * FROM statements WHERE object IS NOT NULL AND value IS NOT NULL")
    invalid = len(res)
    if invalid:
        logger.error(
            f"{invalid} rows where both 'object' and 'value' have values (one must be NULL)"
        )
        message_count += 1

    # Get prefixes to check against
    res = conn.execute("SELECT prefix, base FROM prefix")
    prefixes = {x["prefix"]: x["base"] for x in res}

    # Check subjects, stanzas, predicates, and objects
    for col in ["stanza", "subject", "predicate", "object"]:
        if limit and message_count >= limit:
            # Do not exceed the limit of messages
            break
        results = conn.execute(f"SELECT {col} FROM statements")
        for res in results:
            value = res[col]
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


if __name__ == "__main__":
    main()
