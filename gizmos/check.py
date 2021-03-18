import logging
import sqlite3
import sys

from argparse import ArgumentParser


def check_prefix(cur):
    """Check the structure of the prefix table. It must have the columns 'prefix' and 'base'."""
    logger = logging.getLogger()

    # Check for required columns
    cur.execute("PRAGMA table_info(prefix)")
    columns = {x[1]: x[2] for x in cur.fetchall()}
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
        cur.execute("SELECT * FROM prefix WHERE prefix = ?", (prefix,))
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
    cur.execute("PRAGMA table_info(statements)")
    columns = {x[1]: x[2] for x in cur.fetchall()}
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
    cur.execute("PRAGMA index_list(statements)")
    for row in cur.fetchall():
        index = row[1]
        cur.execute(f"PRAGMA index_info({index})")
        col = cur.fetchone()[2]
        if col == "stanza":
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


def main():
    p = ArgumentParser()
    p.add_argument("db", help="Path to SQLite database to check")
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

    with sqlite3.connect(args.db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [x[0] for x in cur.fetchall()]

        if "prefix" not in tables:
            logger.error("missing 'prefix' table")
            prefix_ok = False
        else:
            prefix_ok = check_prefix(cur)

        if "statements" not in tables:
            logger.error("missing 'statements' table")
            statements_ok = False
        elif prefix_ok:
            statements_ok = check_statements(cur, limit=limit)

    if not statements_ok or not prefix_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
