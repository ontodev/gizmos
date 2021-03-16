import logging
import sqlite3
import sys

from argparse import ArgumentParser


def check_prefix(cur):
    """Check the structure of the prefix table. It must have the columns 'prefix' and 'base'."""
    logger = logging.getLogger()

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
    return True


def check_statements(cur):
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
        logger.error(
            "'statements' column(s) do not have type 'TEXT': " + ", ".join(bad_type)
        )
        return False

    # Get prefixes to check against
    cur.execute("SELECT prefix, base FROM prefix")
    prefixes = {x[0]: x[1] for x in cur.fetchall()}

    # Check subjects, stanzas, predicates, and objects
    for col in ["stanza", "subject", "predicate", "object"]:
        cur.execute(f"SELECT {col} FROM statements")
        for row in cur.fetchall():
            value = row[0]
            if value is None:
                if col != "object":
                    # Object can be null when there is a value
                    logger.error(f"{col} cannot be NULL")
                continue
            if value.startswith("<") and value.endswith(">"):
                iri = value.lstrip("<").rstrip(">")
                for prefix, base in prefixes.items():
                    if iri.startswith(base):
                        logger.warning(f"{col} '{value}' can use prefix '{prefix}'")
                continue
            if ":" not in value:
                logger.error(f"{col} '{value}' is not a valid CURIE")
                statements_ok = False
                continue
            if value.startswith("_:"):
                if col == "stanza":
                    # Stanza should never be blank node, everything else is OK
                    # TODO: should we warn on blank predicate?
                    logger.error(
                        f"{col} '{value}' should be a named entity"
                    )
                    statements_ok = False
                continue
            prefix = value.split(":")[0]
            if prefix not in prefixes:
                logger.error(
                    f"{col} '{value}' does not have a valid prefix"
                )
                statements_ok = False
    return statements_ok


def main():
    p = ArgumentParser()
    p.add_argument("db", help="Path to SQLite database to check")
    args = p.parse_args()

    # Set up logger
    logger = logging.getLogger()
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    logger.setLevel(logging.WARNING)
    ch.setLevel(logging.WARNING)
    logger.addHandler(ch)

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
            statements_ok = check_statements(cur)

    if not statements_ok or not prefix_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
