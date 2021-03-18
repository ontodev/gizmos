import json
import sys

from argparse import ArgumentParser
from .helpers import get_connection


def main():
    p = ArgumentParser()
    p.add_argument("db", help="Database file (.db) or configuration (.ini)")
    p.add_argument("text", nargs="?", help="Text to search")
    p.add_argument("-l", "--limit", help="Limit for number of results", type=int, default=30)
    args = p.parse_args()
    conn = get_connection(args.db)
    sys.stdout.write(search(conn, args.text, args.limit))


def search(conn, text, limit=30):
    names = get_names(conn, text, limit)
    return json.dumps(names, indent=4)


def get_names(conn, text, limit):
    """Return a list of name details.
    Each item in the list is a dict containing 'display_name' (label) and 'value' (CURIE)."""
    names = []
    cur = conn.cursor()
    if text:
        cur.execute(
            f"""SELECT DISTINCT subject, value
                        FROM statements
                        WHERE predicate = "rdfs:label"
                        AND value LIKE "%{text}%"
                        ORDER BY length(value)
                        LIMIT {limit}"""
        )
    else:
        cur.execute(
            f"""SELECT DISTINCT subject, value
                        FROM statements
                        WHERE predicate = "rdfs:label"
                        ORDER BY length(value)
                        LIMIT {limit}"""
        )
    for res in cur.fetchall():
        names.append({"display_name": res[1], "value": res[0]})
    return names


if __name__ == "__main__":
    main()
