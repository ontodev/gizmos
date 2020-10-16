import json
import sqlite3
import sys

from argparse import ArgumentParser
from .helpers import dict_factory


def main():
    p = ArgumentParser()
    p.add_argument("db", help="SQLite database to search for labels")
    p.add_argument("text", nargs="?", help="Text to search")
    p.add_argument("-l", "--limit", help="Limit for number of results", type=int, default=30)
    args = p.parse_args()

    names = get_names(args.db, args.text, args.limit)
    output = "Content-Type: application/json\n\n"
    output += json.dumps(names, indent=4)
    sys.stdout.write(output)


def get_names(db_path, text, limit):
    """Return a list of name details.
    Each item in the list is a dict containing 'display_name' (label) and 'value' (CURIE)."""
    names = []
    with sqlite3.connect(db_path, uri=True) as conn:
        conn.row_factory = dict_factory
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
            names.append({"display_name": res["value"], "value": res["subject"]})
    return names


if __name__ == "__main__":
    main()
