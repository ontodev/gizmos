import json
import sys

from argparse import ArgumentParser
from collections import defaultdict
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .helpers import get_connection


def main():
    p = ArgumentParser()
    p.add_argument("db", help="Database file (.db) or configuration (.ini)")
    p.add_argument("text", nargs="?", help="Text to search")
    p.add_argument(
        "-L", "--label", help="Property for labels, default rdfs:label", default="rdfs:label"
    )
    p.add_argument("-S", "--short-label", help="Property for short labels, default is excluded")
    p.add_argument(
        "-s",
        "--synonyms",
        help="A property to include as synonym, default is excluded",
        action="append",
    )
    p.add_argument("-l", "--limit", help="Limit for number of results", type=int, default=30)
    args = p.parse_args()
    conn = get_connection(args.db)
    sys.stdout.write(
        search(
            conn,
            args.text,
            label=args.label,
            short_label=args.short_label,
            synonyms=args.synonyms,
            limit=args.limit,
        )
    )


def search(
    conn: Connection,
    search_text: str,
    label: str = "rdfs:label",
    short_label: str = None,
    synonyms: list = None,
    limit: int = 30,
) -> str:
    """Return a string containing the search results in JSON format."""
    res = get_search_results(
        conn, search_text, label=label, short_label=short_label, synonyms=synonyms, limit=limit
    )
    return json.dumps(res, indent=4)


def get_search_results(
    conn: Connection,
    search_text: str,
    label: str = "rdfs:label",
    short_label: str = None,
    synonyms: list = None,
    limit: int = 30,
) -> list:
    """Return a list containing search results. Each search result has:
    - id
    - label
    - short_label
    - synonym
    - property
    - order"""
    names = defaultdict(dict)
    if search_text:
        # Get labels
        query = sql_text(
            """SELECT DISTINCT subject, value FROM statements
            WHERE predicate = :label AND lower(value) LIKE :text"""
        )
        results = conn.execute(query, label=label, text=f"%%{search_text.lower()}%%")
        for res in results:
            term_id = res["subject"]
            if term_id not in names:
                names[term_id] = dict()
            names[term_id]["label"] = res["value"]

        # Get short labels
        if short_label:
            if short_label.lower() == "id":
                query = sql_text(
                    "SELECT DISTINCT stanza FROM statements WHERE lower(stanza) LIKE :text"
                )
                results = conn.execute(query, text=f"%%{search_text.lower()}%%")
                for res in results:
                    term_id = res["stanza"]
                    if term_id not in names:
                        names[term_id] = dict()
                    if term_id.startswith("<") and term_id.endswith(">"):
                        term_id = term_id[1:-1]
                    names[term_id]["short_label"] = term_id
            else:
                query = sql_text(
                    """SELECT DISTINCT subject, value FROM statements
                    WHERE predicate = :short_label AND lower(value) LIKE :text"""
                )
                results = conn.execute(query, short_label=short_label, text=f"%%{search_text.lower()}%%")
                for res in results:
                    term_id = res["subject"]
                    if term_id not in names:
                        names[term_id] = dict()
                    names[term_id]["short_label"] = res["value"]

        # Get synonyms
        if synonyms:
            for syn in synonyms:
                query = sql_text(
                    """SELECT DISTINCT subject, value FROM statements
                    WHERE predicate = :syn AND lower(value) LIKE :text"""
                )
                results = conn.execute(query, syn=syn, text=f"%%{search_text.lower()}%%")
                for res in results:
                    term_id = res["subject"]
                    value = res["value"]
                    if term_id not in names:
                        names[term_id] = dict()
                        ts = dict()
                    else:
                        ts = names[term_id].get("synonyms", dict())
                    ts[value] = syn
                    names[term_id]["synonyms"] = ts

    else:
        # No text, no results
        return []

    search_res = {}
    term_to_match = {}
    for term_id, details in names.items():
        term_label = details.get("label")
        term_short_label = details.get("short_label")
        term_synonyms = details.get("synonyms", {})

        # Determine which property was the text that matched
        matched_property = None
        term_synonym = None
        matched_value = None
        if term_label:
            matched_property = label
            matched_value = term_label
        elif term_short_label:
            matched_property = short_label
            matched_value = term_short_label

        if term_synonyms:
            # May be more than one, but we will just grab the first and go
            term_synonym = list(term_synonyms.keys())[0]
            if not term_label and not term_short_label:
                matched_property = list(term_synonyms.values())[0]
                matched_value = term_synonym

        if not matched_property:
            # We shouldn't get here, but this means that nothing actually matched
            continue

        # Add the other, missing property values
        if not term_label:
            # Label did not match text, retrieve it to display
            query = sql_text(
                """SELECT DISTINCT value FROM statements
                WHERE predicate = :label AND stanza = :term_id"""
            )
            res = conn.execute(query, label=label, term_id=term_id).fetchone()
            if res:
                term_label = res["value"]

        if not term_short_label:
            # Short label did not match text, retrieve it to display
            if short_label and short_label.lower() == "id":
                if term_id.startswith("<") and term_id.endswith(">"):
                    term_short_label = term_id[1:-1]
                else:
                    term_short_label = term_id
            else:
                query = sql_text(
                    """SELECT DISTINCT value FROM statements
                    WHERE predicate = :short_label AND stanza = :term_id"""
                )
                res = conn.execute(query, short_label=short_label, term_id=term_id).fetchone()
                if res:
                    term_short_label = res["value"]

        term_to_match[term_id] = matched_value
        # Add results to JSON output
        search_res[term_id] = {
            "id": term_id,
            "label": term_label,
            "short_label": term_short_label,
            "synonym": term_synonym,
            "property": matched_property,
        }

    # Order the matched values by length, shortest first, regardless of matched property
    term_to_match = sorted(term_to_match, key=lambda key: len(term_to_match[key]))[:limit]
    res = []
    i = 1
    for term in term_to_match:
        details = search_res[term]
        details["order"] = i
        res.append(details)
        i += 1
    return res


if __name__ == "__main__":
    main()
