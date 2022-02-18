import json
import sys

from argparse import ArgumentParser
from collections import defaultdict
from typing import Optional

from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .helpers import get_children, get_connection, get_descendants


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
    search_text: str = "",
    label: str = "rdfs:label",
    limit: Optional[int] = 30,
    other_annotations: list = None,
    short_label: str = None,
    statements: str = "statements",
    synonyms: list = None,
    terms: list = None,
) -> list:
    """Return a list containing search results. Each search result has:
    - id
    - label
    - short_label
    - synonym
    - property
    - order

    :param conn: database connection to query
    :param search_text: substring to match
    :param terms: IDs of terms to restrict search results to
    :param label: property for label annotations
    :param short_label: property for short label annotations
    :param synonyms: list of properties for synonym annotations
    :param other_annotations:
    :param limit: max number of search results to return
    :param statements: name of the statements table (default: statements)"""
    names = defaultdict(dict)
    if not search_text and not terms:
        # Nothing to search, no results
        return []

    stanza_in = None
    if terms:
        stanza_in = " AND stanza IN (" + ", ".join([f"'{x}'" for x in terms]) + ")"

    # Get labels
    query = f"""SELECT DISTINCT stanza, value FROM {statements}
    WHERE predicate = :label AND lower(value) LIKE :text"""
    if stanza_in:
        query += stanza_in
    query = sql_text(query)
    results = conn.execute(query, label=label, text=f"%%{search_text.lower()}%%")
    for res in results:
        term_id = res["stanza"]
        if term_id not in names:
            names[term_id] = dict()
        names[term_id]["label"] = res["value"]

    # Get short labels
    if short_label:
        if short_label.lower() == "id":
            query = f"SELECT DISTINCT stanza FROM {statements} WHERE lower(stanza) LIKE :text"
            if stanza_in:
                query += stanza_in
            query = sql_text(query)
            results = conn.execute(query, text=f"%%{search_text.lower()}%%")
            for res in results:
                term_id = res["stanza"]
                if term_id not in names:
                    names[term_id] = dict()
                if term_id.startswith("<") and term_id.endswith(">"):
                    term_id = term_id[1:-1]
                names[term_id]["short_label"] = term_id
        else:
            query = f"""SELECT DISTINCT stanza, value FROM {statements}
            WHERE predicate = :short_label AND lower(value) LIKE :text"""
            if stanza_in:
                query += stanza_in
            query = sql_text(query)
            results = conn.execute(
                query, short_label=short_label, text=f"%%{search_text.lower()}%%"
            )
            for res in results:
                term_id = res["stanza"]
                if term_id not in names:
                    names[term_id] = dict()
                names[term_id]["short_label"] = res["value"]

    # Get synonyms
    if synonyms:
        for syn in synonyms:
            query = f"""SELECT DISTINCT stanza, value FROM {statements}
            WHERE predicate = :syn AND lower(value) LIKE :text"""
            if stanza_in:
                query += stanza_in
            query = sql_text(query)
            results = conn.execute(query, syn=syn, text=f"%%{search_text.lower()}%%")
            for res in results:
                term_id = res["stanza"]
                value = res["value"]
                if term_id not in names:
                    names[term_id] = dict()
                    ts = dict()
                else:
                    ts = names[term_id].get("synonyms", dict())
                ts[value] = syn
                names[term_id]["synonyms"] = ts

    if other_annotations:
        for oa in other_annotations:
            query = f"""SELECT DISTINCT stanza, value FROM {statements}
            WHERE predicate = :oa AND lower(value) LIKE :text"""
            if stanza_in:
                query += stanza_in
            query = sql_text(query)
            results = conn.execute(query, oa=oa, text=f"%%{search_text.lower()}%%")
            for res in results:
                term_id = res["stanza"]
                value = res["value"]
                if term_id not in names:
                    names[term_id] = dict()
                    ts = []
                else:
                    ts = names[term_id].get(oa, [])
                ts.append(value)
                names[term_id][oa] = ts

    search_res = {}
    term_to_match = {}
    for term_id, details in names.items():
        matched_property = None
        term_synonym = None
        matched_value = None

        term_label = details.get("label")
        term_short_label = details.get("short_label")
        term_synonyms = details.get("synonyms", {})
        if other_annotations:
            for oa in other_annotations:
                term_other = details.get(oa)
                if term_other:
                    matched_property = oa
                    matched_value = term_other
                    break

        # Determine which property was the text that matched
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
            query = f"""SELECT DISTINCT value FROM {statements}
            WHERE predicate = :label AND stanza = :term_id"""
            if stanza_in:
                query += stanza_in
            query = sql_text(query)
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
                query = f"""SELECT DISTINCT value FROM {statements}
                WHERE predicate = :short_label AND stanza = :term_id"""
                if stanza_in:
                    query += stanza_in
                query = sql_text(query)
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
    term_to_match = sorted(term_to_match, key=lambda key: len(term_to_match[key]))
    if limit:
        term_to_match = term_to_match[:limit]
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
