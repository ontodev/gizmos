import csv
import logging
import sys

from argparse import ArgumentParser
from collections import defaultdict, OrderedDict
from io import StringIO
from typing import List

from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .helpers import get_ancestors, get_children, get_connection, get_descendants, get_parents


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "-d", "--database", required=True, help="Database file (.db) or configuration (.ini)"
    )
    parser.add_argument(
        "-i", "--imports", required=True, help="TSV or CSV file containing import module details"
    )
    parser.add_argument(
        "-I",
        "--intermediates",
        help="Included ancestor/descendant intermediates (default: all)",
        default="all",
    )
    parser.add_argument(
        "-L",
        "--limit",
        help="Max number of terms to display as reason (default: 3)",
        type=int,
        default=3,
    )
    parser.add_argument("-f", "--format", help="Output table format (default: tsv)", default="tsv")
    args = parser.parse_args()
    sys.stdout.write(run_expand(args))


def run_expand(args):
    conn = get_connection(args.database)
    sep = "\t"
    if args.imports.endswith(".csv"):
        sep = ","

    with open(args.imports, "r") as f:
        reader = csv.DictReader(f, delimiter=sep)
        explicit_rows = expand(conn, list(reader), intermediates=args.intermediates, limit=args.limit)

    out = StringIO()
    sep = "\t"
    if args.format == "csv":
        sep = ","
    elif args.format != "tsv":
        sep = "\t"
        logging.warning(f"Unknown output format ({args.format}) - output will be written as TSV")
    headers = list(explicit_rows[0].keys())
    if "Related" in headers:
        headers.remove("Related")
    writer = csv.DictWriter(out, delimiter=sep, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(explicit_rows)
    return out.getvalue()


def expand(conn: Connection, rows: List[dict], intermediates="all", limit=3) -> List[dict]:
    # dict of term ID -> row from import table
    terms = {}
    # track row number
    i = 1
    # get headers for writing table
    headers = []
    # create terms dict & get headers
    for row in rows:
        i += 1
        row["Reason"] = "defined in input"
        terms[row["ID"]] = dict(row)
        if not headers:
            headers = list(row.keys())
    headers.remove("Related")

    # create dict of explict terms and the reason(s) they are included
    explicit_terms = {}
    for term_id, row in terms.items():
        related = row.get("Related")
        if not related:
            continue
        label = row.get("Label")
        for rel in related.split(","):
            rel = rel.strip()
            if rel == "ancestors":
                ancestors = get_ancestors(conn, term_id, set(terms.keys()), intermediates)
                if term_id in ancestors:
                    # remove self relation
                    ancestors.remove(term_id)
                for a in ancestors:
                    explicit_terms = update_explicit_terms(label or term_id, a, explicit_terms, "ancestor_of")
            elif rel == "children":
                children = get_children(conn, term_id)
                for c in children:
                    explicit_terms = update_explicit_terms(label or term_id, c, explicit_terms, "child_of")
            elif rel == "descendants":
                descendants = get_descendants(conn, term_id, intermediates)
                if term_id in descendants:
                    # remove self relation
                    descendants.remove(term_id)
                for d in descendants:
                    explicit_terms = update_explicit_terms(label or term_id, d, explicit_terms, "descendant_of")
            elif rel == "parents":
                parents = get_parents(conn, term_id)
                for p in parents:
                    explicit_terms = update_explicit_terms(label or term_id, p, explicit_terms, "parent_of")
            else:
                raise Exception(f"Unknown relation for {term_id} on row {i}: {rel}")

    # add explicit terms to all terms dict
    for term_id, reasons in explicit_terms.items():
        row = {"ID": term_id}
        if term_id in terms:
            continue
        if "Label" in headers:
            query = sql_text(
                "SELECT value FROM statements WHERE subject = :term_id AND predicate = 'rdfs:label'"
            )
            res = conn.execute(query, term_id=term_id).fetchone()
            if res:
                row["Label"] = res["value"]
        reason_str = []
        if "ancestor_of" in reasons:
            reason_str.append(create_reason_str(reasons, "ancestor", limit=limit))
        if "child_of" in reasons:
            reason_str.append(create_reason_str(reasons, "child", limit=limit))
        if "descendant_of" in reasons:
            reason_str.append(create_reason_str(reasons, "descendant", limit=limit))
        if "parent_of" in reasons:
            reason_str.append(create_reason_str(reasons, "parent", limit=limit))
        row["Reason"] = " & ".join(reason_str)
        terms[term_id] = row

    return list(OrderedDict(sorted(terms.items())).values())


def create_reason_str(reasons: dict, relation: str, limit: int = 3):
    """Return a string defining the reason a term is included in the explict output."""
    ancestor_of = reasons[f"{relation}_of"]
    if len(ancestor_of) > limit:
        return f"{relation} of {len(ancestor_of)} terms"
    return f"{relation} of " + ", ".join(ancestor_of)


def update_explicit_terms(
    term_id_or_label: str, related_term_id: str, explicit_terms: dict, key: str
):
    """Update the explict terms dictionary with the relation between the related term and the given term."""
    # check if this term already exists, and get existing relations if so
    term_dict = defaultdict(set)
    if related_term_id in explicit_terms:
        term_dict = explicit_terms[related_term_id]
    if key not in term_dict:
        term_dict[key] = set()

    # add the term either by label or ID
    if " " in term_id_or_label:
        term_dict[key].add(f"'{term_id_or_label}'")
    else:
        term_dict[key].add(term_id_or_label)

    # update the master dict
    explicit_terms[related_term_id] = term_dict
    return explicit_terms


if __name__ == "__main__":
    main()
