import csv
import io
import logging
import re
import sqlite3
import sys

from argparse import ArgumentParser
from collections import defaultdict

from .helpers import add_labels, dict_factory, get_ids, get_terms
from .hiccup import render

"""
Usage: python3 -m gizmos.export -d <sqlite-database> -t <term-curie> > <output-file>

Creates a TSV output with the term details where the fields are the predicates.

The sqlite-database must be created by RDFTab (https://github.com/ontodev/rdftab.rs)
and include 'statements' and 'prefixes' tables.

You may specify multiple CURIEs to extract with `-T <file>`/`--terms <file>`
where the file contains a list of CURIEs to extract.

You may also specify which predicates you would like to include with
`-p <curie>`/`--predicate <curie>` or `-P <file>`/`--predicates <file>`
where the file contains a list of predicate CURIEs.

You can optionally specify a different format:
- CSV
- JSON
- HTML

When there is more than one value for a predicate, the values will be separated by a comma 
(unless you are writing to JSON, in which case it will be an array). 
If you want to split on a different character, use `-s <split>`

Finally, if writing to TSV, CSV, or HTML, you can also pass -n/--no-headers 
to exclude the table headers.
"""


def main():
    p = ArgumentParser()
    p.add_argument("-d", "--database", required=True, help="SQLite database")
    p.add_argument("-t", "--term", action="append", help="CURIE or label of term to extract")
    p.add_argument(
        "-T", "--terms", help="File containing CURIES or labels of terms to extract",
    )
    p.add_argument(
        "-p", "--predicate", action="append", help="CURIE or label of predicate to include",
    )
    p.add_argument(
        "-P", "--predicates", help="File containing CURIEs or labels of predicates to include",
    )
    p.add_argument("-f", "--format", help="Output format (tsv, csv, html)", default="tsv")
    p.add_argument("-s", "--split", help="Character to split multiple values on", default="|")
    p.add_argument("-c", "--contents-only", action="store_true", help="If provided with HTML format, render HTML without roots")
    p.add_argument("-V", "--values", help="Default value format for cell values", default="IRI")
    p.add_argument(
        "-n",
        "--no-headers",
        action="store_true",
        help="If provided, do not include headers in output table",
    )
    args = p.parse_args()
    sys.stdout.write(export(args))


def export(args):
    """Wrapper for export_terms."""
    terms = get_terms(args.term, args.terms)
    if not terms:
        logging.critical("One or more term(s) must be specified with --term or --terms")
        sys.exit(1)
    predicates = get_terms(args.predicate, args.predicates)
    return export_terms(
        args.database, terms, predicates, args.format, standalone=not args.contents_only, split=args.split, no_headers=args.no_headers
    )


def get_html_value(value_format, predicate_id, vo):
    """Return a hiccup-style HTML href or simple string for a value or object dictionary based on
    the value format. The href will only be returned if the dictionary has an 'iri' key."""
    if "value" in vo:
        return ["p", {"property": predicate_id}, vo["value"]]
    elif value_format == "label":
        iri = vo.get("iri")
        text = vo.get("label") or vo["id"]
    elif value_format == "curie":
        iri = vo.get("iri")
        text = vo["id"]
    else:
        iri = vo.get("iri")
        text = iri
    if predicate_id not in ["CURIE", "IRI", "label"]:
        return ["p", ["a", {"property": predicate_id, "resource": vo["id"], "href": iri}, text]]
    if predicate_id == "label":
        return ["p", {"property": "rdfs:label"}, text]
    return ["a", {"href": iri}, text]


def get_iri(prefixes, term):
    """Get the IRI from a CURIE."""
    if term.startswith("<"):
        return term.lstrip("<").rstrip(">")
    prefix = term.split(":")[0]
    namespace = prefixes.get(prefix)
    if not namespace:
        raise Exception(f"Prefix '{prefix}' is not defined in prefix table")
    local_id = term.split(":")[1]
    return namespace + local_id


def get_objects(cur, prefixes, term, predicate_ids):
    """Get a dict of predicate label -> objects. The object will either be the term ID or label,
    when the label exists."""
    predicates_str = ", ".join(
        [f"'{x}'" for x in predicate_ids.keys() if x not in ["CURIE", "IRI", "label"]]
    )
    term_objects = defaultdict(list)
    cur.execute(
        f"""SELECT DISTINCT predicate, s.object AS object, l.label AS object_label
            FROM statements s JOIN labels l ON s.object = l.term
            WHERE s.subject = '{term}' AND s.predicate IN ({predicates_str})"""
    )
    for row in cur.fetchall():
        p = row["predicate"]
        p_label = predicate_ids[p]
        if p_label not in term_objects:
            term_objects[p_label] = list()

        obj = row["object"]
        if obj.startswith("_:"):
            # TODO - handle blank nodes
            continue
        obj_label = row["object_label"]

        d = {"id": obj, "iri": get_iri(prefixes, term)}
        # Maybe add the label
        if obj != obj_label:
            d["label"] = obj_label
        term_objects[p_label].append(d)
    return term_objects


def get_predicate_ids(cur, id_or_labels=None):
    """Create a map of predicate label or full header (if the header has a value format) -> ID."""
    predicate_ids = {}
    if id_or_labels:
        for id_or_label in id_or_labels:
            m = re.match(r"(.+) \[.+]$", id_or_label)
            if m:
                id_or_label = m.group(1)
            if id_or_label in ["CURIE", "IRI", "label"]:
                predicate_ids[id_or_label] = id_or_label
                continue
            cur.execute(f"SELECT term FROM labels WHERE label = '{id_or_label}'")
            res = cur.fetchone()
            if res:
                predicate_ids[res["term"]] = id_or_label
            else:
                # Make sure this exists as an ID
                cur.execute(f"SELECT label FROM labels WHERE term = '{id_or_label}'")
                res = cur.fetchone()
                if res:
                    predicate_ids[id_or_label] = id_or_label
                else:
                    logging.warning(f"'{id_or_label}' does not exist in database")
        return predicate_ids

    cur.execute(
        """SELECT DISTINCT s.predicate AS term, l.label AS label
           FROM statements s JOIN labels l ON s.predicate = l.term"""
    )
    for row in cur.fetchall():
        predicate_ids[row["term"]] = row["label"]
    if "rdf:type" in predicate_ids:
        del predicate_ids["rdf:type"]
    return predicate_ids


def get_string_value(value_format, vo):
    """Return a string from a value or object dictionary based on the value format."""
    if "value" in vo:
        return vo["value"]
    elif value_format == "label":
        # Label or CURIE (when no label)
        return vo.get("label") or vo["id"]
    elif value_format == "curie":
        # Always the CURIE
        return vo["id"]
    # IRI or CURIE (when no IRI, which shouldn't happen)
    return vo.get("iri") or vo["id"]


def get_term_details(cur, prefixes, term, predicate_ids):
    """Get a dict of predicate label -> object or value."""
    term_details = {}

    # Handle special cases
    cur.execute(f"SELECT label FROM labels WHERE term = '{term}'")
    res = cur.fetchone()
    base_dict = {"id": term, "iri": get_iri(prefixes, term)}
    if res:
        base_dict["label"] = res["label"]
    if "CURIE" in predicate_ids:
        term_details["CURIE"] = base_dict
    if "IRI" in predicate_ids:
        term_details["IRI"] = base_dict
    if "label" in predicate_ids:
        term_details["label"] = base_dict

    # Get all details
    term_details.update(get_values(cur, term, predicate_ids))
    term_details.update(get_objects(cur, prefixes, term, predicate_ids))

    # Format predicates with multiple values - a single value should not be an array
    term_details_fixed = {}
    for predicate, values in term_details.items():
        if len(values) == 1:
            term_details_fixed[predicate] = values[0]
        else:
            term_details_fixed[predicate] = values
    return term_details_fixed


def get_values(cur, term, predicate_ids):
    """Get a dict of predicate label -> literal values."""
    predicates_str = ", ".join(
        [f"'{x}'" for x in predicate_ids.keys() if x not in ["CURIE", "IRI", "label"]]
    )
    term_values = defaultdict(list)
    cur.execute(
        f"""SELECT DISTINCT predicate, value FROM statements s
            WHERE subject = '{term}' AND predicate IN ({predicates_str}) AND value IS NOT NULL"""
    )
    for row in cur.fetchall():
        p = row["predicate"]
        p_label = predicate_ids[p]
        value = row["value"]
        if value:
            if p_label not in term_values:
                term_values[p_label] = list()
            term_values[p_label].append({"value": value})
    return term_values


def render_html(prefixes, value_formats, predicate_ids, details, standalone=True, no_headers=False):
    """Render an HTML table."""
    predicate_labels = {v: k for k,v in predicate_ids.items()}
    # Create the prefix element
    pref_strs = []
    for prefix, base in prefixes.items():
        pref_strs.append(f"{prefix}: {base}")
    pref_str = "\n".join(pref_strs)
    table = ["table", {"class": "table table-striped", "prefix": pref_str}]

    # Get headers - in order
    headers = []
    for k in value_formats.keys():
        headers.append(k)

    if not no_headers:
        # Table headers
        thead = ["thead"]
        tr = ["tr"]
        for h in headers:
            tr.append(["th", h])
        thead.append(tr)
        table.append(thead)

    # Table body
    tbody = ["tbody"]
    for term, detail in details.items():
        tr = ["tr", {"resource": term}]
        for h in headers:
            m = re.match(r"(.+) \[.+]", h)
            if m:
                pred_label = m.group(1)
            else:
                pred_label = h

            predicate_id = predicate_labels[pred_label]
            value_format = value_formats[h]
            vo_list = detail.get(pred_label)
            if not vo_list:
                tr.append(["td"])
                continue
            if isinstance(vo_list, list):
                items = []
                for vo in vo_list:
                    items.append(get_html_value(value_format, predicate_id, vo))
                ele = ["td"] + items
                tr.append(ele)
            else:
                display = get_html_value(value_format, predicate_id, vo_list)
                if isinstance(display, str):
                    if predicate_id == "label":
                        predicate_id = "rdfs:label"
                    display = ["p", {"property": predicate_id}, display]
                tr.append(["td", display])
        tbody.append(tr)
    table.append(tbody)

    # Render full HTML
    if standalone:
        # HTML Headers & CSS
        head = [
            "head",
            ["meta", {"charset": "utf-8"}],
            [
                "meta",
                {
                    "name": "viewport",
                    "content": "width=device-width, initial-scale=1, shrink-to-fit=no",
                },
            ],
            [
                "link",
                {
                    "rel": "stylesheet",
                    "href": "https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css",
                    "crossorigin": "anonymous",
                },
            ],
        ]
        html = ["html", head, ["body", table]]
    else:
        html = table
    return render(None, html)


def render_output(
    prefixes, value_formats, predicate_ids, details, fmt, split="|", standalone=True, no_headers=False
):
    """Render the string output based on the format."""
    if fmt == "tsv":
        return render_table(value_formats, details, "\t", split=split, no_headers=no_headers)
    elif fmt == "csv":
        return render_table(value_formats, details, ",", split=split, no_headers=no_headers)
    elif fmt == "html":
        return render_html(prefixes, value_formats, predicate_ids, details, standalone=standalone, no_headers=no_headers)
    else:
        raise Exception("Invalid format: " + fmt)


def render_table(value_formats, details, separator, split="|", no_headers=False):
    """Render a TSV or CSV table."""
    # First fix the output to be writable by DictWriter
    rows = []
    for d in details.values():
        row = {}
        for header, value_format in value_formats.items():
            m = re.match(r"(.+) \[.+]", header)
            if m:
                pred_label = m.group(1)
            else:
                pred_label = header
            value = d.get(pred_label)
            if not value:
                continue
            if isinstance(value, list):
                items = []
                for itm in value:
                    items.append(get_string_value(value_format, itm))
                value = split.join(items)
                row[header] = value
            else:
                row[header] = get_string_value(value_format, value)
        rows.append(row)

    # Then get headers - in order
    headers = []
    for k in value_formats.keys():
        headers.append(k)

    # Finally write to string
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers, delimiter=separator, lineterminator="\n")
    if not no_headers:
        writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def export_terms(
    database, terms, predicates, fmt, split="|", standalone=True, default_value_format="IRI", no_headers=False
):
    """Retrieve details for given terms and render in the given format."""

    # Validate default format
    if default_value_format not in ["CURIE", "IRI", "label"]:
        raise Exception(
            f"The default value format ('{default_value_format}') must be one of: CURIE, IRI, label"
        )
    # Validate output format
    if fmt.lower() not in ["tsv", "csv", "html"]:
        raise Exception(f"Output format '{fmt}' must be one of: tsv, csv, html")

    details = {}
    # Check if writing to JSON - the dict output will be different
    with sqlite3.connect(database) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()

        # Create a tmp labels table
        cur.execute("ATTACH DATABASE '' AS tmp")
        add_labels(cur)

        term_ids = get_ids(cur, terms)

        if not predicates:
            # Get all predicates if not provided
            predicate_ids = {default_value_format: default_value_format}
            value_formats = {default_value_format: default_value_format.lower()}
            predicate_ids.update(get_predicate_ids(cur))
            predicate_id_str = ", ".join([f"'{x}'" for x in predicate_ids.keys()])
            cur.execute(f"SELECT DISTINCT label FROM labels WHERE term IN ({predicate_id_str})")
            for row in cur.fetchall():
                value_formats[row["label"]] = default_value_format.lower()

        else:
            # Current predicates are IDs or labels - make sure we get all the IDs
            predicate_ids = get_predicate_ids(cur, predicates)
            value_formats = {}
            for p in predicates:
                if p in ["CURIE", "IRI", "label"]:
                    value_format = p.lower()
                else:
                    value_format = default_value_format.lower()
                    m = re.match(r".+ \[(.+)]$", p)
                    if m:
                        value_format = m.group(1).lower()
                value_formats[p] = value_format

        # Get prefixes
        prefixes = {}
        cur.execute(f"SELECT DISTINCT prefix, base FROM prefix")
        for row in cur.fetchall():
            prefixes[row["prefix"]] = row["base"]

        # Get the term details
        for term in term_ids:
            term_details = get_term_details(cur, prefixes, term, predicate_ids)
            details[term] = term_details

    return render_output(
        prefixes, value_formats, predicate_ids, details, fmt, split=split, standalone=standalone, no_headers=no_headers
    )


if __name__ == "__main__":
    main()
