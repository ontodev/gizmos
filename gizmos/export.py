import csv
import io
import re
import sys

from argparse import ArgumentParser, Namespace
from collections import defaultdict
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text

from .helpers import get_connection, get_ids, get_terms
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
    p.add_argument(
        "-d", "--database", required=True, help="Database file (.db) or configuration (.ini)"
    )
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
    p.add_argument(
        "-c",
        "--contents-only",
        action="store_true",
        help="If provided with HTML format, render HTML without roots",
    )
    p.add_argument("-V", "--values", help="Default value format for cell values", default="IRI")
    p.add_argument("-w", "--where", help="SQL WHERE statement to include when selecting terms")
    p.add_argument(
        "-n",
        "--no-headers",
        action="store_true",
        help="If provided, do not include headers in output table",
    )
    args = p.parse_args()
    sys.stdout.write(run_export(args))


def run_export(args: Namespace) -> str:
    """Wrapper for export_terms."""
    terms = get_terms(args.term, args.terms)
    predicates = get_terms(args.predicate, args.predicates)
    conn = get_connection(args.database)
    return export(
        conn,
        terms,
        predicates,
        args.format,
        default_value_format=args.values,
        standalone=not args.contents_only,
        split=args.split,
        no_headers=args.no_headers,
        where=args.where,
    )


def get_html_value(value_format: str, predicate_id: str, vo: dict) -> list:
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


def get_iri(prefixes: dict, term: str) -> str:
    """Get the IRI from a CURIE."""
    if term.startswith("<"):
        return term.lstrip("<").rstrip(">")
    prefix = term.split(":")[0]
    namespace = prefixes.get(prefix)
    if not namespace:
        raise Exception(f"Prefix '{prefix}' is not defined in prefix table")
    local_id = term.split(":")[1]
    return namespace + local_id


def get_objects(
    conn: Connection, prefixes: dict, term_ids: list, predicate_ids: dict, statements: str = "statements"
) -> dict:
    """Get a dict of predicate label -> objects. The object will either be the term ID or label,
    when the label exists."""
    predicates = [x for x in predicate_ids.keys() if x not in ["CURIE", "IRI", "label"]]
    term_objects = defaultdict(dict)
    query = sql_text(
        f"""SELECT DISTINCT subject, predicate, object
        FROM "{statements}" WHERE subject IN :terms
            AND predicate IN :predicates
            AND object NOT LIKE '_:%'"""
    ).bindparams(bindparam("predicates", expanding=True), bindparam("terms", expanding=True))
    results = conn.execute(query, terms=term_ids, predicates=predicates).fetchall()
    # Get the labels for any objects
    objects = [res["object"] for res in results]
    query = sql_text(
        f"""SELECT DISTINCT subject, value FROM "{statements}"
        WHERE subject IN :terms AND predicate = 'rdfs:label'"""
    ).bindparams(bindparam("terms", expanding=True))
    object_labels = {res["subject"]: res["value"] for res in conn.execute(query, terms=objects).fetchall()}
    for res in results:
        s = res["subject"]
        if s not in term_objects:
            term_objects[s] = defaultdict(list)
        p = res["predicate"]
        p_label = predicate_ids[p] or p
        if p_label not in term_objects[s]:
            term_objects[s][p_label] = list()

        obj = res["object"]
        obj_label = object_labels.get(obj, obj)

        d = {"id": obj}
        if prefixes:
            d["iri"] = get_iri(prefixes, s)
        # Maybe add the label
        if obj_label:
            d["label"] = obj_label
        term_objects[s][p_label].append(d)
    return term_objects


def get_predicate_ids(
    conn: Connection, id_or_labels: list = None, statements: str = "statements"
) -> dict:
    """"""
    predicate_ids = {}
    if id_or_labels:
        # Subset of predicates was specified, just get the ID -> label map for these
        id_or_labels_trimmed = []
        for id_or_label in id_or_labels:
            m = re.match(r"(.+) \[.+]$", id_or_label)
            if m:
                id_or_label = m.group(1)
            if id_or_label in ["CURIE", "IRI", "label"]:
                predicate_ids[id_or_label] = id_or_label
                continue
            id_or_labels_trimmed.append(id_or_label)
        predicate_ids.update(
            get_ids(conn, id_or_labels_trimmed, statements=statements, id_type="predicate")
        )
        return predicate_ids

    # Otherwise, get all predicate IDs -> labels
    results = conn.execute(
        f"""WITH labels AS (
                SELECT DISTINCT subject, value
                FROM statements WHERE predicate = 'rdfs:label'
            )
            SELECT DISTINCT
                s.predicate AS subject,
                l.value AS value
            FROM "{statements}" s
            LEFT JOIN labels l ON s.predicate = l.subject;"""
    ).fetchall()
    for res in results:
        predicate_ids[res["subject"]] = res["value"]
    if "rdf:type" in predicate_ids:
        del predicate_ids["rdf:type"]
    return predicate_ids


def get_string_value(value_format: str, vo: dict) -> str:
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


def get_term_details(
    conn: Connection, term_ids: list, predicate_ids: dict, prefixes: dict = None, statements: str = "statements"
) -> dict:
    """Get a dict of predicate label -> object or value."""
    term_details = {}

    # Get all details
    term_details.update(get_values(conn, term_ids, predicate_ids, statements=statements))
    term_details.update(get_objects(conn, prefixes, term_ids, predicate_ids, statements=statements))

    for t in term_ids:
        # Handle special cases
        base_dict = {"id": t}
        if prefixes:
            base_dict["iri"] = get_iri(prefixes, t)
        query = sql_text(
            f"SELECT value FROM \"{statements}\" WHERE subject = :term AND predicate = 'rdfs:label'"
        )
        res = conn.execute(query, term=t).fetchone()
        if res:
            base_dict["label"] = res["value"]
        if t not in term_details:
            term_details[t] = defaultdict(dict)
        if "CURIE" in predicate_ids:
            term_details[t]["CURIE"] = base_dict
        if "IRI" in predicate_ids:
            term_details[t]["IRI"] = base_dict
        if "label" in predicate_ids:
            term_details[t]["label"] = base_dict
    return term_details


def get_values(
    conn: Connection, term_ids: list, predicate_ids: dict, statements: str = "statements"
) -> dict:
    """Get a dict of predicate label -> literal values."""
    predicates = [x for x in predicate_ids.keys() if x not in ["CURIE", "IRI", "label"]]
    term_values = defaultdict(dict)
    query = sql_text(
        f"""SELECT DISTINCT subject, predicate, value, datatype, language FROM {statements} s
            WHERE subject IN :terms AND predicate IN :predicates AND value IS NOT NULL"""
    ).bindparams(bindparam("predicates", expanding=True), bindparam("terms", expanding=True))
    result = conn.execute(query, terms=term_ids, predicates=predicates)
    for res in result:
        s = res["subject"]
        if s not in term_values:
            term_values[s] = defaultdict(list)
        p = res["predicate"]
        p_label = predicate_ids[p] or p
        value = res["value"]
        if value:
            if p_label not in term_values[s]:
                term_values[s][p_label] = list()
            term_values[s][p_label].append({"value": value, "datatype": res["datatype"], "language": res["language"]})
    return term_values


def render_html(
    prefixes: dict,
    value_formats: dict,
    predicate_ids: dict,
    details: dict,
    standalone: bool = True,
    no_headers: bool = False,
) -> str:
    """Render an HTML table."""
    predicate_labels = {v: k for k, v in predicate_ids.items()}
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

            predicate_id = predicate_labels.get(pred_label)
            value_format = value_formats.get(h)
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
    return render([], html)


def render_output(
    prefixes: dict,
    value_formats: dict,
    predicate_ids: dict,
    details: dict,
    fmt: str,
    split: str = "|",
    standalone: bool = True,
    no_headers: bool = False,
) -> str:
    """Render the string output based on the format."""
    if fmt == "tsv":
        return render_table(value_formats, details, "\t", split=split, no_headers=no_headers)
    elif fmt == "csv":
        return render_table(value_formats, details, ",", split=split, no_headers=no_headers)
    elif fmt == "html":
        return render_html(
            prefixes,
            value_formats,
            predicate_ids,
            details,
            standalone=standalone,
            no_headers=no_headers,
        )
    else:
        raise Exception("Invalid format: " + fmt)


def render_table(
    value_formats: dict, details: dict, separator: str, split: str = "|", no_headers: bool = False
) -> str:
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


def export(
    conn: Connection,
    terms: list,
    predicates: list,
    fmt: str,
    default_value_format: str = "IRI",
    no_headers: bool = False,
    split: str = "|",
    standalone: bool = True,
    statements: str = "statements",
    where: str = None,
) -> str:
    """Retrieve details for given terms and render in the given format.

    :param conn: SQLAlchemy database connection
    :param terms: list of terms to export (by ID or label)
    :param predicates: list of properties to include in export
    :param fmt: output format of export (tsv, csv, or html)
    :param default_value_format: how values should be rendered (IRI, CURIE, or label)
    :param no_headers: if true, do not include the header row in export
    :param split: character to split multiple values on in single cell
    :param standalone: if true and format is HTML, include HTML headers
    :param statements: name of the ontology statements table
    :param where: SQL WHERE statement to include in query to get terms
    :return: string export in given format
    """

    # Validate default format
    if default_value_format not in ["CURIE", "IRI", "label"]:
        raise Exception(
            f"The default value format ('{default_value_format}') must be one of: CURIE, IRI, label"
        )
    # Validate output format
    if fmt.lower() not in ["tsv", "csv", "html"]:
        raise Exception(f"Output format '{fmt}' must be one of: tsv, csv, html")

    details = {}

    if terms:
        term_ids = get_ids(conn, terms)
    else:
        term_ids = []
        if where:
            # Use provided query filter to select terms
            query = f"SELECT DISTINCT stanza FROM {statements} WHERE " + where
        else:
            # Get all, excluding blank nodes
            query = f"SELECT DISTINCT stanza FROM {statements} WHERE stanza NOT LIKE '_:%%'"
        for res in conn.execute(query):
            term_ids.append(res["stanza"])

    if not predicates:
        # Get all predicates if not provided
        predicate_ids = {default_value_format: default_value_format}
        predicate_ids.update(
            {
                pid: label or pid
                for pid, label in get_predicate_ids(conn, statements=statements).items()
            }
        )
        value_formats = {label: default_value_format.lower() for label in predicate_ids.values()}
        value_formats[default_value_format] = default_value_format.lower()

    else:
        # Current predicates are IDs or labels - make sure we get all the IDs
        predicate_ids = get_predicate_ids(conn, predicates, statements=statements)
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
    for row in conn.execute(f"SELECT DISTINCT prefix, base FROM prefix"):
        prefixes[row["prefix"]] = row["base"]

    # Get the term details
    details = get_term_details(conn, term_ids, predicate_ids, prefixes=prefixes, statements=statements)

    return render_output(
        prefixes,
        value_formats,
        predicate_ids,
        details,
        fmt,
        split=split,
        standalone=standalone,
        no_headers=no_headers,
    )


if __name__ == "__main__":
    main()
