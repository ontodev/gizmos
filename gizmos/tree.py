#!/usr/bin/env python3

import logging
import os
import sys

from argparse import ArgumentParser
from collections import defaultdict
from typing import Optional

from gizmos.hiccup import render
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from .helpers import get_connection

"""
Usage: python3 tree.py <sqlite-database> <term-curie> > <html-file>

Creates an HTML page containing the tree structure of the term & its annotations.
HTML is written to stdout.

The sqlite-database must be created by RDFTab (https://github.com/ontodev/rdftab.rs)
and include 'statements' and 'prefixes' tables.

The term-curie must use a prefix from the 'prefixes' table.
"""

LOGGER = logging.getLogger("main")
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s - %(asctime)s - %(name)s - %(message)s"
)

# Plus sign to show a node has children
PLUS = [
    "svg",
    {"width": "14", "height": "14", "fill": "#808080", "viewBox": "0 0 16 16"},
    [
        "path",
        {
            "fill-rule": "evenodd",
            "d": "M8 15A7 7 0 1 0 8 1a7 7 0 0 0 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z",
        },
    ],
    [
        "path",
        {
            "fill-rule": "evenodd",
            "d": "M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 "
            + "0-1h3v-3A.5.5 0 0 1 8 4z",
        },
    ],
]

# Top levels to display in tree
TOP_LEVELS = {
    "ontology": "Ontology",
    "owl:Class": "Class",
    "owl:AnnotationProperty": "Annotation Property",
    "owl:DataProperty": "Data Property",
    "owl:ObjectProperty": "Object Property",
    "owl:Individual": "Individual",
    "rdfs:Datatype": "Datatype",
}

# Stylesheets & JS scripts
bootstrap_css = "https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css"
bootstrap_js = "https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js"
popper_js = "https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js"
typeahead_js = "https://cdnjs.cloudflare.com/ajax/libs/typeahead.js/0.11.1/typeahead.bundle.min.js"


def main():
    p = ArgumentParser("tree.py", description="create an HTML page to display an ontology term")
    p.add_argument("db", help="Database file (.db) or configuration (.ini)")
    p.add_argument("term", help="CURIE of ontology term to display", nargs="?")
    p.add_argument("-t", "--title", help="Optional tree title")
    p.add_argument("-p", "--predicate", action="append", help="CURIE of predicate to include")
    p.add_argument("-P", "--predicates", help="File containing CURIEs of predicates to include")
    p.add_argument(
        "-d",
        "--include-db",
        help="If provided, include 'db' param in query string",
        action="store_true",
    )
    p.add_argument("-H", "--href", help="Format string to convert CURIEs to tree links")
    p.add_argument(
        "-s", "--include-search", help="If provided, include a search bar", action="store_true"
    )
    p.add_argument(
        "-c",
        "--contents-only",
        action="store_true",
        help="If provided, render HTML without the roots",
    )
    args = p.parse_args()

    # Maybe get predicates to include
    predicate_ids = args.predicate or []
    if args.predicates:
        with open(args.predicates, "r") as f:
            predicate_ids.extend([x.strip() for x in f.readlines()])

    if args.href:
        href = args.href
        if "{curie}" not in href:
            raise RuntimeError("The --href argument must contain '{curie}'")
    else:
        href = "?id={curie}"
        if args.include_db:
            href += "&db={db}"

    treename = os.path.splitext(os.path.basename(args.db))[0]
    conn = get_connection(args.db)

    # Run tree and write HTML to stdout
    sys.stdout.write(
        tree(
            conn,
            treename,
            args.term,
            title=args.title,
            href=href,
            predicate_ids=predicate_ids,
            include_search=args.include_search,
            standalone=not args.contents_only,
        )
    )


def tree(
    conn: Connection,
    treename: str,
    term_id: Optional[str],
    href: str = "?id={curie}",
    title: str = None,
    predicate_ids: list = None,
    include_search: bool = False,
    standalone: bool = True,
) -> str:
    """Create an HTML/RDFa tree for the given term.
    If term_id is None, create the tree for owl:Class."""
    # Get the prefixes
    results = conn.execute("SELECT * FROM prefix ORDER BY length(base) DESC")
    all_prefixes = [(x["prefix"], x["base"]) for x in results]

    ps = set()
    body = []
    if not term_id:
        p, t = term2rdfa(conn, all_prefixes, treename, [], "owl:Class", [], title=title, href=href)
        ps.update(p)
        body.append(t)

    # Maybe find a * in the IDs that represents all remaining predicates
    predicate_ids_split = None
    if predicate_ids and "*" in predicate_ids:
        before = []
        after = []
        found = False
        for pred in predicate_ids:
            if pred == "*":
                found = True
                continue
            if not found:
                before.append(pred)
            else:
                after.append(pred)
        predicate_ids_split = [before, after]

    # Run for given terms if terms have not yet been filled out
    if not body:
        if predicate_ids and predicate_ids_split:
            # If some IDs were provided with *, add the remaining predicates
            # These properties go in between the before & after defined in the split
            rem_predicate_ids = get_sorted_predicates(conn, exclude_ids=predicate_ids)

            # Separate before & after with the remaining properties
            predicate_ids = predicate_ids_split[0]
            predicate_ids.extend(rem_predicate_ids)
            predicate_ids.extend(predicate_ids_split[1])
        elif not predicate_ids:
            predicate_ids = get_sorted_predicates(conn)

        query = sql_text(
            """SELECT stanza, subject, predicate, object, value, datatype, language
            FROM statements WHERE stanza = :term_id"""
        )
        results = conn.execute(query, term_id=term_id)
        stanza = []
        for res in results:
            stanza.append(dict(res))

        p, t = term2rdfa(
            conn, all_prefixes, treename, predicate_ids, term_id, stanza, title=title, href=href
        )
        ps.update(p)
        body.append(t)

    if not title:
        title = treename + " Browser"

    # Create the prefix element
    pref_strs = []
    for prefix, base in all_prefixes:
        pref_strs.append(f"{prefix}: {base}")
    pref_str = "\n".join(pref_strs)

    body_wrapper = ["div", {"prefix": pref_str}]
    if include_search:
        body_wrapper.append(
            [
                "div",
                {"class": "form-row mt-2 mb-2"},
                [
                    "input",
                    {
                        "id": f"statements-typeahead",
                        "class": "typeahead form-control",
                        "type": "text",
                        "value": "",
                        "placeholder": "Search",
                    },
                ],
            ]
        )
    body = body_wrapper + body

    # JQuery
    body.append(
        [
            "script",
            {
                "src": "https://code.jquery.com/jquery-3.5.1.min.js",
                "integrity": "sha256-9/aliU8dGd2tb6OSsuzixeV4y/faTqgFtohetphbbj0=",
                "crossorigin": "anonymous",
            },
        ]
    )

    if include_search:
        # Add JS imports for running search
        body.append(["script", {"type": "text/javascript", "src": popper_js}])
        body.append(["script", {"type": "text/javascript", "src": bootstrap_js}])
        body.append(["script", {"type": "text/javascript", "src": typeahead_js}])

    # Custom JS for show more children
    js = """function show_children() {
            hidden = $('#children li:hidden').slice(0, 100);
            if (hidden.length > 1) {
                hidden.show();
                setTimeout(show_children, 100);
            } else {
                console.log("DONE");
            }
            $('#more').hide();
        }"""

    # Custom JS for search bar using Typeahead
    if include_search:
        # Built the href to return when you select a term
        href_split = href.split("{curie}")
        before = href_split[0].format(db=treename)
        after = href_split[1].format(db=treename)
        js_funct = f'str.push("{before}" + encodeURIComponent(obj[p]) + "{after}");'

        # Build the href to return names JSON
        remote = "'?text=%QUERY&format=json'"
        if "db=" in href:
            # Add tree name to query params
            remote = f"'?db={treename}&text=%QUERY&format=json'"
        js += (
            """
    $('#search-form').submit(function () {
        $(this)
            .find('input[name]')
            .filter(function () {
                return !this.value;
            })
            .prop('name', '');
    });
    function jump(currentPage) {
      newPage = prompt("Jump to page", currentPage);
      if (newPage) {
        href = window.location.href.replace("page="+currentPage, "page="+newPage);
        window.location.href = href;
      }
    }
    function configure_typeahead(node) {
      if (!node.id || !node.id.endsWith("-typeahead")) {
        return;
      }
      table = node.id.replace("-typeahead", "");
      var bloodhound = new Bloodhound({
        datumTokenizer: Bloodhound.tokenizers.obj.nonword('short_label', 'label', 'synonym'),
        queryTokenizer: Bloodhound.tokenizers.nonword,
        sorter: function(a, b) {
          return a.order - b.order;
        },
        remote: {
          url: """
            + remote
            + """,
          wildcard: '%QUERY',
          transform : function(response) {
              return bloodhound.sorter(response);
          }
        }
      });
      $(node).typeahead({
        minLength: 0,
        hint: false,
        highlight: true
      }, {
        name: table,
        source: bloodhound,
        display: function(item) {
          if (item.label && item.short_label && item.synonym) {
            return item.short_label + ' - ' + item.label + ' - ' + item.synonym;
          } else if (item.label && item.short_label) {
            return item.short_label + ' - ' + item.label;
          } else if (item.label && item.synonym) {
            return item.label + ' - ' + item.synonym;
          } else if (item.short_label && item.synonym) {
            return item.short_label + ' - ' + item.synonym;
          } else if (item.short_label && !item.label) {
            return item.short_label;
          } else {
            return item.label;
          }
        },
        limit: 40
      });
      $(node).bind('click', function(e) {
        $(node).select();
      });
      $(node).bind('typeahead:select', function(ev, suggestion) {
        $(node).prev().val(suggestion.id);
        go(table, suggestion.id);
      });
      $(node).bind('keypress',function(e) {
        if(e.which == 13) {
          go(table, $('#' + table + '-hidden').val());
        }
      });
    }
    $('.typeahead').each(function() { configure_typeahead(this); });
    function go(table, value) {
      q = {}
      table = table.replace('_all', '');
      q[table] = value
      window.location = query(q);
    }
    function query(obj) {
      var str = [];
      for (var p in obj)
        if (obj.hasOwnProperty(p)) {
          """
            + js_funct
            + """
        }
      return str.join("&");
    }"""
        )

    body.append(["script", {"type": "text/javascript"}, js])

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
            ["link", {"rel": "stylesheet", "href": bootstrap_css, "crossorigin": "anonymous"}],
            ["link", {"rel": "stylesheet", "href": "../style.css"}],
            ["title", title],
            [
                "style",
                """
        #annotations {
          padding-left: 1em;
          list-style-type: none !important;
        }
        #annotations ul {
          padding-left: 3em;
          list-style-type: circle !important;
        }
        #annotations ul ul {
          padding-left: 2em;
          list-style-type: none !important;
        }
        .hierarchy {
          padding-left: 0em;
          list-style-type: none !important;
        }
        .hierarchy ul {
          padding-left: 1em;
          list-style-type: none !important;
        }
        .hierarchy ul.multiple-children > li > ul {
          border-left: 1px dotted #ddd;
        }
        .hierarchy .children {
          border-left: none;
          margin-left: 2em;
          text-indent: -1em;
        }
        .hierarchy .children li::before {
          content: "\2022";
          color: #ddd;
          display: inline-block;
          width: 0em;
          margin-left: -1em;
        }
        #nonpeptides .tt-dataset {
          max-height: 300px;
          overflow-y: scroll;
        }
        span.twitter-typeahead .tt-menu {
          cursor: pointer;
        }
        .dropdown-menu, span.twitter-typeahead .tt-menu {
          position: absolute;
          top: 100%;
          left: 0;
          z-index: 1000;
          display: none;
          float: left;
          min-width: 160px;
          padding: 5px 0;
          margin: 2px 0 0;
          font-size: 1rem;
          color: #373a3c;
          text-align: left;
          list-style: none;
          background-color: #fff;
          background-clip: padding-box;
          border: 1px solid rgba(0, 0, 0, 0.15);
          border-radius: 0.25rem; }
        span.twitter-typeahead .tt-suggestion {
          display: block;
          width: 100%;
          padding: 3px 20px;
          clear: both;
          font-weight: normal;
          line-height: 1.5;
          color: #373a3c;
          text-align: inherit;
          white-space: nowrap;
          background: none;
          border: 0; }
        span.twitter-typeahead .tt-suggestion:focus,
        .dropdown-item:hover,
        span.twitter-typeahead .tt-suggestion:hover {
            color: #2b2d2f;
            text-decoration: none;
            background-color: #f5f5f5; }
        span.twitter-typeahead .active.tt-suggestion,
        span.twitter-typeahead .tt-suggestion.tt-cursor,
        span.twitter-typeahead .active.tt-suggestion:focus,
        span.twitter-typeahead .tt-suggestion.tt-cursor:focus,
        span.twitter-typeahead .active.tt-suggestion:hover,
        span.twitter-typeahead .tt-suggestion.tt-cursor:hover {
            color: #fff;
            text-decoration: none;
            background-color: #0275d8;
            outline: 0; }
        span.twitter-typeahead .disabled.tt-suggestion,
        span.twitter-typeahead .disabled.tt-suggestion:focus,
        span.twitter-typeahead .disabled.tt-suggestion:hover {
            color: #818a91; }
        span.twitter-typeahead .disabled.tt-suggestion:focus,
        span.twitter-typeahead .disabled.tt-suggestion:hover {
            text-decoration: none;
            cursor: not-allowed;
            background-color: transparent;
            background-image: none;
            filter: "progid:DXImageTransform.Microsoft.gradient(enabled = false)"; }
        span.twitter-typeahead {
          width: 100%; }
          .input-group span.twitter-typeahead {
            display: block !important; }
            .input-group span.twitter-typeahead .tt-menu {
              top: 2.375rem !important; }""",
            ],
        ]
        body = ["body", {"class": "container"}, body]
        html = ["html", head, body]
    else:
        html = body
    return render(all_prefixes, html, href=href, db=treename)


def annotations2rdfa(
    treename: str,
    data: dict,
    predicate_ids: list,
    term_id: str,
    stanza: list,
    href: str = "?term={curie}",
) -> list:
    """Create a hiccup-style vector for the annotation on a term."""
    # The subjects in the stanza that are of type owl:Axiom:
    annotation_bnodes = set()
    for row in stanza:
        if row["predicate"] == "owl:annotatedSource":
            annotation_bnodes.add(row["subject"])

    # Annotations, etc. on the right-hand side for the subjects contained in
    # annotation_bnodes:
    annotations = defaultdict(dict)
    for row in stanza:
        # subject is the blank node, _:...
        subject = row["subject"]
        if subject not in annotation_bnodes:
            continue

        if subject not in annotations:
            annotations[subject] = {}

        predicate = row["predicate"]
        obj = row["object"]
        value = row["value"]

        if predicate not in [
            "owl:annotatedSource",
            "owl:annotatedTarget",
            "owl:annotatedProperty",
            "rdf:type",
        ]:
            # This is the actual axiom that we care about and contains display value
            annotations[subject]["predicate"] = predicate
            if obj:
                annotations[subject]["object"] = obj
            if value:
                annotations[subject]["value"] = value
            annotations[subject]["annotation"] = row

        if predicate == "owl:annotatedSource":
            annotations[subject]["source"] = obj

        elif predicate == "owl:annotatedProperty":
            annotations[subject]["target_predicate"] = obj

        elif predicate == "owl:annotatedTarget":
            if obj:
                annotations[subject]["target_object"] = obj
            if value:
                annotations[subject]["target_value"] = value

    spv2annotation = {}
    for bnode, details in annotations.items():
        source = details["source"]
        target_predicate = details["target_predicate"]
        target = details.get("target_object", None) or details.get("target_value", None)

        if source in spv2annotation:
            # list of predicate -> values on this target (combo of predicate + value)
            pred2val = spv2annotation[source]
        else:
            pred2val = {}

        if target_predicate in pred2val:
            annotated_values = pred2val[target_predicate]
        else:
            annotated_values = {}

        if target in annotated_values:
            ax_annotations = annotated_values[target]
        else:
            ax_annotations = {}

        # predicate of the annotation
        ann_predicate = details["predicate"]
        if ann_predicate in ax_annotations:
            # values of the annotation
            anns = ax_annotations[ann_predicate]
        else:
            anns = []
        anns.append(details["annotation"])

        ax_annotations[ann_predicate] = anns
        annotated_values[target] = ax_annotations
        pred2val[target_predicate] = annotated_values
        spv2annotation[source] = pred2val

    # The initial hiccup, which will be filled in later:
    items = ["ul", {"id": "annotations", "class": "col-md"}]
    labels = data["labels"]

    # s2 maps the predicates of the given term to their corresponding rows (there can be more than
    # one row per predicate):
    s2 = defaultdict(list)
    for row in stanza:
        if row["subject"] == term_id:
            s2[row["predicate"]].append(row)
    pcs = list(s2.keys())

    # Loop through the rows of the stanza that correspond to the predicates of the given term:
    for predicate in predicate_ids:
        if predicate not in pcs:
            continue
        predicate_label = predicate
        if predicate.startswith("<"):
            predicate_label = predicate.lstrip("<").rstrip(">")
        anchor = [
            "a",
            {"href": href.format(curie=predicate, db=treename)},
            labels.get(predicate, predicate_label),
        ]
        # Initialise an empty list of "o"s, i.e., hiccup representations of objects:
        objs = []
        for row in s2[predicate]:
            # Convert the `data` map, that has entries for the tree and for a list of the labels
            # corresponding to all of the curies in the stanza, into a hiccup object `o`:
            o = ["li", row2o(stanza, data, row)]

            # Check for axiom annotations and create nested
            nest = build_nested(treename, data, labels, spv2annotation, term_id, row, [], href=href)
            if nest:
                o += nest

            # Append the `o` to the list of `os`:
            objs.append(o)
        if objs:
            items.append(["li", anchor, ["ul"] + objs])
    return items


def build_nested(
    treename: str,
    data: dict,
    labels: dict,
    spv2annotation: dict,
    source: str,
    row: dict,
    ele: list,
    href: str = "?id={curie}",
) -> list:
    """Build a nested hiccup list of axiom annotations."""
    predicate = row["predicate"]
    if source in spv2annotation:
        annotated_predicates = spv2annotation[source]
        if predicate in annotated_predicates:
            annotated_values = annotated_predicates[predicate]
            target = row.get("object", None) or row.get("value", None)
            if target in annotated_values:
                ax_annotations = annotated_values[target]
                for ann_predicate, ann_rows in ax_annotations.items():
                    # Build the nested list "anchor" (predicate)
                    anchor = [
                        "li",
                        [
                            "small",
                            [
                                "a",
                                {"href": href.format(curie=ann_predicate, db=treename)},
                                labels.get(ann_predicate, ann_predicate),
                            ],
                        ],
                    ]

                    # Collect the axiom annotation objects/values
                    ax_os = []
                    for ar in ann_rows:
                        ax_os.append(["li", ["small", row2o([], data, ar)]])
                        build_nested(
                            treename,
                            data,
                            labels,
                            spv2annotation,
                            ar["subject"],
                            ar,
                            ax_os,
                            href=href,
                        )
                    ele.append(["ul", anchor, ["ul"] + ax_os])
    return ele


def thing2rdfa(
    conn: Connection,
    all_prefixes: list,
    treename: str,
    predicate_ids: list,
    title: str = None,
    href: str = "?id={curie}",
):
    """Create a hiccup-style HTML vector for owl:Thing as the parent of all top-level terms."""
    # Select all classes without parents and set them as children of owl:Thing
    results = conn.execute(
        """SELECT DISTINCT subject FROM statements 
        WHERE subject NOT IN 
            (SELECT subject FROM statements
             WHERE predicate = 'rdfs:subClassOf')
        AND subject IN 
            (SELECT subject FROM statements 
             WHERE predicate = 'rdf:type'
             AND object = 'owl:Class' AND subject NOT LIKE '_:%%');"""
    )
    add_children = [x["subject"] for x in results if x["subject"] != "owl:Thing"]
    results = conn.execute(
        """SELECT stanza, subject, predicate, object, value, datatype, language
           FROM statements WHERE stanza = 'owl:Thing'"""
    )
    stanza = []
    for res in results:
        stanza.append(dict(res))
    if not stanza:
        stanza = [
            {
                "stanza": "owl:Thing",
                "subject": "owl:Thing",
                "predicate": "rdf:type",
                "object": "owl:Class",
                "value": None,
                "datatype": None,
                "language": None,
            }
        ]
    return term2rdfa(
        conn,
        all_prefixes,
        treename,
        predicate_ids,
        "owl:Thing",
        stanza,
        title=title,
        add_children=add_children,
        href=href,
    )


def curie2iri(prefixes: list, curie: str) -> str:
    """Convert a CURIE to IRI"""
    if curie.startswith("<"):
        return curie.lstrip("<").rstrip(">")
    elif curie.startswith("_:"):
        return curie
    for prefix, base in prefixes:
        if curie.startswith(prefix + ":"):
            return curie.replace(prefix + ":", base)
    raise ValueError(f"No matching prefix for {curie}")


def get_entity_type(conn: Connection, term_id: str) -> str:
    """Get the OWL entity type for a term."""
    query = sql_text(
        """SELECT object FROM statements WHERE stanza = :term_id
        AND subject = :term_id AND predicate = 'rdf:type'"""
    )
    results = list(conn.execute(query, term_id=term_id))
    if len(results) > 1:
        for res in results:
            if res["object"] in TOP_LEVELS:
                return res["object"]
        return "owl:Individual"
    elif len(results) == 1:
        entity_type = results[0]["object"]
        if entity_type == "owl:NamedIndividual":
            entity_type = "owl:Individual"
        return entity_type
    else:
        entity_type = None
        query = sql_text(
            "SELECT predicate FROM statements WHERE stanza = :term_id AND subject = :term_id"
        )
        results = conn.execute(query, term_id=term_id)
        preds = [row["predicate"] for row in results]
        if "rdfs:subClassOf" in preds:
            return "owl:Class"
        elif "rdfs:subPropertyOf" in preds:
            return "owl:AnnotationProperty"
        if not entity_type:
            query = sql_text("SELECT predicate FROM statements WHERE object = :term_id")
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                return "owl:Class"
            elif "rdfs:subPropertyOf" in preds:
                return "owl:AnnotationProperty"
    return "owl:Class"


def get_hierarchy(
    conn: Connection, term_id: str, entity_type: str, add_children: list = None
) -> (dict, set):
    """Return a hierarchy dictionary for a term and all its ancestors and descendants."""
    # Build the hierarchy
    if entity_type == "owl:Individual":
        query = sql_text(
            """SELECT DISTINCT object AS parent, subject AS child FROM statements
            WHERE stanza = :term_id
             AND subject = :term_id
             AND predicate = 'rdf:type'
             AND object NOT IN ('owl:Individual', 'owl:NamedIndividual')
             AND object NOT LIKE '_:%%'"""
        )
        results = conn.execute(query, term_id=term_id)
    else:
        pred = "rdfs:subPropertyOf"
        if entity_type == "owl:Class":
            pred = "rdfs:subClassOf"
        query = sql_text(
            """WITH RECURSIVE ancestors(parent, child) AS (
            VALUES (:term_id, NULL)
            UNION
            -- The children of the given term:
            SELECT object AS parent, subject AS child
            FROM statements
            WHERE predicate = :pred
              AND object = :term_id
            UNION
            --- Children of the children of the given term
            SELECT object AS parent, subject AS child
            FROM statements
            WHERE object IN (SELECT subject FROM statements
                             WHERE predicate = :pred AND object = :term_id)
              AND predicate = :pred
            UNION
            -- The non-blank parents of all of the parent terms extracted so far:
            SELECT object AS parent, subject AS child
            FROM statements, ancestors
            WHERE ancestors.parent = statements.stanza
              AND statements.predicate = :pred
              AND statements.object NOT LIKE '_:%%'
          )
          SELECT * FROM ancestors"""
        )
        results = conn.execute(query, term_id=term_id, pred=pred)
    results = [[x["parent"], x["child"]] for x in results]
    if add_children:
        results.extend([[term_id, child] for child in add_children])

    hierarchy = {
        entity_type: {"parents": [], "children": []},
        term_id: {"parents": [], "children": []},
    }
    curies = set()
    for res in results:
        # Consider the parent column of the current row:
        parent = res[0]
        if not parent or parent == "owl:Thing":
            continue
        # If it is not null, add it to the list of all of the compact URIs described by this tree:
        curies.add(parent)
        # If it is not already in the tree, add a new entry for it to the tree:
        if parent not in hierarchy:
            hierarchy[parent] = {
                "parents": [],
                "children": [],
            }

        # Consider the child column of the current row:
        child = res[1]
        if not child:
            continue
        # If it is not null, add it to the list of all the compact URIs described by this tree:
        curies.add(child)
        # If the child is not already in the tree, add a new entry for it to the tree:
        if child not in hierarchy:
            hierarchy[child] = {
                "parents": [],
                "children": [],
            }

        # Fill in the appropriate relationships in the entries for the parent and child:
        hierarchy[parent]["children"].append(child)
        hierarchy[child]["parents"].append(parent)

    if not hierarchy[term_id]["parents"]:
        # Place cur term directly under top level entity
        hierarchy[term_id]["parents"].append(entity_type)
        hierarchy[entity_type]["children"].append(term_id)

    # Add entity type as top level to anything without a parent
    for term_id, mini_tree in hierarchy.items():
        if not mini_tree["parents"]:
            hierarchy[term_id]["parents"].append(entity_type)

    return hierarchy, curies


def get_sorted_predicates(conn: Connection, exclude_ids: list = None) -> list:
    """Return a list of predicates IDs sorted by their label, optionally excluding some predicate
    IDs. If the predicate does not have a label, use the ID as the label."""
    exclude = None
    if exclude_ids:
        exclude = ", ".join([f"'{x}'" for x in exclude_ids])

    # Retrieve all predicate IDs
    results = conn.execute("SELECT DISTINCT predicate FROM statements")
    all_predicate_ids = [x["predicate"] for x in results]
    if exclude:
        all_predicate_ids = [x for x in all_predicate_ids if x not in exclude_ids]

    # Retrieve predicates with labels
    query = sql_text(
        """SELECT DISTINCT subject, value
        FROM statements WHERE subject IN :ap AND predicate = 'rdfs:label';"""
    ).bindparams(bindparam("ap", expanding=True))
    results = conn.execute(query, {"ap": all_predicate_ids})
    predicate_label_map = {x["subject"]: x["value"] for x in results}

    # Add unlabeled predicates to map with label = ID
    for p in all_predicate_ids:
        if p not in predicate_label_map:
            predicate_label_map[p] = p

    # Return list of keys sorted by value (label)
    return [k for k, v in sorted(predicate_label_map.items(), key=lambda x: x[1].lower())]


def get_ontology(conn: Connection, prefixes: list) -> (str, str):
    """Get the ontology IRI and title (or None).

    :param conn: database connection
    :param prefixes: list of prefix tuples (prefix, base)
    :return: IRI, title or None
    """
    res = conn.execute(
        "SELECT subject FROM statements WHERE predicate = 'rdf:type' AND object = 'owl:Ontology'"
    ).fetchone()
    if not res:
        return None, None
    iri = res["subject"]
    dct = "<http://purl.org/dc/terms/title>"
    for prefix, base in prefixes:
        if base == "http://purl.org/dc/terms/":
            dct = f"{prefix}:title"
    query = sql_text(
        "SELECT value FROM statements WHERE stanza = :iri AND subject = :iri AND predicate = :dct"
    )
    res = conn.execute(query, iri=iri, dct=dct).fetchone()
    if not res:
        return iri, None
    return iri, res["value"]


def term2rdfa(
    conn: Connection,
    prefixes: list,
    treename: str,
    predicate_ids: list,
    term_id: str,
    stanza: list,
    title: str = None,
    add_children: list = None,
    href: str = "?id={curie}",
) -> (str, str):
    """Create a hiccup-style HTML vector for the given term."""
    ontology_iri, ontology_title = get_ontology(conn, prefixes)
    if term_id not in TOP_LEVELS:
        # Get a hierarchy under the entity type
        entity_type = get_entity_type(conn, term_id)
        hierarchy, curies = get_hierarchy(conn, term_id, entity_type, add_children=add_children)
    else:
        # Get the top-level for this entity type
        entity_type = term_id
        if term_id == "ontology":
            hierarchy = {term_id: {"parents": [], "children": []}}
            curies = set()
            if ontology_iri:
                curies.add(ontology_iri)
        else:
            pred = None
            if term_id == "owl:Individual":
                # No user input, safe to use f-string for query
                tls = ", ".join([f"'{x}'" for x in TOP_LEVELS.keys()])
                results = conn.execute(
                    f"""SELECT DISTINCT subject FROM statements
                    WHERE subject NOT IN
                        (SELECT subject FROM statements
                         WHERE predicate = 'rdf:type'
                         AND object NOT IN ('owl:Individual', 'owl:NamedIndividual'))
                    AND subject IN
                        (SELECT subject FROM statements
                         WHERE predicate = 'rdf:type' AND object NOT IN ({tls}))"""
                )
            elif term_id == "rdfs:Datatype":
                results = conn.execute(
                    """SELECT DISTINCT subject FROM statements
                    WHERE predicate = 'rdf:type' AND object = 'rdfs:Datatype'"""
                )
            else:
                pred = "rdfs:subPropertyOf"
                if term_id == "owl:Class":
                    pred = "rdfs:subClassOf"
                # Select all classes without parents and set them as children of owl:Thing
                query = sql_text(
                    """SELECT DISTINCT subject FROM statements 
                    WHERE subject NOT IN 
                        (SELECT subject FROM statements
                         WHERE predicate = :pred
                         AND object != 'owl:Thing')
                    AND subject IN 
                        (SELECT subject FROM statements 
                         WHERE predicate = 'rdf:type'
                         AND object = :term_id AND subject NOT LIKE '_:%%'
                         AND subject NOT IN ('owl:Thing', 'rdf:type'));"""
                )
                results = conn.execute(query, pred=pred, term_id=term_id)
            children = [res["subject"] for res in results]
            child_children = defaultdict(set)
            if pred and children:
                # Get children of children for classes & properties
                query = sql_text(
                    """SELECT DISTINCT object AS parent, subject AS child FROM statements
                    WHERE predicate = :pred AND object IN :children"""
                ).bindparams(bindparam("pred"), bindparam("children", expanding=True))
                results = conn.execute(query, {"pred": pred, "children": children})
                for res in results:
                    p = res["parent"]
                    if p not in child_children:
                        child_children[p] = set()
                    child_children[p].add(res["child"])
            hierarchy = {term_id: {"parents": [], "children": children}}
            curies = {term_id}
            for c in children:
                c_children = child_children.get(c, set())
                hierarchy[c] = {"parents": [term_id], "children": list(c_children)}
                curies.update(c_children)
                curies.add(c)

    # Add all of the other compact URIs in the stanza to the set of compact URIs:
    stanza.sort(key=lambda x: x["predicate"])
    for row in stanza:
        curies.add(row.get("subject"))
        curies.add(row.get("predicate"))
        curies.add(row.get("object"))
    curies.discard("")
    curies.discard(None)

    # Get all the prefixes that are referred to by the compact URIs:
    ps = set()
    for curie in curies:
        if not isinstance(curie, str) or len(curie) == 0 or curie[0] in ("_", "<"):
            continue
        prefix, local = curie.split(":")
        ps.add(prefix)

    # Get all of the rdfs:labels corresponding to all of the compact URIs, in the form of a map
    # from compact URIs to labels:
    labels = {}
    query = sql_text(
        """SELECT subject, value FROM statements
        WHERE stanza IN :ids AND predicate = 'rdfs:label' AND value IS NOT NULL"""
    ).bindparams(bindparam("ids", expanding=True))
    results = conn.execute(query, {"ids": list(curies)})
    for res in results:
        labels[res["subject"]] = res["value"]
    for t, o_label in TOP_LEVELS.items():
        labels[t] = o_label
    if ontology_iri and ontology_title:
        labels[ontology_iri] = ontology_title

    obsolete = []
    query = sql_text(
        """SELECT DISTINCT subject FROM statements
        WHERE stanza in :ids AND predicate='owl:deprecated' AND value='true'"""
    ).bindparams(bindparam("ids", expanding=True))
    results = conn.execute(query, {"ids": list(curies)})
    for res in results:
        obsolete.append(res["subject"])

    # If the compact URIs in the labels map are also in the tree, then add the label info to the
    # corresponding node in the tree:
    for key in hierarchy.keys():
        if key in labels:
            hierarchy[key]["label"] = labels[key]

    # Initialise a map with one entry for the tree and one for all of the labels corresponding to
    # all of the compact URIs in the stanza:
    data = {"labels": labels, "obsolete": obsolete, treename: hierarchy, "iri": ontology_iri}

    # Determine the label to use for the given term id when generating RDFa (the term might have
    # multiple labels, in which case we will just choose one and show it everywhere). This defaults
    # to the term id itself, unless there is a label for the term in the stanza corresponding to the
    # label for that term in the labels map:
    if term_id in labels:
        selected_label = labels[term_id]
    else:
        selected_label = term_id
    label = term_id
    for row in stanza:
        predicate = row["predicate"]
        value = row["value"]
        if predicate == "rdfs:label" and value == selected_label:
            label = value
            break

    subject = None
    si = None
    subject_label = None
    if term_id == "ontology" and ontology_iri:
        subject = ontology_iri
        subject_label = data["labels"].get(ontology_iri, ontology_iri)
        si = curie2iri(prefixes, subject)
    elif term_id != "ontology":
        subject = term_id
        si = curie2iri(prefixes, subject)
        subject_label = label

    rdfa_tree = term2tree(data, treename, term_id, entity_type, href=href)

    if not title:
        title = treename + " Browser"

    if (term_id in TOP_LEVELS and term_id != "ontology") or (
        term_id == "ontology" and not ontology_iri
    ):
        si = None
        if ontology_iri:
            si = curie2iri(prefixes, ontology_iri)
        items = [
            "ul",
            {"id": "annotations", "class": "col-md"},
            ["p", {"class": "lead"}, "Hello! This is an ontology browser."],
            [
                "p",
                "An ",
                [
                    "a",
                    {"href": "https://en.wikipedia.org/wiki/Ontology_(information_science)"},
                    "ontology",
                ],
                " is a terminology system designed for both humans and machines to read. Click the",
                " links on the left to browse the hierarchy of terms. Terms have parent terms, ",
                "child terms, annotations, and ",
                [
                    "a",
                    {"href": "https://en.wikipedia.org/wiki/Web_Ontology_Language"},
                    "logical axioms",
                ],
                ". The page for each term is also machine-readable using ",
                ["a", {"href": "https://en.wikipedia.org/wiki/RDFa"}, "RDFa"],
                ".",
            ],
        ]
        term = [
            "div",
            ["div", {"class": "row"}, ["h2", title]],
        ]
        if si:
            # If ontology IRI, add it to the page
            term.append(["div", {"class": "row"}, ["a", {"href": si}, si]])
        term.append(["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items])
    else:
        items = annotations2rdfa(treename, data, predicate_ids, subject, stanza, href=href)
        term = [
            "div",
            {"resource": subject},
            ["div", {"class": "row"}, ["h2", subject_label]],
            ["div", {"class": "row"}, ["a", {"href": si}, si]],
            ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items],
        ]
    return ps, term


def parent2tree(
    data: dict, treename: str, selected_term, selected_children, node, href="?id={curie}"
) -> list:
    """Return a hiccup-style HTML vector of the full hierarchy for a parent node."""
    cur_hierarchy = ["ul", ["li", tree_label(data, treename, selected_term), selected_children]]
    if node in TOP_LEVELS:
        # Parent is top-level, nothing to add
        return cur_hierarchy

    # Add parents to the hierarchy
    i = 0
    while node and i < 100:
        i += 1
        oc = node
        object_label = tree_label(data, treename, node)
        parents = data[treename][node]["parents"]
        if len(parents) == 0:
            # No parent
            o = [
                "a",
                {"resource": oc, "href": href.format(curie=node, db=treename)},
                object_label,
            ]
            cur_hierarchy = ["ul", ["li", o, cur_hierarchy]]
            break
        parent = parents[0]
        if node == parent:
            # Parent is the same
            o = [
                "a",
                {"resource": oc, "href": href.format(curie=node, db=treename)},
                object_label,
            ]
            cur_hierarchy = ["ul", ["li", o, cur_hierarchy]]
            break
        if parent in TOP_LEVELS:
            href_ele = {"href": href.format(curie=node, db=treename)}
        else:
            href_ele = {
                "about": parent,
                "rev": "rdfs:subClassOf",
                "resource": oc,
                "href": href.format(curie=node, db=treename),
            }
        o = ["a", href_ele, object_label]
        cur_hierarchy = ["ul", ["li", o, cur_hierarchy]]
        node = parent
        if node in TOP_LEVELS:
            break
    return cur_hierarchy


def term2tree(
    data: dict,
    treename: str,
    term_id: str,
    entity_type: str,
    href: str = "?id={curie}",
    max_children: int = 100,
) -> list:
    """Create a hiccup-style HTML hierarchy vector for the given term."""
    if treename not in data or term_id not in data[treename]:
        return []

    term_tree = data[treename][term_id]
    obsolete = data["obsolete"]
    child_labels = []
    obsolete_child_labels = []
    for child in term_tree["children"]:
        if child in obsolete:
            obsolete_child_labels.append([child, data["labels"].get(child, child)])
        else:
            child_labels.append([child, data["labels"].get(child, child)])
    child_labels.sort(key=lambda x: x[1].lower())
    obsolete_child_labels.sort(key=lambda x: x[1].lower())
    child_labels.extend(obsolete_child_labels)

    if entity_type == "owl:Class":
        predicate = "rdfs:subClassOf"
    elif entity_type == "owl:Individual":
        predicate = "rdf:type"
    else:
        predicate = "rdfs:subPropertyOf"

    # Get the children for our target term
    children = []
    for child, label in child_labels:
        if child not in data[treename]:
            continue
        oc = child
        object_label = tree_label(data, treename, oc)
        o = ["a", {"rev": predicate, "resource": oc}, object_label]
        # Check for children of the child and add a plus next to label if so
        if data[treename][oc]["children"]:
            o.append(PLUS)
        attrs = {}
        if len(children) > max_children:
            attrs["style"] = "display: none"
        children.append(["li", attrs, o])

        if len(children) == max_children:
            total = len(term_tree["children"])
            attrs = {"href": "javascript:show_children()"}
            children.append(["li", {"id": "more"}, ["a", attrs, f"Click to show all {total} ..."]])
    children = ["ul", {"id": "children"}] + children
    if len(children) == 0:
        children = ""
    term_label = tree_label(data, treename, term_id)

    # Get the parents for our target term
    parents = term_tree["parents"]
    if parents:
        hierarchy = ["ul"]
        for p in parents:
            if p.startswith("_:"):
                continue
            hierarchy.append(parent2tree(data, treename, term_id, children.copy(), p, href=href))
    else:
        hierarchy = ["ul", ["li", term_label, children]]

    i = 0
    hierarchies = ["ul", {"id": f"hierarchy", "class": "hierarchy multiple-children col-md"}]
    for t, object_label in TOP_LEVELS.items():
        o = ["a", {"href": href.format(curie=t, db=treename)}, object_label]
        if t == entity_type:
            if term_id == entity_type:
                hierarchies.append(hierarchy)
            else:
                hierarchies.append(["ul", ["li", o, hierarchy]])
            continue
        hierarchies.append(["ul", ["li", o]])
        i += 1
    return hierarchies


def tree_label(data: dict, treename: str, s: str) -> list:
    """Retrieve the hiccup-style vector label of a term."""
    node = data[treename][s]
    label = node.get("label", s)
    if s in data["obsolete"]:
        return ["s", label]
    return label


def row2o(_stanza: list, _data: dict, _uber_row: dict) -> list:
    """Given a stanza, a map (`_data`) with entries for the tree structure of the stanza and for all
    of the labels in it, and a row in the stanza, convert the object or value of the row to
    hiccup-style HTML."""

    def renderNonBlank(given_row: dict) -> list:
        """Renders the non-blank object from the given row"""
        return [
            "a",
            {"rel": given_row["predicate"], "resource": given_row["object"]},
            _data["labels"].get(given_row["object"], given_row["object"]),
        ]

    def renderLiteral(given_row: dict) -> list:
        """Renders the object contained in the given row as a literal IRI"""
        # Literal IRIs are enclosed in angle brackets.
        iri = given_row["object"][1:-1]
        return ["a", {"rel": given_row["predicate"], "href": iri}, iri]

    def getOwlOperands(given_row: dict) -> list:
        """Extract all of the operands pointed to by the given row and return them as a list"""
        LOGGER.debug("Finding operands for row with predicate: {}".format(given_row["predicate"]))

        if not given_row["object"].startswith("_:"):
            LOGGER.debug("Found non-blank operand: {}".format(given_row["object"]))
            return [renderNonBlank(given_row)]

        # Find the rows whose subject matches the object from the given row. In general there will
        # be a few. If we find one with an rdf:type predicate then we call the appropriate function
        # to render either a restriction or a class, as the case may be. Otherwise if we find a row
        # with an rdf:first predicate, then if it is a blank node, it points to further operands,
        # which we recursively chase and render, and similarly if the predicate is rdf:rest (which
        # will always have a blank (or nil) object). If the predicate is rdf:first but the object is
        # not blank, then we can render it directly.
        inner_rows = [row for row in _stanza if row["subject"] == given_row["object"]]

        operands = []
        for inner_row in inner_rows:
            inner_subj = inner_row["subject"]
            inner_pred = inner_row["predicate"]
            inner_obj = inner_row["object"]
            LOGGER.debug(f"Found row with <s,p,o> = <{inner_subj}, {inner_pred}, {inner_obj}>")

            if inner_pred == "rdf:type":
                if inner_obj == "owl:Restriction":
                    operands.append(renderOwlRestriction(inner_rows))
                    break
                elif inner_obj == "owl:Class":
                    operands.append(renderOwlClassExpression(inner_rows))
                    break
            elif inner_pred == "rdf:rest":
                if inner_obj != "rdf:nil":
                    operands.append(["span", {"rel": inner_pred}] + getOwlOperands(inner_row))
                else:
                    operands.append(["span", {"rel": inner_pred, "resource": "rdf:nil"}])
                LOGGER.debug(f"Returned from recursing on {inner_pred}")
            elif inner_pred == "rdf:first":
                if inner_obj.startswith("_:"):
                    LOGGER.debug(f"{inner_pred} points to a blank node, following the trail")
                    operands.append(["span", {"rel": inner_pred}] + getOwlOperands(inner_row))
                    LOGGER.debug(f"Returned from recursing on {inner_pred}")
                else:
                    LOGGER.debug(f"Rendering non-blank object with predicate: {inner_pred}")
                    operands.append(renderNonBlank(inner_row))

        return operands

    def renderNaryRelation(class_pred: str, operands: list) -> list:
        """Render an n-ary relation using the given predicate and operands"""

        def relate_ops(oplist: list, operator: str) -> list:
            """
            Relate the logical operands in 'oplist' using the given operator word. E.g., if oplist
            contains the logical operands: op1, op2, op3, and the operator is 'and', then an 'and'
            should be rendered in between each of the logical operands.
            """
            # There should always be exactly two operands (see comment below):
            if len(oplist) != 2:
                LOGGER.error(
                    "Unexpected number of operands: {} in relate_ops. Got "
                    "operands: {}".format(len(oplist), oplist)
                )
                return []

            # The list that will be returned, with instances of `operator` inserted:
            related_list = []

            # Get the two operands and their attributes:
            first_op = oplist[0]
            first_op_attrs = first_op[1]
            second_op = oplist[1]
            second_op_attrs = second_op[1]

            # Append the first operand to the related_list:
            related_list.append(first_op)

            # Now handle the second operand:
            if (
                not second_op_attrs.get("rel") == "rdf:rest"
                or second_op_attrs.get("resource") == "rdf:nil"
            ):
                # If there are no more logical operands, append the last rdf:nil span:
                related_list.append(second_op)
            else:
                # Otherwise, logically connect the remaining ones with `operator` and recurse:
                related_list += [" ", operator, " "]
                related_list.append(
                    [second_op[0], second_op[1]] + relate_ops(second_op[2:], operator)
                )

            return related_list

        # There should always be exactly two operands, even if, logically, there are more. Either
        # they'll be literals or they'll be organised into a linked list like the following:
        # [[rdf:first]
        #  [rdf:rest [[rdf:first]
        #             [rdf:rest [rdf:first]
        #                       [rdf:rest ...
        #                                         [rdf:first]
        #                                         [rdf:rest nil]]]...]
        # In this latter case we need to recurse into the rdf:rest spans to find all of the
        # operands other than the first.
        if len(operands) != 2:
            LOGGER.error(
                "Wrong number of operands ({}) to renderNaryRelation. Got class predicate: "
                "{}; operands: {}".format(len(operands), class_pred, operands)
            )
            return ["div"]

        if class_pred == "owl:intersectionOf":
            operator = "and"
        elif class_pred == "owl:unionOf":
            operator = "or"
        else:
            LOGGER.error(f"Unrecognized predicate for n-ary relation: {class_pred}")
            return ["div"]

        owl_div = ["span", {"rel": class_pred}, " ", "("]
        owl_div += relate_ops(operands, operator)
        owl_div.append(")")
        return owl_div

    def renderUnaryRelation(class_pred: str, operands: list) -> list:
        """Render a unary relation using the given predicate and operands"""
        if len(operands) != 1:
            LOGGER.error(
                f"Something is wrong. Wrong number of operands to '{class_pred}': {operands}"
            )
            return ["div"]

        if class_pred == "owl:complementOf":
            operator = "not"
        elif class_pred == "owl:oneOf":
            operator = "one of"
        else:
            LOGGER.error(f"Unrecognized predicate for unary relation: {class_pred}")
            return ["div"]

        operand = operands[0]
        owl_div = ["span", {"rel": class_pred}, operator, " ", operand]
        return owl_div

    def renderOwlRestriction(given_rows: list) -> list:
        """Renders the OWL restriction described by the given rows"""
        # OWL restrictions are represented using three rows. The first will have the predicate
        # 'rdf:type' and its object should always be 'owl:Restriction'. The second row will have the
        # predicate 'owl:onProperty' and its object will represent the property being restricted,
        # which can be either a blank or a non-blank node. The third row will have either the
        # predicate 'owl:allValuesFrom' or the predicate 'owl:someValuesFrom', which we render,
        # respectively, as 'only' and 'some'. The object of this row is what the property being
        # restricted is being restricted in relation to.
        # E.g., in the restriction: "'has grain' some 'sodium phosphate'": 'has grain' is extracted
        # via the object of the second row, while 'some' and 'sodium phosphate' are
        # extracted via the predicate and object, respectively, of the third row.
        rdf_type_row = [row for row in given_rows if row["predicate"] == "rdf:type"]
        property_row = [row for row in given_rows if row["predicate"] == "owl:onProperty"]
        target_row = [
            row for row in given_rows if row["predicate"] not in ("rdf:type", "owl:onProperty")
        ]
        for rowset in [rdf_type_row, property_row, target_row]:
            if len(rowset) != 1:
                LOGGER.error(f"Rows: {given_rows} do not represent a valid restriction")
                return ["div"]

        property_row = property_row[0]
        target_row = target_row[0]
        rdf_type_row = rdf_type_row[0]
        if rdf_type_row["object"] != "owl:Restriction":
            LOGGER.error(
                "Unexpected rdf:type: '{}' found in OWL restriction".format(rdf_type_row["object"])
            )
            return ["div"]

        target_pred = target_row["predicate"]
        target_obj = target_row["object"]
        LOGGER.debug("Rendering OWL restriction {} for object {}".format(target_pred, target_obj))
        if target_obj.startswith("_:"):
            inner_rows = [row for row in _stanza if row["subject"] == target_obj]
            target_link = renderOwlClassExpression(inner_rows, target_pred)
        else:
            target_link = renderNonBlank(target_row)

        if target_pred == "owl:someValuesFrom":
            operator = "some"
        elif target_pred == "owl:allValuesFrom":
            operator = "only"
        else:
            LOGGER.error("Unrecognised predicate: {}".format(target_pred))
            return ["div"]

        return [
            "span",
            ["span", {"rel": rdf_type_row["predicate"], "resource": rdf_type_row["object"]}],
            [
                "a",
                {"rel": property_row["predicate"], "resource": property_row["object"]},
                _data["labels"].get(property_row["object"], property_row["object"]),
            ],
            " ",
            operator,
            target_link,
        ]

    def renderOwlClassExpression(given_rows: list, rel: str = None) -> list:
        """Render the OWL class expression pointed to by the given row"""
        # The sub-stanza corresponding to an owl:Class should have two rows. One of these points
        # to the actual class referred to (either a named class or a blank node). From this row we
        # get the subject, predicate, and object to render. The second row will have the object
        # type, which we expect to be 'owl:Class'.
        rdf_type_row = [row for row in given_rows if row["predicate"] == "rdf:type"]
        class_row = [row for row in given_rows if row["predicate"].startswith("owl:")]
        LOGGER.debug(f"Found rows: {rdf_type_row}, {class_row}")

        rdf_type_row = rdf_type_row[0]
        class_row = class_row[0]
        class_subj = class_row["subject"]
        class_pred = class_row["predicate"]
        class_obj = class_row["object"]

        # All blank class expressions will have operands, which we retrieve here:
        operands = getOwlOperands(class_row)

        hiccup = [
            "span",
            ["span", {"rel": rdf_type_row["predicate"], "resource": rdf_type_row["object"]}],
        ]

        # If `rel` is given, insert the attribute into the second position of the hiccup:
        if rel:
            hiccup = hiccup[:1] + [{"rel": rel}] + hiccup[1:]

        LOGGER.debug(f"Rendering <s,p,o> = <{class_subj}, {class_pred}, {class_obj}>")
        if class_pred in ["owl:intersectionOf", "owl:unionOf"]:
            hiccup.append(renderNaryRelation(class_pred, operands))
        elif class_pred in ["owl:complementOf", "owl:oneOf"]:
            hiccup.append(renderUnaryRelation(class_pred, operands))
        elif class_pred == "owl:onProperty":
            hiccup.append(renderOwlRestriction(given_rows))
        elif class_obj.startswith("<"):
            hiccup.append(renderLiteral(class_row))
        else:
            LOGGER.warning(
                f"Rendering for <s,p,o> = <{class_subj}, {class_pred}, {class_obj}> not implemented"
            )
            hiccup.append(["a", {"rel": class_pred}, _data["labels"].get(class_obj, class_obj)])

        return hiccup

    uber_subj = _uber_row["subject"]
    uber_pred = _uber_row["predicate"]
    uber_obj = _uber_row["object"]
    LOGGER.debug(f"Called row2o on <s,p,o> = <{uber_subj}, {uber_pred}, {uber_obj}>")

    if not isinstance(uber_obj, str):
        if _uber_row["value"]:
            LOGGER.debug("Rendering non-string object with value: {}".format(_uber_row["value"]))
            return ["span", {"property": uber_pred}, _uber_row["value"]]
        else:
            LOGGER.error("Received non-string object with null value; returning empty div")
            return ["div"]
    elif uber_obj.startswith("<"):
        LOGGER.debug(f"Rendering literal IRI: {uber_obj}")
        return renderLiteral(_uber_row)
    elif uber_obj.startswith("_:"):
        LOGGER.debug(
            f"Rendering triple with blank object: <s,p,o> = <{uber_subj}, {uber_pred}, {uber_obj}>"
        )
        inner_rows = [row for row in _stanza if row["subject"] == uber_obj]
        object_type = [row for row in inner_rows if row["predicate"] == "rdf:type"]
        if len(object_type) != 1:
            LOGGER.warning(f"Wrong number of object types found for {uber_obj}: {object_type}")
        object_type = object_type[0]["object"] if len(object_type) > 0 else None

        if object_type == "owl:Class":
            LOGGER.debug(f"Rendering OWL class pointed to by {uber_obj}")
            return ["span", {"rel": uber_pred}, renderOwlClassExpression(inner_rows)]
        elif object_type == "owl:Restriction":
            LOGGER.debug(f"Rendering OWL restriction pointed to by {uber_obj}")
            return ["span", {"rel": uber_pred}, renderOwlRestriction(inner_rows)]
        else:
            if not object_type:
                LOGGER.warning(f"Could not determine object type for {uber_pred}")
            else:
                LOGGER.warning(f"Unrecognised object type: {object_type} for predicate {uber_pred}")
            return ["span", {"property": uber_pred}, uber_obj]
    else:
        LOGGER.debug(
            f"Rendering non-blank triple: <s,p,o> = <{uber_subj}, {uber_pred}, {uber_obj}>"
        )
        return renderNonBlank(_uber_row)


if __name__ == "__main__":
    main()
