import json
import sys

from argparse import ArgumentParser
from collections import defaultdict
from typing import Optional

from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .helpers import get_connection
from .hiccup import render

# Stylesheets & JS scripts
bootstrap_css = "https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css"
bootstrap_js = "https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js"
popper_js = "https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js"
typeahead_js = "https://cdnjs.cloudflare.com/ajax/libs/typeahead.js/0.11.1/typeahead.bundle.min.js"


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
    p.add_argument("-l", "--limit", help="Limit for number of results", default=30)
    p.add_argument("-f", "--format", help="", default="json")
    p.add_argument("-H", "--href", help="Format string to convert CURIEs to tree links")
    p.add_argument(
        "-d",
        "--db-name",
        help="If provided, include 'db' param in query string with the given name",
    )
    p.add_argument(
        "-r", "--include-search", help="If provided, include a search bar", action="store_true"
    )
    p.add_argument(
        "-c",
        "--contents-only",
        action="store_true",
        help="If provided, render HTML without the roots",
    )
    args = p.parse_args()

    if args.limit == "none":
        limit = None
    else:
        try:
            limit = int(args.limit)
        except ValueError:
            raise RuntimeError("--limit must be an integer")

    if args.href:
        href = args.href
        if "{curie}" not in href:
            raise RuntimeError("The --href argument must contain '{curie}'")
    else:
        href = "?id={curie}"
        if args.db_name:
            href += "&db={db}"

    conn = get_connection(args.db)
    sys.stdout.write(
        search(
            conn,
            args.text,
            label=args.label,
            short_label=args.short_label,
            synonyms=args.synonyms,
            limit=limit,
            fmt=args.format,
            href=href,
            db=args.db_name,
            include_search=args.include_search,
            standalone=not args.contents_only,
        )
    )


def search(
    conn: Connection,
    search_text: str,
    label: str = "rdfs:label",
    short_label: str = None,
    synonyms: list = None,
    limit: Optional[int] = 30,
    fmt: str = "json",
    href: str = "?id={curie}",
    db: Optional[str] = None,
    include_search: bool = False,
    standalone: bool = True,
) -> str:
    """Return a string containing the search results in JSON format."""
    res = get_search_results(
        conn, search_text, label=label, short_label=short_label, synonyms=synonyms, limit=limit
    )
    if fmt == "json":
        return json.dumps(res, indent=4)
    else:
        return render_html(
            res, search_text, href=href, db=db, include_search=include_search, standalone=standalone
        )


def get_search_results(
    conn: Connection,
    search_text: str,
    label: str = "rdfs:label",
    short_label: str = None,
    synonyms: list = None,
    limit: Optional[int] = 30,
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
                results = conn.execute(
                    query, short_label=short_label, text=f"%%{search_text.lower()}%%"
                )
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


def render_html(
    res: list,
    text: str,
    href: str = "?id={curie}",
    db: Optional[str] = None,
    include_search: bool = False,
    standalone: bool = True,
) -> str:
    body = ["div", {"class": "container"}]
    if include_search:
        form = [
            "form",
            {"class": "form-row mt-2 mb-2", "method": "get"},
            [
                "input",
                {
                    "id": f"statements-typeahead",
                    "name": "search_text",
                    "class": "typeahead form-control",
                    "type": "text",
                    "value": "",
                    "placeholder": "Search",
                },
            ],
        ]
        if db:
            form.append(["input", {"name": "db", "value": db, "type": "hidden"}])
        body.append(form)

    body.append(["div", {"class": "row"}, ["h2", f"Search results for '{text}'"]])
    body.append(
        [
            "div",
            {"class": "row"},
            ["p", {"class": "font-italic"}, "Showing ", ["b", str(len(res))], " results"],
        ]
    )
    for itm in res:
        term_id = itm["id"]
        link = href.replace("{curie}", term_id)
        if db:
            link = link.replace("{db}", db)
        href_ele = ["a", {"href": link}, itm["label"]]
        ele = ["p", {"class": "lead"}, href_ele, "&nbsp;&nbsp;&nbsp;", ["code", term_id]]
        body.append(["div", {"class": "row"}, ele])

    if include_search:
        # JQuery
        body.append(
            [
                "script",
                {"type": "text/javascript", "src": "https://code.jquery.com/jquery-3.5.1.min.js",},
            ]
        )

        # Add JS imports for running search
        body.append(["script", {"type": "text/javascript", "src": popper_js}])
        body.append(["script", {"type": "text/javascript", "src": bootstrap_js}])
        body.append(["script", {"type": "text/javascript", "src": typeahead_js}])

        # Built the href to return when you select a term
        href_split = href.split("{curie}")
        before = href_split[0]
        if db:
            before = before.format(db=db)
        after = href_split[1]
        if db:
            after = after.format(db=db)
        js_funct = f'str.push("{before}" + encodeURIComponent(obj[p]) + "{after}");'

        # Build the href to return names JSON
        remote = "'?text=%QUERY&format=json'"
        if db:
            # Add tree name to query params
            remote = f"'?db={db}&text=%QUERY&format=json'"
        js = (
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
            # ["title", title]
            [
                "style",
                """
                .tt-dataset {
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
        html = ["html", head, body]
    else:
        html = body
    return render([], html)


if __name__ == "__main__":
    main()
