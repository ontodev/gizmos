import os
import sqlite3
import sys

from argparse import ArgumentParser
from collections import defaultdict


"""
Usage: python3 tree.py <sqlite-database> <term-curie> > <html-file>

Creates an HTML page containing the tree structure of the term & its annotations.
HTML is written to stdout.

The sqlite-database must be created by RDFTab (https://github.com/ontodev/rdftab.rs)
and include 'statements' and 'prefixes' tables.

The term-curie must use a prefix from the 'prefixes' table.
"""


def main():
    p = ArgumentParser("tree.py", description="create an HTML page to display an ontology term")
    p.add_argument("db", help="SQLite database")
    p.add_argument("term", help="CURIE of ontology term to display")
    args = p.parse_args()

    treename = os.path.splitext(os.path.basename(args.db))[0]

    with sqlite3.connect(args.db) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        sys.stdout.write(terms2rdfa(cur, treename, [args.term]))


def curie2href(curie):
    """Convert a CURIE to an HREF"""
    return f"?id={curie}".replace("#", "%23")


def curie2iri(prefixes, curie):
    """Convert a CURIE to IRI"""
    for prefix, base in prefixes:
        if curie.startswith(prefix + ":"):
            return curie.replace(prefix + ":", base)
    raise Exception(f"No matching prefix for {curie}")


def dict_factory(cursor, row):
    """Create a dict factory for sqlite cursor"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def term2tree(data, treename, term_id):
    """Create a hiccup-style HTML hierarchy vector for the given term."""
    if treename not in data or term_id not in data[treename]:
        return ""

    tree = data[treename][term_id]
    child_labels = []
    for child in tree["children"]:
        child_labels.append([child, data["labels"].get(child, child)])
    child_labels.sort(key=lambda x: x[1].lower())

    max_children = 100
    children = []
    for child, label in child_labels:
        if child not in data[treename]:
            continue
        predicate = "rdfs:subClassOf"
        oc = child
        object_label = tree_label(data, treename, oc)
        o = ["a", {"rev": predicate, "resource": oc}, object_label]
        attrs = {}
        if len(children) > max_children:
            attrs["style"] = "display: none"
        children.append(["li", attrs, o])
        if len(children) == max_children:
            total = len(tree["children"])
            attrs = {"href": "javascript:show_children()"}
            children.append(
                ["li", {"id": "more"}, ["a", attrs, f"Click to show all {total} ..."]]
            )
    children = ["ul", {"id": "children"}] + children
    if len(children) == 0:
        children = ""
    # <a about="parent_id" rev="rdfs:subClassOf" resource="term_id" href="?id=term_id">entity</a>
    hierarchy = ["ul", ["li", tree_label(data, treename, term_id), children]]
    i = 0
    parents = tree["parents"]
    if parents:
        node = parents[0]
        while node and i < 100:
            i += 1
            oc = node
            object_label = tree_label(data, treename, node)
            parents = data[treename][node]["parents"]
            if len(parents) == 0:
                # No parent
                o = ["a", {"resource": oc, "href": curie2href(term_id)}, object_label]
                hierarchy = ["ul", ["li", o, hierarchy]]
                break
            parent = parents[0]
            if node == parent:
                # Parent is the same
                o = ["a", {"resource": oc, "href": curie2href(term_id)}, object_label]
                hierarchy = ["ul", ["li", o, hierarchy]]
                break
            o = ["a", {"about": parent, "rev": "rdfs:subClassOf", "resource": oc,
                       "href": curie2href(term_id)}, object_label]
            hierarchy = ["ul", ["li", o, hierarchy]]
            node = parent

    hierarchy.insert(1, {"id": "hierarchy", "class": "col-md"})
    return hierarchy


def term2rdfa(cur, prefixes, treename, stanza, term_id):
    """Create a hiccup-style HTML vector for the given term."""
    if len(stanza) == 0:
        return set(), "Not found"

    # Create the tree
    curies = set()
    tree = {}
    cur.execute(
        f"""
      WITH RECURSIVE ancestors(parent, child) AS (
        VALUES ('{term_id}', NULL)
        UNION
        SELECT object AS parent, subject AS child
        FROM statements
        WHERE predicate = 'rdfs:subClassOf'
          AND object = '{term_id}'
        UNION
        SELECT object AS parent, subject AS child
        FROM statements, ancestors
        WHERE ancestors.parent = statements.stanza
          AND statements.predicate = 'rdfs:subClassOf'
          AND statements.object NOT LIKE '_:%'
      )
      SELECT * FROM ancestors"""
    )
    row = None
    for row in cur.fetchall():
        parent = row["parent"]
        if not parent:
            continue
        curies.add(parent)
        if parent not in tree:
            tree[parent] = {
                "parents": [],
                "children": [],
            }
        child = row["child"]
        if not child:
            continue
        curies.add(child)
        if child not in tree:
            tree[child] = {
                "parents": [],
                "children": [],
            }
        tree[parent]["children"].append(child)
        tree[child]["parents"].append(parent)

    data = {"labels": {}, treename: tree}

    stanza.sort(key=lambda x: x["predicate"])

    for row in stanza:
        curies.add(row.get("subject"))
        curies.add(row.get("predicate"))
        curies.add(row.get("object"))
    curies.discard("")
    curies.discard(None)
    ps = set()
    for curie in curies:
        if not isinstance(curie, str) or len(curie) == 0 or curie[0] in ("_", "<"):
            continue
        prefix, local = curie.split(":")
        ps.add(prefix)

    # Get all labels
    labels = {}
    ids = "', '".join(curies)
    cur.execute(
        f"""SELECT subject, value
      FROM statements
      WHERE stanza IN ('{ids}')
        AND predicate = 'rdfs:label'
        AND value IS NOT NULL"""
    )
    for row in cur:
        labels[row["subject"]] = row["value"]
    data["labels"] = labels
    for key in tree.keys():
        if key in labels:
            tree[key]["label"] = labels[key]

    # Select the label used in tree as the primary label
    # (shows same label everywhere if there are multiple labels)
    selected_label = labels[term_id]

    label = term_id
    for row in stanza:
        predicate = row["predicate"]
        value = row["value"]
        if predicate == "rdfs:label" and value == selected_label:
            label = value
            break

    # Add annotations, etc. on right-hand side
    annotation_bnodes = set()
    for row in stanza:
        if row["predicate"] == "rdf:type" and row["object"] == "owl:Axiom":
            annotation_bnodes.add(row["subject"])
    annotations = {}
    for row in stanza:
        subject = row["subject"]
        if subject not in annotation_bnodes:
            continue
        if subject not in annotations:
            annotations[subject] = {"row": {"stanza": row["stanza"]}, "rows": []}
        predicate = row["predicate"]
        if predicate == "rdf:type":
            continue
        elif predicate == "owl:annotatedSource":
            annotations[subject]["row"]["subject"] = row["object"]
            annotations[subject]["source"] = row
        elif predicate == "owl:annotatedProperty":
            annotations[subject]["row"]["predicate"] = row["object"]
            annotations[subject]["property"] = row
        elif predicate == "owl:annotatedTarget":
            annotations[subject]["row"]["object"] = row["object"]
            annotations[subject]["row"]["value"] = row["value"]
            annotations[subject]["row"]["datatype"] = row["datatype"]
            annotations[subject]["row"]["language"] = row["language"]
            annotations[subject]["target"] = row
        else:
            annotations[subject]["rows"].append(row)

    subject = row["subject"]
    si = curie2iri(prefixes, subject)
    subject_label = label

    items = ["ul", {"id": "annotations", "class": "col-md"}]
    s2 = defaultdict(list)
    for row in stanza:
        if row["subject"] == term_id:
            s2[row["predicate"]].append(row)
    pcs = list(s2.keys())
    pcs.sort()
    for predicate in pcs:
        p = ["a", {"href": curie2href(predicate)}, labels.get(predicate, predicate)]
        os = []
        for row in s2[predicate]:
            if predicate == "rdfs:subClassOf" and row["object"].startswith("_:"):
                # TODO - render blank nodes properly
                continue
            o = ["li", row2o(data, row)]
            for key, ann in annotations.items():
                if row != ann["row"]:
                    continue
                ul = ["ul"]
                for a in ann["rows"]:
                    ul.append(["li"] + row2po(prefixes, data, a))
                o.append(
                    [
                        "small",
                        {"resource": key},
                        [
                            "div",
                            {"hidden": "true"},
                            row2o(data, ann["source"]),
                            row2o(data, ann["property"]),
                            row2o(data, ann["target"]),
                        ],
                        ul,
                    ]
                )
                break
            os.append(o)
        if os:
            items.append(["li", p, ["ul"] + os])

    hierarchy = term2tree(data, treename, term_id)
    h2 = ""  # term2tree(data, treename, term_id)

    term = [
        "div",
        {"resource": subject},
        ["h2", subject_label],
        ["a", {"href": si}, si],
        ["div", {"class": "row"}, hierarchy, h2, items],
    ]
    return ps, term


def terms2rdfa(cur, treename, term_ids):
    """Create a hiccup-style HTML vector for the given terms."""
    cur.execute(f"SELECT * FROM prefix ORDER BY length(base) DESC")
    all_prefixes = [(x["prefix"], x["base"]) for x in cur.fetchall()]
    ps = set()
    terms = []
    for term_id in term_ids:
        cur.execute(f"SELECT * FROM statements WHERE stanza = '{term_id}'")
        stanza = cur.fetchall()
        p, t = term2rdfa(cur, all_prefixes, treename, stanza, term_id)
        ps.update(p)
        terms.append(t)

    data = {"labels": {}}

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
        ["link", {"rel": "stylesheet", "href": "../style.css"}],
        ["title", data["labels"].get(term_ids[0], treename + " Browser")],
    ]

    # Create the prefix element
    pref_strs = []
    for prefix, base in all_prefixes:
        pref_strs.append(f"{prefix}: {base}")
    pref_str = "\n".join(pref_strs)

    body = ["body", {"class": "container", "prefix": pref_str}] + terms
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
    body.append(
        [
            "script",
            {"type": "text/javascript"},
            """function show_children() {
        hidden = $('#children li:hidden').slice(0, 100);
        if (hidden.length > 1) {
            hidden.show();
            setTimeout(show_children, 100);
        } else {
            console.log("DONE");
        }
        $('#more').hide();
    }""",
        ]
    )
    html = ["html", head, body]
    output = "Content-Type: text/html\n\n" + render(all_prefixes, html)
    # escaped = output.replace("<","&lt;").replace(">","&gt;")
    # output += f"<pre><code>{escaped}</code></pre>"
    return output


def tree_label(data, treename, s):
    """Retrieve the label of a term."""
    node = data[treename][s]
    return node.get("label", s)


def render(prefixes, element, depth=0):
    """Render hiccup-style HTML vector as HTML."""
    indent = "  " * depth
    if not isinstance(element, list):
        raise Exception(f"Element is not a list: {element}")
    if len(element) == 0:
        raise Exception(f"Element is an empty list")
    tag = element.pop(0)
    if not isinstance(tag, str):
        raise Exception(f"Tag '{tag}' is not a string in '{element}'")
    output = f"{indent}<{tag}"

    if len(element) > 0 and isinstance(element[0], dict):
        attrs = element.pop(0)
        if tag == "a" and "href" not in attrs and "resource" in attrs:
            attrs["href"] = curie2href(attrs["resource"])
        for key, value in attrs.items():
            if key in ["checked"]:
                if value:
                    output += f" {key}"
            else:
                output += f' {key}="{value}"'

    if tag in ["meta", "link"]:
        output += "/>"
        return output
    output += ">"
    spacing = ""
    if len(element) > 0:
        for child in element:
            if isinstance(child, str):
                output += child
            elif isinstance(child, list):
                try:
                    output += "\n" + render(prefixes, child, depth=depth + 1)
                    spacing = f"\n{indent}"
                except Exception as e:
                    raise Exception(f"Bad child in '{element}'", e)
            else:
                raise Exception(f"Bad type for child '{child}' in '{element}'")
    output += f"{spacing}</{tag}>"
    return output


def row2o(data, row):
    """Convert an object from a sqlite query to hiccup-style HTML."""
    predicate = row["predicate"]
    obj = row["object"]
    if isinstance(obj, str):
        if obj.startswith("<"):
            iri = obj[1:-1]
            return ["a", {"rel": predicate, "href": iri}, iri]
        elif obj.startswith("_:"):
            return ["span", {"property": predicate}, obj]
        else:
            return [
                "a",
                {"rel": predicate, "resource": obj},
                data["labels"].get(obj, obj),
            ]
    # TODO: OWL expressions
    # TODO: other blank objects
    # TODO: datatypes
    # TODO: languages
    elif row["value"]:
        return ["span", {"property": predicate}, row["value"]]


def row2po(prefixes, data, row):
    """Convert a predicate and object from a sqlite query result row to hiccup-style HTML."""
    predicate = row["predicate"]
    predicate_label = data["labels"].get(predicate, predicate)
    p = ["a", {"href": curie2href(predicate)}, predicate_label]
    o = row2o(data, row)
    return [p, o]


if __name__ == "__main__":
    main()
