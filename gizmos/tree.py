#!/usr/bin/env python3

import os
import sqlite3
import sys

from argparse import ArgumentParser
from collections import defaultdict

# TODO: remove this import
from pprint import pprint, pformat


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
    # print("*********** " + treename + " ************")

    with sqlite3.connect(args.db) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        terms2rdfa(cur, treename, [args.term])
        # Commented out only temporarily:
        # sys.stdout.write(terms2rdfa(cur, treename, [args.term]))


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
            children.append(["li", {"id": "more"}, ["a", attrs, f"Click to show all {total} ..."]])
    children = ["ul", {"id": "children"}] + children
    if len(children) == 0:
        children = ""

    hierarchy = ["ul", ["li", tree_label(data, treename, term_id), children]]
    i = 0
    parents = tree["parents"]
    if parents:
        node = parents[0]
        while node and i < 100:
            i += 1
            predicate = "rdfs:subClassOf"
            oc = node
            object_label = tree_label(data, treename, node)
            o = ["a", {"rel": predicate, "resource": oc}, object_label]
            hierarchy = ["ul", ["li", o, hierarchy]]
            parents = data[treename][node]["parents"]
            if len(parents) == 0:
                break
            parent = parents[0]
            if node == parent:
                break
            node = parent

    hierarchy.insert(1, {"id": "hierarchy", "class": "col-md"})
    return hierarchy


def term2rdfa(cur, prefixes, treename, stanza, term_id):
    """Create a hiccup-style HTML vector for the given term."""
    if len(stanza) == 0:
        return set(), "Not found"

    # A set that will be filled in with all of the compact URIs in the given stanza:
    curies = set()

    # A tree that we will generate to describe all of the given term's relationships with its
    # children and ancestors.
    tree = {}
    cur.execute(
        f"""
      WITH RECURSIVE ancestors(parent, child) AS (
        VALUES ('{term_id}', NULL)
        UNION
        -- The children of the given term:
        SELECT object AS parent, subject AS child
        FROM statements
        WHERE predicate = 'rdfs:subClassOf'
          AND object = '{term_id}'
        UNION
        -- The non-blank parents of all of the parent terms extracted so far:
        SELECT object AS parent, subject AS child
        FROM statements, ancestors
        WHERE ancestors.parent = statements.stanza
          AND statements.predicate = 'rdfs:subClassOf'
          AND statements.object NOT LIKE '_:%'
      )
      SELECT * FROM ancestors"""
    )
    for row in cur.fetchall():
        # print("Got child: {} with parent: {}".format(row["child"], row["parent"]))
        # Consider the parent column of the current row:
        parent = row["parent"]
        if not parent:
            continue
        # If it is not null, add it to the list of all of the compact URIs described by this tree:
        curies.add(parent)
        # If it is not already in the tree, add a new entry for it to the tree:
        if parent not in tree:
            tree[parent] = {
                "parents": [],
                "children": [],
            }

        # Consider the child column of the current row:
        child = row["child"]
        if not child:
            continue
        # If it is not null, add it to the list of all the compact URIs described by this tree:
        curies.add(child)
        # If the child is not already in the tree, add a new entry for it to the tree:
        if child not in tree:
            tree[child] = {
                "parents": [],
                "children": [],
            }

        # Fill in the approprate relationships in the entries for the parent and child:
        tree[parent]["children"].append(child)
        tree[child]["parents"].append(parent)

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

    # Initialise a map with one entry for the tree and one for all of the labels corresponding to
    # all of the compact URIs in the stanza:
    data = {"labels": labels, treename: tree}

    # If the compact URIs in the labels map are also in the tree, then add the label info to the
    # corresponding node in the tree:
    for key in tree.keys():
        if key in labels:
            tree[key]["label"] = labels[key]

    # print("Your prefixes are: {}".format(pformat(ps)))
    # print("Your curies are: {}".format(pformat(curies)))
    # print("Your labels are: {}".format(pformat(labels)))
    # print("Your tree is: {}".format(pformat(tree)))
    # print(''.join(['*' for i in range(0,80)]))

    # Determine the label to use for the given term id when generating RDFa (the term might have
    # multiple labels, in which case we will just choose one and show it everywhere). This defaults
    # to the term id itself, unless there is a label for the term in the stanza corresponding to the
    # label for that term in the labels map:
    selected_label = labels[term_id]
    label = term_id
    for row in stanza:
        predicate = row["predicate"]
        value = row["value"]
        if predicate == "rdfs:label" and value == selected_label:
            label = value
            break

    # The subjects in the stanza that are of type owl:Axiom:
    annotation_bnodes = set()
    for row in stanza:
        if row["predicate"] == "rdf:type" and row["object"] == "owl:Axiom":
            annotation_bnodes.add(row["subject"])

    # Annotations, etc. on the right-hand side for the subjects contained in
    # annotation_bnodes:
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

    # Note that in python, a variable `foo` declared in the scope of a for loop is available to
    # be referred to _after_ the end of the for loop. For example, the following works in python:
    # for foo in myList:
    #   ...
    # ...
    # print(foo)
    #
    # The variable `row` below is the last `row` retrieved from the immediately preceeding for loop.
    subject = row["subject"]
    si = curie2iri(prefixes, subject)
    subject_label = label

    # The initial hiccup, which will be filled in later:
    items = ["ul", {"id": "annotations", "class": "col-md"}]

    # s2 maps the predicates of the given term to their corresponding rows (there can be more than
    # one row per predicate):
    s2 = defaultdict(list)
    for row in stanza:
        if row["subject"] == term_id:
            s2[row["predicate"]].append(row)

    # Loop through the rows of the stanza that correspond to the predicates of the given term:
    pcs = list(s2.keys())
    pcs.sort()
    for predicate in pcs:
        anchor = ["a", {"href": curie2href(predicate)}, labels.get(predicate, predicate)]
        # Initialise an empty list of "o"s, i.e., hiccup representations of objects:
        os = []
        for row in s2[predicate]:
            # Convert the `data` map, that has entries for the tree and for a list of the labels
            # corresponding to all of the curies in the stanza, into a hiccup object `o`:
            o = ["li", row2o(cur, data, row)]

            # Render the annotations for the current row:
            for key, ann in annotations.items():
                if row != ann["row"]:
                    continue
                # Use the data map and the annotations rows to generate some hiccup for the
                # annotations, which we then append to our `o`:
                ul = ["ul"]
                for a in ann["rows"]:
                    ul.append(["li"] + row2po(cur, data, a))
                o.append(
                    [
                        "small",
                        {"resource": key},
                        [
                            "div",
                            {"hidden": "true"},
                            row2o(cur, data, ann["source"]),
                            row2o(cur, data, ann["property"]),
                            row2o(cur, data, ann["target"]),
                        ],
                        ul,
                    ]
                )
                break
            # Append the `o` to the list of `os`:
            os.append(o)
        if os:
            items.append(["li", anchor, ["ul"] + os])

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


def renderBlankNode(cur, data, row):
    """TODO: INSERT DOCSTRING HERE"""

    def renderOperands(given_row):
        """ TODO: docstring goes here """
        print("Finding operands for {} ...".format(given_row["predicate"]))
        cur.execute(
            """SELECT *
            FROM statements
            WHERE stanza = '{stanza}'
            AND subject = '{obj}'""".format(
                stanza=given_row["stanza"], obj=given_row["object"]
            )
        )
        inner_rows = [row for row in cur]
        operands = []
        for inner_row in inner_rows:
            inner_subj = inner_row["subject"]
            inner_pred = inner_row["predicate"]
            inner_obj = inner_row["object"]
            print(f"Found row with <s,p,o> = <{inner_subj}, {inner_pred}, {inner_obj}>")

            if inner_pred == "rdf:rest" and inner_obj != "rdf:nil":
                operands += renderOperands(inner_row)
                print("Returned from recursing (rdf:rest).")
            elif inner_pred == "rdf:first" or not inner_pred.startswith("rdf:"):
                if inner_obj.startswith("_:"):
                    print(f"{inner_pred} points to a blank node, following the trail ...")
                    operands += renderOperands(inner_row)
                    print(f"Returned from recursing (blank {inner_pred}).")
                else:
                    print(f"Rendering non-blank object of {inner_pred}")
                    operands.append(row2o(cur, data, inner_row))

        return operands

    def renderEquivalentClass(given_row):
        # There should be only one row returned from this query, which fetches the row describing
        # the object of the owl:equivalentClass predicate for the given stanza:
        cur.execute(
            """SELECT *
            FROM statements
            WHERE stanza = '{stanza}'
            AND subject = '{obj}'
            AND predicate LIKE 'owl:%'""".format(
                stanza=given_row["stanza"], obj=given_row["object"]
            )
        )
        ec_row = next(cur)
        ec_subj = ec_row["subject"]
        ec_pred = ec_row["predicate"]
        ec_obj = ec_row["object"]

        operands = renderOperands(ec_row)
        owl_div = []
        if ec_pred in ["owl:intersectionOf", "owl:unionOf"]:
            print(f"Rendering <s,p,o> = <{ec_subj}, {ec_pred}, {ec_obj}> ...")
            tag = "conjunction" if ec_pred == "owl:intersectionOf" else "disjunction"
            operator = "and" if ec_pred == "owl:intersectionOf" else "or"
            owl_div += [tag, " ", "("]
            for idx, operand in enumerate(operands):
                owl_div.append(operand)
                if (idx + 1) < len(operands):
                    owl_div += [" ", operator, " "]
            owl_div.append(")")
            pprint(owl_div)
            return owl_div
        elif ec_pred == "owl:complementOf":
            print(f"Rendering <s,p,o> = <{ec_subj}, {ec_pred}, {ec_obj}> ...")
            owl_div += ["not", " " "("]
            if len(operands) > 1:
                print("Something is wrong. Too many operands to 'NOT'")
            for idx, operand in enumerate(operands):
                owl_div.append(operand)
                if (idx + 1) < len(operands):
                    owl_div += [" "]
            owl_div.append(")")
            pprint(owl_div)
            return owl_div
        else:
            print(
                f"Rendering for <s,p,o> = <{ec_subj}, {ec_pred}, {ec_obj}> is not yet implemented"
            )
            return ["div"]

    subj = row["subject"]
    predicate = row["predicate"]
    obj = row["object"]
    # TODO: OWL expressions <-- Focus on this one for now.
    # TODO: other blank objects
    if predicate == "rdfs:subClassOf":
        # In the old `tree.py` code we were skipping subClassOf, and in knotation we also
        # don't render this one. So maybe let's skip it here as well at least for now:
        return ["span"]
    elif predicate == "owl:equivalentClass":
        return renderEquivalentClass(row)
    else:
        print(f"Handling of {predicate} not implemented yet.")
        return ["span", {"property": predicate}, obj]


def row2o(cur, data, row):
    """Convert an object from a sqlite query to hiccup-style HTML."""
    subj = row["subject"]
    predicate = row["predicate"]
    obj = row["object"]
    if isinstance(obj, str):
        # TODO: datatypes
        # TODO: languages
        if obj.startswith("<"):
            # Literal IRIs are enclosed in angle brackets.
            iri = obj[1:-1]
            return ["a", {"rel": predicate, "href": iri}, iri]
        elif obj.startswith("_:"):
            # Blank nodes
            print(f"Rendering triple with blank object <s,p,o> = <{subj}, {predicate}, {obj}> ...")
            return renderBlankNode(cur, data, row)
        else:
            unary_op = ["span", "not"] if predicate == "owl:complementOf" else ["span"]
            return [
                "span",
                unary_op,
                ["a", {"rel": predicate, "resource": obj}, data["labels"].get(obj, obj)],
            ]
    elif row["value"]:
        return ["span", {"property": predicate}, row["value"]]


def row2po(cur, data, row):
    """Convert a predicate and object from a sqlite query result row to hiccup-style HTML."""
    predicate = row["predicate"]
    predicate_label = data["labels"].get(predicate, predicate)
    p = ["a", {"href": curie2href(predicate)}, predicate_label]
    o = row2o(cur, data, row)
    return [p, o]


if __name__ == "__main__":
    main()
