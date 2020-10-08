#!/usr/bin/env python3

import logging
import os
import sqlite3
import sys

from argparse import ArgumentParser
from collections import defaultdict
from gizmos.hiccup import curie2href, render

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

OWL_PREFIX = "http://www.w3.org/2002/07/owl#{}"


def main():
    p = ArgumentParser("tree.py", description="create an HTML page to display an ontology term")
    p.add_argument("db", help="SQLite database")
    p.add_argument("term", help="CURIE of ontology term to display", nargs="?")
    p.add_argument(
        "-i",
        "--include-db",
        help="If provided, include db param in query string",
        action="store_true",
    )
    args = p.parse_args()

    treename = os.path.splitext(os.path.basename(args.db))[0]
    if args.term:
        term = [args.term]
    else:
        term = None

    with sqlite3.connect(args.db) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        sys.stdout.write(terms2rdfa(cur, treename, term, include_db=args.include_db))


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


def term2tree(data, treename, term_id, include_db=False):
    """Create a hiccup-style HTML hierarchy vector for the given term."""
    if treename not in data or term_id not in data[treename]:
        return ""

    db = None
    if include_db:
        db = treename

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
                o = ["a", {"resource": oc, "href": curie2href(node, db)}, object_label]
                hierarchy = ["ul", ["li", o, hierarchy]]
                break
            parent = parents[0]
            if node == parent:
                # Parent is the same
                o = ["a", {"resource": oc, "href": curie2href(node, db)}, object_label]
                hierarchy = ["ul", ["li", o, hierarchy]]
                break
            o = [
                "a",
                {
                    "about": parent,
                    "rev": "rdfs:subClassOf",
                    "resource": oc,
                    "href": curie2href(node, db),
                },
                object_label,
            ]
            hierarchy = ["ul", ["li", o, hierarchy]]
            node = parent

    hierarchy.insert(1, {"id": "hierarchy", "class": "col-md"})
    return hierarchy


def term2rdfa(cur, prefixes, treename, stanza, term_id, include_db=False, add_children=None):
    """Create a hiccup-style HTML vector for the given term."""
    if stanza and len(stanza) == 0:
        return set(), "Not found"

    db = None
    if include_db:
        db = treename

    # A set that will be filled in with all of the compact URIs in the given stanza:
    curies = set()

    # A tree that we will generate to describe all of the given term's relationships with its
    # children and ancestors.
    tree = {}

    # The query we will use to generate the tree:
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
    res = cur.fetchall()
    if add_children:
        res.extend([{"parent": term_id, "child": child} for child in add_children])
    for row in res:
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

    # The subjects in the stanza that are of type owl:Axiom:
    annotation_bnodes = set()
    for row in stanza:
        if row["predicate"] == "rdf:type" and row["object"] == "owl:Axiom":
            annotation_bnodes.add(row["subject"])

    # Annotations, etc. on the right-hand side for the subjects contained in
    # annotation_bnodes:
    annotations = {}
    row = None
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
    if row:
        subject = row["subject"]
        si = curie2iri(prefixes, subject)
        subject_label = label
    else:
        subject = term_id
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
        anchor = [
            "a",
            {"href": curie2href(predicate, db)},
            labels.get(predicate, predicate),
        ]
        # Initialise an empty list of "o"s, i.e., hiccup representations of objects:
        os = []
        for row in s2[predicate]:
            # Convert the `data` map, that has entries for the tree and for a list of the labels
            # corresponding to all of the curies in the stanza, into a hiccup object `o`:
            o = ["li", row2o(stanza, data, row)]

            # Render the annotations for the current row:
            for key, ann in annotations.items():
                if row != ann["row"]:
                    continue
                # Use the data map and the annotations rows to generate some hiccup for the
                # annotations, which we then append to our `o`:
                ul = ["ul"]
                for a in ann["rows"]:
                    ul.append(["li"] + row2po(stanza, data, a, db))
                o.append(
                    [
                        "small",
                        {"resource": key},
                        [
                            "div",
                            {"hidden": "true"},
                            row2o(stanza, data, ann["source"]),
                            row2o(stanza, data, ann["property"]),
                            row2o(stanza, data, ann["target"]),
                        ],
                        ul,
                    ]
                )
                break
            # Append the `o` to the list of `os`:
            os.append(o)
        if os:
            items.append(["li", anchor, ["ul"] + os])

    hierarchy = term2tree(data, treename, term_id, include_db=include_db)
    h2 = ""  # term2tree(data, treename, term_id)

    term = [
        "div",
        {"resource": subject},
        ["h2", subject_label],
        ["a", {"href": si}, si],
        ["div", {"class": "row"}, hierarchy, h2, items],
    ]
    return ps, term


def thing2rdfa(cur, all_prefixes, treename, include_db=False):
    """Create a hiccup-style HTML vector for owl:Thing as the parent of all top-level terms."""
    # Select all classes without parents and set them as children of owl:Thing
    cur.execute(
        """SELECT DISTINCT subject FROM statements 
        WHERE subject NOT IN 
            (SELECT subject FROM statements
             WHERE predicate = 'rdfs:subClassOf')
        AND subject IN 
            (SELECT subject FROM statements 
             WHERE predicate = 'rdf:type'
             AND object = 'owl:Class' AND subject NOT LIKE '_:%');"""
    )
    res = cur.fetchall()
    add_children = [x["subject"] for x in res if x["subject"] != "owl:Thing"]
    cur.execute(f"SELECT * FROM statements WHERE stanza = 'owl:Thing'")
    stanza = cur.fetchall()
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
        cur,
        all_prefixes,
        treename,
        stanza,
        "owl:Thing",
        include_db=include_db,
        add_children=add_children,
    )


def terms2rdfa(cur, treename, term_ids, include_db=False):
    """Create a hiccup-style HTML vector for the given terms.
    If there are no terms, create the HTML vector for owl:Thing."""
    cur.execute("SELECT * FROM prefix ORDER BY length(base) DESC")
    all_prefixes = [(x["prefix"], x["base"]) for x in cur.fetchall()]
    ps = set()
    terms = []
    if not term_ids:
        # First check if there are terms that are declared children of owl:Thing
        # If results exits, the normal tree script will run with term owl:Thing
        cur.execute(
            """SELECT DISTINCT subject FROM statements
            WHERE predicate = 'rdfs:subClassOf' AND object = 'owl:Thing';"""
        )
        res = cur.fetchall()
        term_ids = ["owl:Thing"]
        if not res:
            # No declared children of owl:Thing, find the top-level ourselves
            p, t = thing2rdfa(cur, all_prefixes, treename, include_db=include_db)
            ps.update(p)
            terms.append(t)

    # Run for given terms if terms have not yet been filled out
    if not terms:
        for term_id in term_ids:
            cur.execute(f"SELECT * FROM statements WHERE stanza = '{term_id}'")
            stanza = cur.fetchall()
            p, t = term2rdfa(cur, all_prefixes, treename, stanza, term_id, include_db=include_db)
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

    db = None
    if include_db:
        db = treename
    output = "Content-Type: text/html\n\n" + render(all_prefixes, html, db)
    # escaped = output.replace("<","&lt;").replace(">","&gt;")
    # output += f"<pre><code>{escaped}</code></pre>"
    return output


def tree_label(data, treename, s):
    """Retrieve the label of a term."""
    node = data[treename][s]
    return node.get("label", s)


def row2o(_stanza, _data, _uber_row):
    """Given a stanza, a map (`_data`) with entries for the tree structure of the stanza and for all
    of the labels in it, and a row in the stanza, convert the row to hiccup-style HTML."""

    def renderNonBlank(given_row):
        """Renders the non-blank object from the given row"""
        return [
            "a",
            {"rel": given_row["predicate"], "resource": given_row["object"]},
            _data["labels"].get(given_row["object"], given_row["object"]),
        ]

    def renderLiteral(given_row):
        """Renders the object contained in the given row as a literal IRI"""
        # Literal IRIs are enclosed in angle brackets.
        iri = given_row["object"][1:-1]
        return ["a", {"rel": given_row["predicate"], "href": iri}, iri]

    def getOwlOperands(given_row):
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

    def renderNaryRelation(class_pred, operands):
        """Render an n-ary relation using the given predicate and operands"""
        if len(operands) < 2:
            LOGGER.error(
                f"Something is wrong. Wrong number of operands to '{class_pred}': {operands}"
            )
            return ["div"]

        if class_pred == "owl:intersectionOf":
            operator = "and"
        elif class_pred == "owl:unionOf":
            operator = "or"
        elif class_pred == "owl:oneOf":
            operator = "one of"
        else:
            LOGGER.error(f"Unrecognized predicate for n-ary relation: {class_pred}")
            return ["div"]

        owl_div = ["span", {"rel": class_pred}, " ", "("]
        for idx, operand in enumerate(operands):
            owl_div.append(operand)
            if (idx + 1) < len(operands):
                owl_div += [" ", operator, " "]
        owl_div.append(")")
        return owl_div

    def renderUnaryRelation(class_pred, operands):
        """Render a unary relation using the given predicate and operands"""
        if len(operands) != 1:
            LOGGER.error(
                f"Something is wrong. Wrong number of operands to '{class_pred}': {operands}"
            )
            return ["div"]

        if class_pred == "owl:complementOf":
            operator = "not"
        else:
            LOGGER.error(f"Unrecognized predicate for unary relation: {class_pred}")
            return ["div"]

        operand = operands[0]
        owl_div = ["span", {"rel": class_pred}, operator, " ", operand]
        return owl_div

    def renderOwlRestriction(given_rows):
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

    def renderOwlClassExpression(given_rows, rel=None):
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
        if class_pred in ["owl:intersectionOf", "owl:unionOf", "owl:oneOf"]:
            hiccup.append(renderNaryRelation(class_pred, operands))
        elif class_pred == "owl:complementOf":
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

    # Here, we expect "outer rows" only, i.e., rows that have non-blank nodes as their subject. The
    # inner functions above will handle rows with blank subjects. This means that the check below
    # shouldn't really be necessary, since the caller should not send us an input row with a blank
    # subject, but we double-check anyway:
    if uber_subj.startswith("_:"):
        LOGGER.error("Received row with blank subject in row2o; returning empty div")
        return ["div"]

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


def row2po(stanza, data, row, db):
    """Convert a predicate and object from a sqlite query result row to hiccup-style HTML."""
    predicate = row["predicate"]
    predicate_label = data["labels"].get(predicate, predicate)
    p = ["a", {"href": curie2href(predicate, db)}, predicate_label]
    o = row2o(stanza, data, row)
    return [p, o]


if __name__ == "__main__":
    main()
