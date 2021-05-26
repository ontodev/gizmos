#!/usr/bin/env python3

import csv
import json
import re
import sqlite3
import sys

from argparse import ArgumentParser
from collections import OrderedDict
from copy import deepcopy
from gizmos.hiccup import render
from pprint import pformat
from rdflib import Graph, BNode, URIRef, Literal

from util import compare_graphs

TSV = "tests/thin.tsv"
EXPECTED_OWL = 'example.rdf'
#TSV = "build/obi_core.tsv"
#EXPECTED_OWL = 'tests/resources/obi_core_no_trailing_ws.owl'

# Create an OrderedDict of prefixes, sorted in descending order by the length
# of the prefix's long form:
prefixes = []
with open("tests/resources/prefix.tsv") as fh:
    rows = csv.DictReader(fh, delimiter="\t")
    for row in rows:
        if row.get("prefix"):
            prefixes.append((row["prefix"], row["base"]))
prefixes.sort(key=lambda x: len(x[1]), reverse=True)
prefixes = OrderedDict(prefixes)

nesting = 0
def log(message):
    global nesting
    message = message.replace('\n', '\n' + '    '*nesting)
    print('    '*nesting, end='', file=sys.stderr)
    print(message, file=sys.stderr)

# def dict_factory(cursor, row):
#     d = {}
#     for idx, col in enumerate(cursor.description):
#         d[col[0]] = row[idx]
#     return d
# con = sqlite3.connect('obi_core.db')
# con.row_factory = dict_factory
# cur = con.cursor()
# thin = []
# for row in cur.execute('SELECT * FROM statements'):
#     thin.append(row)

def renderSubjects(subjects):
    """Print a nested subject dict as indented lines.
    From
        {"ex:s": {"ex:p": [{"object": "ex:o"}]}}
    to
        ex:s
          ex:p
            {"object": "ex:o"}
    """
    for subject_id in sorted(list(subjects.keys())):
        print(subject_id)
        for predicate in sorted(list(subjects[subject_id].keys())):
            print(" ", predicate)
            for obj in subjects[subject_id][predicate]:
                print("   ", obj)

def row2objectMap(row):
    """Convert a row dict to an object map.
    From
        {"subject": "ex:s", "predicate": "ex:p", "object": "ex:o"}
    to
        {"object": "ex:o"}
        {"value": "Foo"}
        {"value": "Foo", "language": "en"}
        {"value": "0.123", "datatype": "xsd:float"}
    """
    if row.get("object"):
        return {"object": row["object"]}
    elif row.get("value") is not None:
        if row.get("datatype"):
            return {"value": row["value"], "datatype": row["datatype"]}
        elif row.get("language"):
            return {"value": row["value"], "language": row["language"]}
        elif row["value"]:
            return {"value": row["value"]}

    log("Invalid RDF row {}".format(row))
    raise Exception("Invalid RDF row")


def thin2subjects(thin):
    """Convert a list of thin rows to a nested subjects map:
    From
        [{"subject": "ex:s", "predicate": "ex:p", "object": "ex:o"}]
    to
        {"ex:s": {"ex:p": [{"object": "ex:o"}]}}
    """
    dependencies = {}
    subject_ids = set(x["subject"] for x in thin)
    subjects = {}

    # Convert rows to a subject dict.
    for subject_id in subject_ids:
        predicates = {}
        for row in thin:
            if row["subject"] != subject_id:
                continue
            predicate = row["predicate"]
            if predicate not in predicates:
                predicates[predicate] = []
            objects = predicates[predicate]
            objects.append(row2objectMap(row))
            objects.sort(key=lambda k: str(k))
            predicates[predicate] = objects
            if row.get("object") and row["object"].startswith("_:"):
                if not subject_id in dependencies:
                    dependencies[subject_id] = set()
                dependencies[subject_id].add(row["object"])
        subjects[subject_id] = predicates

    # Work from leaves to root, nesting the blank structures.
    last_leaves = 0
    while dependencies:
        leaves = set(subjects.keys()) - set(dependencies.keys())
        if len(leaves) == last_leaves:
            # This is not necessarily a problem, so we comment out the `break` statement here, but
            # we emit a warning anyway.
            log("LOOP!?")
            # break
        last_leaves = len(leaves)
        dependencies = {}
        handled = set()
        for subject_id, predicates in subjects.items():
            for predicate in predicates.keys():
                objects = []
                for obj in predicates[predicate]:
                    if not obj:
                        log("Bad object: <{} {} {}>".format(subject_id, predicate, obj))
                        continue
                    o = obj.get("object")
                    if o and isinstance(o, str) and o.startswith("_:"):
                        if o in leaves:
                            obj = {"object": subjects[o]}
                            handled.add(o)
                        else:
                            if not subject_id in dependencies:
                                dependencies[subject_id] = set()
                            dependencies[subject_id].add(o)
                    objects.append(obj)
                objects.sort(key=lambda k: str(k))
                predicates[predicate] = objects
        for subject_id in handled:
            del subjects[subject_id]

    remove = set()
    subjects_copy = {}
    for subject_id in sorted(subjects.keys()):
        if not subjects_copy.get(subject_id):
            subjects_copy[subject_id] = deepcopy(subjects[subject_id])

        if subjects_copy[subject_id].get("owl:annotatedSource"):
            subject = firstObject(subjects_copy[subject_id], "owl:annotatedSource")
            predicate = firstObject(subjects_copy[subject_id], "owl:annotatedProperty")
            obj = firstObject(subjects_copy[subject_id], "owl:annotatedTarget")

            del subjects_copy[subject_id]["owl:annotatedSource"]
            del subjects_copy[subject_id]["owl:annotatedProperty"]
            del subjects_copy[subject_id]["owl:annotatedTarget"]
            del subjects_copy[subject_id]["rdf:type"]

            if not subjects_copy.get(subject):
                subjects_copy[subject] = deepcopy(subjects[subject])
            if not subjects_copy[subject].get(predicate):
                subjects_copy[subject][predicate] = deepcopy(subjects[subject][predicate])

            objs = subjects_copy[subject][predicate]
            objs_copy = []
            for o in objs:
                o = deepcopy(o)
                if o.get("object") == obj or o.get("value") == obj:
                    o["annotations"] = subjects_copy[subject_id]
                    remove.add(subject_id)
                objs_copy.append(o)
            subjects_copy[subject][predicate] = objs_copy

        if subjects_copy[subject_id].get("rdf:subject"):
            subject = firstObject(subjects_copy[subject_id], "rdf:subject")
            predicate = firstObject(subjects_copy[subject_id], "rdf:predicate")
            obj = firstObject(subjects_copy[subject_id], "rdf:object")

            del subjects_copy[subject_id]["rdf:subject"]
            del subjects_copy[subject_id]["rdf:predicate"]
            del subjects_copy[subject_id]["rdf:object"]
            del subjects_copy[subject_id]["rdf:type"]

            if not subjects_copy.get(subject):
                subjects_copy[subject] = deepcopy(subjects[subject])
            if not subjects_copy[subject].get(predicate):
                subjects_copy[subject][predicate] = deepcopy(subjects[subject][predicate])

            objs = subjects_copy[subject][predicate]
            objs_copy = []
            for o in objs:
                o = deepcopy(o)
                if o.get("object") == obj:
                    o["metadata"] = subjects_copy[subject_id]
                    remove.add(subject_id)
                objs_copy.append(o)
            subjects_copy[subject][predicate] = objs_copy

    for t in remove:
        del subjects_copy[t]

    return subjects_copy


def subjects2thick(subjects):
    """Convert a nested subjects map to thick rows.
    From
        {"ex:s": {"ex:p": [{"object": {"ex:a": [{"value": "A"}]}}]}}
    to
        {"subject": "ex:s", "predicate": "ex:p", "object": "{\"ex:a\":[{\"value\": \"A\"}]}"}
    """
    rows = []
    for subject_id in sorted(list(subjects.keys())):
        for predicate in sorted(list(subjects[subject_id].keys())):
            for obj in subjects[subject_id][predicate]:
                result = {
                    "subject": subject_id,
                    "predicate": predicate,
                    **obj
                }
                if result.get("object") and not isinstance(result["object"], str):
                    result["object"] = json.dumps(result["object"])
                rows.append(result)
    return rows


def thick2subjects(thick):
    pass


### thick to Turtle

def shorten(content):
    if isinstance(content, URIRef):
        m = re.compile(r"(http:\S+(#|\/))(.*)").match(content)
        if m:
            for key in prefixes:
                if m[1] == prefixes[key]:
                    return "{}:{}".format(key, m[3])
    if content.startswith("http"):
        content = "<{}>".format(content)
    return content

def render_graph(graph, fh=sys.stdout):
    ttls = sorted([(s, p, o) for s, p, o in graph])
    for subj, pred, obj in ttls:
        print("{} {} ".format(shorten(subj), shorten(pred)), end="", file=fh)
        if isinstance(obj, Literal) and obj.datatype:
            print('"{}"^^{} '.format(obj.value, shorten(obj.datatype)), end="", file=fh)
        elif isinstance(obj, Literal) and obj.language:
            print('"{}"@{} '.format(obj.value, obj.language), end="", file=fh)
        else:
            print("{} ".format(shorten(obj)), end="", file=fh)
        print(".", file=fh)

def deprefix(content):
    m = re.compile(r"([\w\-]+):(.*)").match(content)
    if m and prefixes.get(m[1]):
        return "{}{}".format(prefixes[m[1]], m[2])

def create_node(content):
    if isinstance(content, URIRef):
        return content
    elif isinstance(content, str) and content.startswith('_:'):
        return BNode(content)
    elif isinstance(content, str) and (content.startswith('<')):
        return URIRef(content.strip('<>'))
    elif isinstance(content, str):
        deprefixed_content = deprefix(content)
        return URIRef(deprefixed_content) if deprefixed_content else Literal(content)
    else:
        if isinstance(content, dict) and 'value' in content and 'language' in content:
            return Literal(content['value'], lang=content['language'])
        elif isinstance(content, dict) and 'value' in content and 'datatype' in content:
            deprefixed_datatype = deprefix(content['datatype'])
            datatype = URIRef(content['datatype']) if not deprefixed_datatype \
                else URIRef(deprefixed_datatype)
            return(Literal(content['value'], datatype=datatype))
        else:
            log("WARNING: Could not create a node corresponding to content. Defaulting to Literal")
            return Literal(format(content))

b_id = 0
def thick2triples(_subject, _predicate, _thick_row):
    if 'object' not in _thick_row and 'value' not in _thick_row:
        raise Exception(f"Don't know how to handle thick_row without value or object: {_thick_row}")

    def predicateMap2triples(pred_map):
        global b_id
        b_id += 1
    
        bnode = f"_:myb{b_id}"
        triples = []
        for predicate, objects in pred_map.items():
            for obj in objects:
                triples += thick2triples(bnode, predicate, obj)
        return triples

    def decompress(thick_row, target, target_type, decomp_type):
        spo_mappings = {
            'annotations': {
                'subject': 'owl:annotatedSource',
                'predicate': 'owl:annotatedProperty',
                'object': 'owl:annotatedTarget'
            },
            'metadata': {
                'subject': 'rdf:subject',
                'predicate': 'rdf:predicate',
                'object': 'rdf:object'
            }
        }
        annodata_subj = spo_mappings[decomp_type]['subject']
        annodata_pred = spo_mappings[decomp_type]['predicate']
        annodata_obj = spo_mappings[decomp_type]['object']

        if isinstance(target, str) or 'value' in target:
            annodata = {annodata_obj: [{target_type: target}]}
        else:
            annodata = {annodata_obj: [{target_type: predicateMap2triples(target)}]}

        annodata[annodata_subj] = [{'object': thick_row['subject']}]
        annodata[annodata_pred] = [{'object': thick_row['predicate']}]
        object_type = 'owl:Axiom' if decomp_type == 'annotations' else 'rdf:Statement'
        annodata['rdf:type'] = [{'object': object_type}]
        for key in thick_row[decomp_type]:
            annodata[key] = thick_row[decomp_type][key]
        return annodata

    def obj2triples(thick_row):
        global b_id

        target = thick_row['object']
        triples = []
        if isinstance(target, list):
            for t in target:
                triples += thick2triples(t['subject'], t['predicate'], t)
            # This is extremely hacky but it should work because of the order in which the ids
            # are generated here. See also the similar comment below. In that case ids are generated
            # in ascending order.
            next_id = b_id - 1
            triples.append({'subject': create_node(_subject),
                            'predicate': create_node(_predicate),
                            'object': create_node(f"_:myb{next_id}")})
        elif not isinstance(target, str):
            # This is a hacky way of doing this, but the logic is right. We need to save
            # the b_id here because predicateMap2Triples is a recursive function and it will
            # increment the b_id every time it is called. What we need here is just whatever the
            # next id will be.
            next_id = b_id + 1
            triples += predicateMap2triples(target)
            triples.append({'subject': create_node(_subject),
                            'predicate': create_node(_predicate),
                            'object': create_node(f"_:myb{next_id}")})
        else:
            triples.append({'subject': create_node(_subject),
                            'predicate': create_node(_predicate),
                            'object': create_node(target)})

        if 'annotations' in thick_row:
            triples += predicateMap2triples(decompress(thick_row, target, 'object', 'annotations'))

        if 'metadata' in thick_row:
            triples += predicateMap2triples(decompress(thick_row, target, 'object', 'metadata'))

        return triples

    def val2triples(thick_row):
        target = value = thick_row['value']
        if 'datatype' in thick_row:
            target = {'value': value, 'datatype': thick_row['datatype']}
        elif 'language' in thick_row:
            target = {'value': value, 'language': thick_row['language']}

        triples = [{'subject': create_node(_subject),
                    'predicate': create_node(_predicate),
                    'object': create_node(target)}]

        if 'annotations' in thick_row:
            triples += predicateMap2triples(decompress(thick_row, target, 'value', 'annotations'))

        if 'metadata' in thick_row:
            triples += predicateMap2triples(decompress(thick_row, target, 'value', 'metadata'))

        return triples

    if "object" in _thick_row:
        return obj2triples(_thick_row)
    elif 'value' in _thick_row:
        return val2triples(_thick_row)

def thicks2triples(thick_rows):
    triples = []
    for row in thick_rows:
        if "object" in row:
            o = row["object"]
            if isinstance(o, str) and o.startswith("{"):
                row["object"] = json.loads(o)
        triples += thick2triples(row['subject'], row['predicate'], row)
    return triples

owlTypes = ["owl:Restriction"]

def firstObject(predicates, predicate):
    """Given a prediate map, return the first 'object'."""
    if predicates.get(predicate):
        for obj in predicates[predicate]:
            if obj.get("object"):
                return obj["object"]
            elif obj.get('value'):
                return obj["value"]

    log("No object found")

def rdf2list(predicates):
    """Convert a nested RDF list to a simple list of objects.
    From
        {'rdf:type': [{'object': 'rdf:List'}],
         'rdf:first': [{'value': 'A'}],
         'rdf:rest': [{
             'object': {
                 'rdf:type': [{'object': 'rdf:List'}],
                 'rdf:first': [{'value': 'B'}],
                 'rdf:rest': [{'object': 'rdf:nil'}]}}]}}
    to
        [{"value": "A"}, {"value": "B"}]
    """
    result = []
    if "rdf:first" in predicates:
        result.append(predicates["rdf:first"][0])
    if "rdf:rest" in predicates:
        o = predicates["rdf:rest"][0]
        if not o:
            return result
        if not o.get("object"):
            return result
        if o["object"] == "rdf:nil":
            return result
        return result + rdf2list(o["object"])
    return result


def rdf2ofs(predicates):
    """Given a predicate map, try to return an OWL Functional S-Expression.
    From
        {'rdf:type': [{'object': 'owl:Restriction'}],
         'owl:onProperty': [{'object': 'ex:part-of'}],
         'owl:someValuesFrom': [{'object': 'ex:bar'}]}
    to
        ["ObjectSomeValuesFrom", "ex:part-of", "ex:bar"]
    """
    rdfType = firstObject(predicates, "rdf:type")
    result = None
    if rdfType == "owl:Restriction":
        onProperty = firstObject(predicates, "owl:onProperty")
        someValuesFrom = firstObject(predicates, "owl:someValuesFrom")
        result = ["ObjectSomeValuesFrom", onProperty, someValuesFrom]
    elif rdfType == "rdf:List":
        result = ["RDFList"] + rdf2list(predicates)
    # TODO: handle all the OFN types (See: https://www.w3.org/TR/2012/REC-owl2-mapping-to-rdf-20121211/)
    else:
        raise Exception(f"Unhandled type '{rdfType}' for: {predicates}")
    return result


def thick2reasoned(thick):
    """Convert logical thick rows to reasoned rows.
    From
        [{"subject": "ex:a", "predicate": "owl:equivalentClass", "object": "ex:b"}]
    to
        [{"super": "ex:a", "sub": "ex:b"}
         {"super": "ex:b", "sub": "ex:a"}]
    """
    reasoned = []
    for row in thick:
        owl = None
        if row["predicate"] in ["rdfs:subClassOf", "owl:equivalentClass"]:
            if row.get("object") and isinstance(row["object"], str):
                if row["object"].startswith("{"):
                    o = rdf2ofs(json.loads(row["object"]))
                else:
                    o = row["object"]
            result = {
                "super": o,
                "sub": row["subject"],
            }
            reasoned.append(result)
        if row["predicate"] in ["owl:equivalentClass"]:
            result = {
                "super": row["subject"],
                "sub": o,
            }
            reasoned.append(result)
    return reasoned


def quote(label):
    if re.search(r'\W', label):
        return f"'{label}'"
    return label


def ofs2omn(labels, ofs):
    """Convert OFS to Manchester (OMN) with labels.
    From
        ["ObjectSomeValuesFrom", "ex:part-of", "ex:bar"]
    to
        'part of' some Bar
    """
    first = ofs[0]
    if first == "ObjectSomeValuesFrom":
        onProperty = quote(labels.get(ofs[1], ofs[1]))
        someValuesFrom = quote(labels.get(ofs[2], ofs[2]))
        return f"{onProperty} some {someValuesFrom}"
    # TODO: handle all the OFN types
    else:
        raise Exception(f"Unhandled expression type '{first}' for: {ofs}")


def po2rdfa(labels, predicate, obj):
    if isinstance(obj, str):
        obj = {"object": obj}
    if obj.get("object"):
        o = obj["object"]
        if isinstance(o, str):
            if o.startswith("<"):
                o = o[1:-1]
            return [
              "a",
              {
                "href": o,
                "property": predicate,
              },
              labels.get(o, o),
            ]
        try:
            return ofs2rdfa(labels, rdf2ofs(o))
        except:
            return ["span", str(o)]
    elif obj.get("value"):
        return [
          "span",
          {"property": predicate},
          obj["value"],
        ]
    else:
        raise Exception(f"Unhandled object: {obj}")


def ofs2rdfa(labels, ofs):
    """Convert an OFS list to an HTML vector."""
    first = ofs[0]
    if first == "ObjectSomeValuesFrom":
        onProperty = po2rdfa(labels, "owl:onProperty", ofs[1])
        someValuesFrom = po2rdfa(labels, "owl:someValuesFrom", ofs[2])
        return ["span", onProperty, " some ", someValuesFrom]
    elif first == "RDFList":
        return ["span", "TODO " + str(ofs)]
    # TODO: handle all the OFN types
    else:
        raise Exception(f"Unhandled expression type '{first}' for: {ofs}")


def rows2labels(rows):
    """Given a list of rows, return a map from subject to rdfs:label value."""
    labels = {}
    for row in rows:
        if row["predicate"] == "rdfs:label":
            labels[row["subject"]] = row["value"]
    return labels


def subject2rdfa(labels, subject_id, predicates):
    """Convert a subject_id and predicate map to an HTML vector."""
    html = ["ul"]
    for predicate in sorted(list(predicates.keys())):
        for obj in predicates[predicate]:
            html.append(["li", po2rdfa(labels, predicate, obj)])
    return ["li", subject_id, html]


def subjects2rdfa(labels, subjects):
    """Convert a subject_id and subjects map to an HTML vector."""
    html = ["ul"]
    for subject_id in sorted(list(subjects.keys())):
        html.append(subject2rdfa(labels, subject_id, subjects[subject_id]))
    return html


if __name__ == "__main__":
    p = ArgumentParser("prototype.py", description="First pass at thick triples prototype")
    p.add_argument("-f", "--filter", nargs="+", default=[],
                   help="filter only on the given comma-separated list of stanzas")
    args = p.parse_args()

    rdfList = {'rdf:type': [{'object': 'rdf:List'}],
               'rdf:first': [{'value': 'A'}],
               'rdf:rest': [{'object': {'rdf:type': [{'object': 'rdf:List'}],
                                        'rdf:first': [{'value': 'B'}],
                                        'rdf:rest': [{'object': 'rdf:nil'}]}}]}
    log("List {}".format(rdf2ofs(rdfList)))

    with open(TSV) as fh:
        thin = list(csv.DictReader(fh, delimiter="\t"))
    if args.filter:
        pruned_thin = [row for row in thin if row['stanza'] in args.filter]
    else:
        pruned_thin = []

    if args.filter and not pruned_thin:
        print("WARNING No stanzas corresponding to {} in db".format(', '.join(args.filter)))
    thin = thin if not pruned_thin else pruned_thin

    ############################
    ####### Generate thick rows
    ############################
    with open("build/prefixes.n3", "w") as fh:
        for prefix in prefixes:
            print("@prefix {}: {} .".format(prefix, prefixes[prefix].strip('<>')), file=fh)

    subjects = thin2subjects(thin)
    with open("build/subjects.json", "w") as fh:
        print(pformat(subjects), file=fh)
    ##renderSubjects(subjects)

    thick = subjects2thick(subjects)
    with open("build/thick_rows.json", "w") as fh:
        [print(pformat(row), file=fh) for row in thick]

    ############################
    # Round-trip: go from thick rows to thin triples, build a graph, and then compare to the
    # original.
    ############################
    triples = thicks2triples(thick)
    with open("build/triples.json", "w") as fh:
        print(pformat(triples), file=fh)

    actual = Graph()
    [actual.add((triple['subject'], triple['predicate'], triple['object'])) for triple in triples]
    with open("build/triples.n3", "w") as fh:
        render_graph(actual, fh)

    expected = Graph()
    expected.parse(EXPECTED_OWL)

    with open("build/expected.ttl", "w") as fh:
        print(expected.serialize(format="n3").decode("utf-8"), file=fh)
    with open("build/actual.ttl", "w") as fh:
        print(actual.serialize(format="n3").decode("utf-8"), file=fh)

    print("Comparing graphs:")
    try:
        compare_graphs(actual, expected, True)
    except AssertionError as e:
        print("Graphs are not identical. Full dumps can be found in build/expected.ttl "
              "and build/expected.ttl")
    else:
        print("Graphs are identical")

    # Wait on this one for now ...
    #reasoned = thick2reasoned(thick)
    #ofs = reasoned[0]["super"]
    #labels = {
    #    "ex:part-of": "part of",
    #    "ex:bar": "Bar",
    #}
    #print("OFS {}".format(ofs))
    #print("OMN {}".format(ofs2omn(labels, ofs)))
    #rdfa = ofs2rdfa(labels, ofs)
    #print("RDFa {}".format(rdfa))
    #print("HTML {}".format(render(prefixes, rdfa)))
    #rdfa = subject2rdfa(labels, "ex:foo", subjects["ex:foo"])
    ##print("RDFa {}".format(rdfa))
    #print("HTML\n" + render(prefixes, rdfa))
