#!/usr/bin/env python3

import csv
import json
import re
import sqlite3
import sys

from argparse import ArgumentParser
from copy import deepcopy
from gizmos.hiccup import render
from pprint import pformat
from rdflib import Graph, BNode, URIRef, Literal

from util import compare_graphs

#TSV = "tests/thin.tsv"
#EXPECTED_OWL = 'example.rdf'
TSV = "build/obi_core.tsv"
EXPECTED_OWL = 'tests/resources/obi_core_no_trailing_ws.owl'

prefixes = {}
with open("tests/resources/prefix.tsv") as fh:
    rows = csv.DictReader(fh, delimiter="\t")
    for row in rows:
        if row.get("prefix"):
            prefixes[row["prefix"]] = row["base"]

LOG_LEVEL = "info"
def debug(message):
    if LOG_LEVEL in "debug":
        print(message, file=sys.stderr)

nesting = 0
def info(message):
    global nesting
    if LOG_LEVEL in ("debug", "info"):
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
    elif row.get("value"):
        if row.get("datatype"):
            return {"value": row["value"], "datatype": row["datatype"]}
        elif row.get("language"):
            return {"value": row["value"], "language": row["language"]}
        else:
            return {"value": row["value"]}
    else:
        debug("Invalid RDF row {}".format(row))
        #raise Exception("Invalid RDF row")


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
            debug("LOOP!?")
            # break
        last_leaves = len(leaves)
        dependencies = {}
        handled = set()
        for subject_id, predicates in subjects.items():
            for predicate in predicates.keys():
                objects = []
                for obj in predicates[predicate]:
                    if not obj:
                        debug("Bad object: <{} {} {}>".format(subject_id, predicate, obj))
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
            debug("OWL annotation: {}".format(subject_id))
            subject = firstObject(subjects_copy[subject_id], "owl:annotatedSource")
            predicate = firstObject(subjects_copy[subject_id], "owl:annotatedProperty")
            obj = firstObject(subjects_copy[subject_id], "owl:annotatedTarget")
            debug("<{}, {}, {}>".format(subject, predicate, obj))

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
                if o.get("object") == obj:
                    o["annotations"] = subjects_copy[subject_id]
                    remove.add(subject_id)
                objs_copy.append(o)
            subjects_copy[subject][predicate] = objs_copy

        if subjects_copy[subject_id].get("rdf:subject"):
            debug("RDF reification: {}".format(subject_id))
            subject = firstObject(subjects_copy[subject_id], "rdf:subject")
            predicate = firstObject(subjects_copy[subject_id], "rdf:predicate")
            obj = firstObject(subjects_copy[subject_id], "rdf:object")
            debug("<{}, {}, {}>".format(subject, predicate, obj))

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

def render_graph(graph, fh=sys.stdout):
    ttls = sorted([(s, p, o) for s, p, o in graph])
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
    if isinstance(content, str) and content.startswith('_:'):
        return BNode(content)
    elif isinstance(content, str) and content.startswith('<'):
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
            debug("WARNING: Could not create a node corresponding to content. Defaulting to Literal")
            return Literal(format(content))

def triples2graph(triples):
    graph = Graph()
    for triple in triples:
        subj = create_node(triple['subject'])
        pred = create_node(triple['predicate'])
        obj = triple['object']
        if isinstance(obj, str) or isinstance(obj, dict):
            graph.add((subj, pred, create_node(obj)))
        else:
            # Look through triple['object'], and if the block is either a reification
            # or an annotation, switch the subject with the object being annotated/reified, otherwise
            # leave the subject as is:
            nested_target = None
            for item in obj:
                if item['predicate'] in ['owl:annotatedTarget', 'rdf:object']:
                    nested_target = item['object']
                    break
                elif not nested_target:
                    nested_target = item['subject']

            graph.add((subj, pred, create_node(nested_target)))
            [graph.add((s, p, o)) for s, p, o in triples2graph(obj)]

    return graph

b_id = 0
def thick2triples(_subject, _predicate, _thick_row):
    global nesting
    nesting += 1
    info("Entering thick2triples")
    debug("In thick2triples. Received thick_row:\n{}".format(pformat(_thick_row)))

    if 'object' not in _thick_row and 'value' not in _thick_row:
        raise Exception(f"Don't know how to handle thick_row without value or object: {_thick_row}")

    def flatten(triples):
        global nesting
        nesting += 1
        #info("TRIPLES TO BE FLATTENED:\n{}".format(pformat(triples)))
        nesting -= 1
        return triples

    def predicateMap2triples(pred_map):
        global nesting
        nesting += 1
        info("Entering predicateMap2triples")
        global b_id
        b_id += 1
        debug("In predicateMap2triples. Received: {}".format(pred_map))

        info("Predicate Map is:\n{}".format(pformat(pred_map)))
    
        bnode = f"_:myb{b_id}"
        triples = []
        for predicate, objects in pred_map.items():
            for obj in objects:
                info("Processing object ({}):\n{}".format(predicate, pformat(obj)))
                triples += thick2triples(bnode, predicate, obj)
        info("TRIPLES\n{}".format(pformat(triples)))
        info("Exiting predicateMap2triples")
        nesting -= 1
        return triples

    def decompress(thick_row, target, target_type, decomp_type):
        global nesting
        nesting += 1
        info("Entering decompress ({})".format(decomp_type))

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

        if isinstance(target, str):
            annodata = {annodata_obj: [{target_type: target}]}
        else:
            annodata = {annodata_obj: [{target_type: predicateMap2triples(target)}]}

        annodata[annodata_subj] = [{'object': thick_row['subject']}]
        annodata[annodata_pred] = [{'object': thick_row['predicate']}]
        object_type = 'owl:Axiom' if decomp_type == 'annotations' else 'rdf:Statement'
        annodata['rdf:type'] = [{'object': object_type}]
        for key in thick_row[decomp_type]:
            annodata[key] = thick_row[decomp_type][key]
        info("ANNOTATIONS / METADATA:\n{}".format(pformat(annodata)))
        info("Exiting decompress_annotation")
        nesting -= 1
        return annodata

    def obj2triples(thick_row):
        # TODO: In the case of a structured target object, we want (always):
        # 1) A triple corresponding to the "no annotations or metadata" case below. The object of
        #    this triple is a blank node that is the subject of the further set of triples generated
        #    by predicateMap2triples(). I think this case is already implemented. It corresponds to
        #    line 19 in thins.txt and the further set of triples corresponds to lines 20--22.
        # 2) An independent set of triples corresponding to the annotations. ("Independent" here
        #    means that the subject of these triples (a blank node) is not the object of any other
        #    triple in the stanza.) We will generate this set of triples as follows:
        #    - For every key, object/value pair in the annotations map (part of a given thick
        #      row), we will create a triple whose predicate is that key and whose object is that
        #      object/value
        #    - A triple for annotatedSource
        #    - A triple for annotatedProperty
        #    - A triple for annotatedTarget. The object of this triple is a duplicate (with a
        #      different blank subject id) of the set of triples generated in 1).
        # 3) An independent set of triples corresponding to the metadata, generated using the same
        #    logic as in 2).
        global nesting
        global b_id
        nesting += 1
        info("Entering obj2triples")
        info("Got thick row:\n{}".format(pformat(thick_row)))
        target = thick_row['object']

        triples = []
        info("Generating the main set of triples ...")
        if isinstance(target, list):
            for t in target:
                triples += thick2triples(t['subject'], t['predicate'], t)
            # TODO: This is extremely hacky but it should work because of the order in which the ids
            # are generated here. See also the similar comment below. In that case ids are generated
            # in ascending order.
            next_id = b_id - 1
            #if _predicate in ['owl:annotatedTarget', 'rdf:object']:
            triples.append({'subject': _subject,
                            'predicate': _predicate,
                            'object': f"_:myb{next_id}"})
        elif not isinstance(target, str):
            # TODO: This is a hacky way of doing this, but the logic is right. We need to save
            # the b_id here because predicateMap2Triples is a recursive function and it will
            # increment the b_id every time it is called. What we need here is just whatever the
            # next id will be.
            next_id = b_id + 1
            triples += predicateMap2triples(target)
            triples.append({'subject': _subject, 'predicate': _predicate, 'object': f"_:myb{next_id}"})
        else:
            triples.append({'subject': _subject, 'predicate': _predicate, 'object': target})

        info("Triples are initially:\n{}".format(pformat(triples)))

        info("Looking for annotations ...")
        if 'annotations' in thick_row:
            annotations = decompress(thick_row, thick_row['object'], 'object', 'annotations')
            annotations = predicateMap2triples(annotations)
            triples += annotations

        info("Looking for metadata ...")
        if 'metadata' in thick_row:
            metadata = decompress(thick_row, thick_row['object'], 'object', 'metadata')
            metadata = predicateMap2triples(metadata)
            triples += metadata

        if 'metadata' in thick_row or 'annotations' in thick_row:
            info("Triples are now:\n{}".format(pformat(triples)))

        triples = flatten(triples)
        info("Exiting obj2triples")
        nesting -= 1
        return triples

    def val2triples(thick_row):
        global nesting
        nesting += 1
        info("Entering val2triples")
        target = value = thick_row['value']
        if 'datatype' in thick_row:
            target = {'value': value, 'datatype': thick_row['datatype']}
        elif 'language' in thick_row:
            target = {'value': value, 'language': thick_row['language']}

        triples = [{'subject': _subject, 'predicate': _predicate, 'object': target}]

        if 'annotations' in thick_row:
            annotations = decompress(thick_row, value, 'value', 'annotations')
            annotations = predicateMap2triples(annotations)
            triples += annotations

        if 'metadata' in thick_row:
            metadata = decompress(thick_row, value, 'value', 'metadata')
            metadata = predicateMap2triples(metadata)
            triples += metadata

        triples = flatten(triples)
        info("Exiting val2triples")
        nesting -= 1
        return triples

    if "object" in _thick_row:
        triples = obj2triples(_thick_row)
        triples = flatten(triples)
        info("Exiting thick2triples")
        nesting -= 1
        return triples
        #return obj2triples(_thick_row)
    elif 'value' in _thick_row:
        triples = val2triples(_thick_row)
        triples = flatten(triples)
        info("Exiting thick2triples")
        nesting -= 1
        return triples
        #return val2triples(_thick_row)

def thicks2triples(thick_rows):
    debug("In thicks2triples. Received thick_rows: {}".format(thick_rows))
    info("Entering thicks2triples")
    triples = []
    for row in thick_rows:
        if "object" in row:
            o = row["object"]
            if isinstance(o, str) and o.startswith("{"):
                row["object"] = json.loads(o)
        triples += thick2triples(row['subject'], row['predicate'], row)

    info("Exiting thicks2triples")
    return triples

owlTypes = ["owl:Restriction"]

def firstObject(predicates, predicate):
    """Given a prediate map, return the first 'object'."""
    if predicates.get(predicate):
        for obj in predicates[predicate]:
            if obj.get("object"):
                return obj["object"]


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
    debug("List {}".format(rdf2ofs(rdfList)))

    with open(TSV) as fh:
        thin = list(csv.DictReader(fh, delimiter="\t"))
    if args.filter:
        pruned_thin = [row for row in thin if row['stanza'] in args.filter]
    else:
        pruned_thin = []

    if not pruned_thin:
        print("WARNING No stanzas corresponding to {} in db".format(', '.join(args.filter)))
    thin = thin if not pruned_thin else pruned_thin

    debug("THIN ROWS:")
    [debug(pformat(row)) for row in thin]

    subjects = thin2subjects(thin)
    with open("build/subjects.json", "w") as fh:
        print(pformat(subjects), file=fh)
    ##renderSubjects(subjects)

    thick = subjects2thick(subjects)
    with open("build/thick_rows.json", "w") as fh:
        [print(pformat(row), file=fh) for row in thick]

    with open("build/prefixes.json", "w") as fh:
        print(pformat(prefixes), file=fh)

    triples = thicks2triples(thick)
    with open("build/triples.json", "w") as fh:
        print(pformat(triples), file=fh)

    actual = triples2graph(triples)
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
        print("Graphs are not identical")
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
