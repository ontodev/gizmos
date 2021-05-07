#!/usr/bin/env python3

import csv
import json
import re
import sqlite3
import sys

from copy import deepcopy
from gizmos.hiccup import render
from pprint import pformat

DEBUG=True
def log(message):
    if DEBUG:
        print(message, file=sys.stderr)

prefixes = {}
with open("tests/prefix.tsv") as fh:
    rows = csv.DictReader(fh, delimiter="\t")
    for row in rows:
        if row.get("prefix"):
            prefixes[row["prefix"]] = row["base"]

with open("tests/thin.tsv") as fh:
#with open("obi-complete.tsv") as fh:
    thin = list(csv.DictReader(fh, delimiter="\t"))

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
        log("Invalid RDF row {}".format(row))
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
            log("LOOP!?")
            break
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
            log("OWL annotation: {}".format(subject_id))
            subject = firstObject(subjects_copy[subject_id], "owl:annotatedSource")
            predicate = firstObject(subjects_copy[subject_id], "owl:annotatedProperty")
            obj = firstObject(subjects_copy[subject_id], "owl:annotatedTarget")
            log("<{}, {}, {}>".format(subject, predicate, obj))

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
            log("RDF reification: {}".format(subject_id))
            subject = firstObject(subjects_copy[subject_id], "rdf:subject")
            predicate = firstObject(subjects_copy[subject_id], "rdf:predicate")
            obj = firstObject(subjects_copy[subject_id], "rdf:object")
            log("<{}, {}, {}>".format(subject, predicate, obj))

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

def triples2ttls(triples):
    ttls = []
    for triple in triples:
        first_part = "{} {} ".format(triple['subject'], triple['predicate'])
        if isinstance(triple['object'], str):
            ttls.append("{}{} .".format(first_part, triple['object']))
        else:
            # Look through triple['object'], and if the block is either a reification
            # or an annotation, switch the subject with the object being annotated/reified, otherwise
            # leave the subject as is:
            nested_target = None
            for item in triple['object']:
                if item['predicate'] in ['owl:annotatedTarget', 'rdf:object']:
                    nested_target = item['object']
                    break
                elif not nested_target:
                    nested_target = item['subject']

            ttls.append("{}{} .".format(first_part, nested_target))
            ttls += triples2ttls(triple['object'])
    return ttls

def thick2obj(thick_row):
    log("In thick2obj. Received thick_row: {}".format(thick_row))

    if 'object' not in thick_row and 'value' not in thick_row:
        raise Exception(f"Don't know how to handle thick_row without value or object: {thick_row}")

    def decompress_annotation(target, kind):
        if isinstance(target, str):
            target = {'owl:annotatedTarget': [{kind: target}]}
        target['owl:annotatedSource'] = [{'object': thick_row['subject']}]
        target['owl:annotatedProperty'] = [{'object': thick_row['predicate']}]
        target['rdf:type'] = [{'object': 'owl:Axiom'}]
        for key in thick_row['annotations']:
            target[key] = thick_row['annotations'][key]
        return target

    def decompress_reification(target, kind):
        if isinstance(target, str):
            target = {'rdf:object': [{kind: target}]}
        target['rdf:subject'] = [{'object': thick_row['subject']}]
        target['rdf:predicate'] = [{'object': thick_row['predicate']}]
        target['rdf:type'] = [{'object': 'rdf:Statement'}]
        for key in thick_row['annotations']:
            target[key] = thick_row['metadata'][key]
        return target

    if "object" in thick_row:
        target = thick_row['object']
        triples = []
        if 'annotations' not in thick_row and 'metadata' not in thick_row:
            if not isinstance(target, str):
                triples = predicateMap2triples(target)
        else:
            if 'annotations' in thick_row:
                triples += predicateMap2triples(decompress_annotation(target, 'object'))
            if 'metadata' in thick_row:
                triples += predicateMap2triples(decompress_reification(target, 'object'))
    elif 'value' in thick_row:
        target = thick_row['value']
        triples = []
        if 'datatype' in thick_row:
            target = '"{}"^^{}'.format(target, thick_row['datatype'])
        elif 'language' in thick_row:
            target = '"{}"@{}'.format(target, thick_row['language'])
        else:
            target = '{}'.format(target)

        if 'annotations' not in thick_row and 'metadata' not in thick_row:
            if not isinstance(target, str):
                triples = predicateMap2triples(target)
        else:
            if 'annotations' in thick_row:
                triples += predicateMap2triples(decompress_annotation(target, 'value'))
            if 'metadata' in thick_row:
                triples += predicat(decompress_reification(target, 'value'))

    return target if not triples else triples

b = 0
def predicateMap2triples(pred_map):
    global b
    b += 1
    log("In predicateMap2triples. Received: {}".format(pred_map))

    bnode = f"_:myb{b}"
    triples = []
    for predicate, objects in pred_map.items():
        for obj in objects:
            obj = thick2obj(obj)
            triples.append({'subject': bnode, 'predicate': predicate, 'object': obj})
    return triples

def thick2triples(thick_rows):
    log("In thick2triples. Received thick_rows: {}".format(thick_rows))
    triples = []
    for row in thick_rows:
        if "object" in row:
            o = row["object"]
            if isinstance(o, str) and o.startswith("{"):
                row["object"] = json.loads(o)

        obj = thick2obj(row)
        triples.append({'subject': row['subject'], 'predicate': row['predicate'], 'object': obj})
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
    rdfList = {'rdf:type': [{'object': 'rdf:List'}], 'rdf:first': [{'value': 'A'}], 'rdf:rest': [{'object': {'rdf:type': [{'object': 'rdf:List'}], 'rdf:first': [{'value': 'B'}], 'rdf:rest': [{'object': 'rdf:nil'}]}}]}
    log("List {}".format(rdf2ofs(rdfList)))

    log("THIN ROWS:")
    [log(row) for row in thin]

    subjects = thin2subjects(thin)
    print("SUBJECTS:")
    print(pformat(subjects))
    #renderSubjects(subjects)
    print("#############################################")

    thick = subjects2thick(subjects)
    print("THICK ROWS:")
    [print(row) for row in thick]
    print("#############################################")

    print("PREFIXES:")
    print(pformat(prefixes))
    print("#############################################")

    triples = thick2triples(thick)
    print("TRIPLES:")
    print(pformat(triples))
    print("#############################################")

    print("TERSE TRIPLES:")
    ttls = triples2ttls(triples)
    [print(ttl) for ttl in ttls]
    print("#############################################")

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
