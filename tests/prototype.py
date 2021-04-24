#!/usr/bin/env python3

import csv
import json
import re
import sqlite3

from copy import deepcopy
from gizmos.hiccup import render
from pprint import pprint


prefixes = {}
with open("tests/prefix.tsv") as fh:
    rows = csv.DictReader(fh, delimiter="\t")
    for row in rows:
        if row.get("prefix"):
            prefixes[row["prefix"]] = row["base"]

with open("tests/thin.tsv") as fh:
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
        print("Invalid RDF row", row)
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
            print("LOOP!?")
            break
        last_leaves = len(leaves)
        dependencies = {}
        handled = set()
        for subject_id, predicates in subjects.items():
            for predicate in predicates.keys():
                objects = []
                for obj in predicates[predicate]:
                    if not obj:
                        print("Bad object", subject_id, predicate, obj)
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
    subjects_copy = deepcopy(subjects)
    for subject_id in sorted(subjects.keys()):
        if subjects[subject_id].get("owl:annotatedSource"):
            #print("OWL annotation", subject_id)
            subject = firstObject(subjects[subject_id], "owl:annotatedSource")
            predicate = firstObject(subjects[subject_id], "owl:annotatedProperty")
            obj = subjects[subject_id]["owl:annotatedTarget"][0]
            print("<{}, {}, {}>".format(subject, predicate, obj))
            del subjects_copy[subject_id]["owl:annotatedSource"]
            del subjects_copy[subject_id]["owl:annotatedProperty"]
            del subjects_copy[subject_id]["owl:annotatedTarget"]
            del subjects_copy[subject_id]["rdf:type"]
            objs = subjects[subject][predicate]
            objs_copy = []
            for o in objs:
                o = deepcopy(o)
                if o == obj:
                    o["annotations"] = subjects_copy[subject_id]
                    remove.add(subject_id)
                objs_copy.append(o)
            subjects_copy[subject][predicate] = objs_copy
        if subjects[subject_id].get("rdf:subject"):
            print("RDF reification", subject_id)
            # The rest is similar to the OWL annotation case above, except that we use
            # rdf:subject, rdf:predicate, and rdf:object instead of owl:annotatedSource,
            # owl:annotatedProperty, and owl:annotatedTarget.
            subject = firstObject(subjects[subject_id], "rdf:subject")
            predicate = firstObject(subjects[subject_id], "rdf:predicate")
            obj = subjects[subject_id]["rdf:object"][0]
            print("<{}, {}, {}>".format(subject, predicate, obj))
            del subjects_copy[subject_id]["rdf:subject"]
            del subjects_copy[subject_id]["rdf:predicate"]
            del subjects_copy[subject_id]["rdf:object"]
            del subjects_copy[subject_id]["rdf:type"]
            objs = subjects[subject][predicate]
            objs_copy = []
            for o in objs:
                o = deepcopy(o)
                if o == obj:
                    o["annotations"] = subjects_copy[subject_id]
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
                #print("OBJ: {}".format(obj))
                result = {
                    "subject": subject_id,
                    "predicate": predicate,
                    **obj
                }
                #print("RESULT: {}".format(result))
                if result.get("object") and not isinstance(result["object"], str):
                    result["object"] = json.dumps(result["object"])
                rows.append(result)
    return rows


def thick2subjects(thick):
    pass


### thick to Turtle

def thick2obj(thick_row):
    if "object" in thick_row:
        if isinstance(thick_row["object"], str):
            return thick_row["object"]
        else:
            return predicateMap2ttls(thick_row["object"])
    if "value" in thick_row:
        # TODO: datatypes and languages
        return '"' + thick_row["value"] + '"'
    else:
        raise Exception(f"Don't know how to handle thick_row: {thick_row}")

b = 0
def predicateMap2ttls(pred_map):
    global b
    b += 1

    bnode = f"_:myb{b}"
    ttls = []
    for predicate, objects in pred_map.items():
        for obj in objects:
            obj = thick2obj(obj)
            ttls.append({'subject': bnode, 'predicate': predicate, 'object': obj})
    return ttls

def thick2ttl(thick_rows):
    triples = []
    for row in thick_rows:
        if "object" in row:
            o = row["object"]
            if isinstance(o, str) and o.startswith("{"):
                row["object"] = json.loads(o)

        obj = thick2obj(row)
        triples.append({'subject': row['subject'], 'predicate': row['predicate'], 'object': obj})
        # TODO: OWL Annotations
        # TODO: RDF Reification
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
    print("List", rdf2ofs(rdfList))

    subjects = thin2subjects(thin)
    pprint(subjects)
    #renderSubjects(subjects)
    print("#############################################")

    thick = subjects2thick(subjects)
    for row in thick:
        print("ROW: {}".format(row))
    print("#############################################")

    print("Prefixes:")
    pprint(prefixes)
    print("#############################################")

    triples = thick2ttl(thick)
    def render_triples(triples):
        for ttl in triples:
            print("{} {} ".format(ttl['subject'], ttl['predicate']), end="")
            if isinstance(ttl['object'], str):
                print("{} .".format(ttl['object']))
            else:
                nested_subject = [item['subject'] for item in ttl['object']].pop()
                print("{} .".format(nested_subject))
                render_triples(ttl['object'])

    render_triples(triples)
    print("#############################################")

    # Wait on this one for now ...
    #reasoned = thick2reasoned(thick)
    #ofs = reasoned[0]["super"]
    #labels = {
    #    "ex:part-of": "part of",
    #    "ex:bar": "Bar",
    #}
    #print("OFS", ofs)
    #print("OMN", ofs2omn(labels, ofs))
    #rdfa = ofs2rdfa(labels, ofs)
    #print("RDFa", rdfa)
    #print("HTML", render(prefixes, rdfa))
    #rdfa = subject2rdfa(labels, "ex:foo", subjects["ex:foo"])
    ##print("RDFa", rdfa)
    #print("HTML\n" + render(prefixes, rdfa))
