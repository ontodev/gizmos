import sys

from copy import deepcopy
import translationUtil as tUtil

DEBUG = True


def log(message):
    if DEBUG:
        print(message, file=sys.stderr)


def handleAllDisjointClasses(subjects):
    remove = set()
    subjects_copy = {}
    for subject in sorted(subjects.keys()):

        if not subjects_copy.get(subject):
            subjects_copy[subject] = deepcopy(subjects[subject])

        if(subject.startswith("_")):
            if(subjects_copy[subject].get("rdf:type")):
                if(tUtil.firstObject(subjects_copy[subject], "rdf:type") == "owl:AllDisjointClasses"):
                    members = tUtil.firstObject(subjects_copy[subject],"owl:members")

                    del subjects_copy[subject]["rdf:type"]
                    del subjects_copy[subject]["owl:members"]
                    #del subjects_copy[subject]
                    memberMap = {}
                    memberMap["owl:members"] = members
                    object = {}
                    object["object"] = memberMap
                    objectList = []
                    objectList.append(object)
                    allDisjointMap = {}
                    allDisjointMap["owl:AllDisjointClasses"] = objectList
                    subjects_copy[subject] = allDisjointMap 

    return subjects_copy

def handleAnnotations(subjects):
    remove = set()
    subjects_copy = {}
    for subject_id in sorted(subjects.keys()):

        if not subjects_copy.get(subject_id):
            subjects_copy[subject_id] = deepcopy(subjects[subject_id]) 

        if subjects_copy[subject_id].get("owl:annotatedSource"):
            log("OWL annotation: {}".format(subject_id))
            subject = tUtil.firstObject(subjects_copy[subject_id], "owl:annotatedSource")
            predicate = tUtil.firstObject(subjects_copy[subject_id], "owl:annotatedProperty")
            obj = tUtil.firstObject(subjects_copy[subject_id], "owl:annotatedTarget")
            log("<{}, {}, {}>".format(subject, predicate, obj))

            print("look")
            print(subjects_copy[subject_id]["owl:annotatedSource"])

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

    for t in remove:
        print("REMOVE")
        print(subjects_copy[t])
        del subjects_copy[t]

    return subjects_copy 

def handleReification(subjects):
    remove = set()
    subjects_copy = {}
    for subject_id in sorted(subjects.keys()):

        if not subjects_copy.get(subject_id):
            subjects_copy[subject_id] = deepcopy(subjects[subject_id]) 

        if subjects_copy[subject_id].get("rdf:subject"):
            log("RDF reification: {}".format(subject_id))
            subject = tUtil.firstObject(subjects_copy[subject_id], "rdf:subject")
            predicate = tUtil.firstObject(subjects_copy[subject_id], "rdf:predicate")
            obj = tUtil.firstObject(subjects_copy[subject_id], "rdf:object")
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
