import csv
import json
import sys
from copy import deepcopy
from pprint import pformat

import thin2subjectSpecialCases as specialCase
import translationUtil as tUtil

with open("testData/allClassExpressions.tsv") as fh:
    thin = list(csv.DictReader(fh, delimiter="\t"))

DEBUG=True
def log(message):
    if DEBUG:
        print(message, file=sys.stderr) 


def row2objectMap(row):
    #TODO: doc string is confusing
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


def tripels2dictionary(thin):
    """Convert a list of thin rows to a nested subjects map (using blank nodes)
    from
        [{"subject": "ex:s", "predicate": "ex:p", "object": "ex:o"}]
    to
        {"ex:s": {"ex:p": [{"object": "ex:o"}]}}
    """ 
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
            #collect objects as an ordered list
            objects = predicates[predicate]   #get already collected objects
            objects.append(row2objectMap(row))#append newly found object
            objects.sort(key=lambda k: str(k))#sort them
            predicates[predicate] = objects   #update
        subjects[subject_id] = predicates

    return subjects

def blankNodeDependencies(thin):
    """Determines (direct) blank node dependencies in RDF triples (given as a 'row dictionary').

    Example input:
        ex:s         ex:p   ex:p
        ex:s         ex:p   _:blankNode1
        ex:s         ex:p   _:blankNode2
        _:blankNode1 ex:p   _:blankNode3

    Example output: 
        {"ex:s": {_:blankNode1, _:blankNode2}, _:blankNode1 : {_:blankNode3}}
    """ 

    dependencies = {}

    for row in thin:
        if row.get("object") and row["object"].startswith("_:"):
            subject = row["subject"]
            if not subject in dependencies:
                dependencies[subject] = set()
            dependencies[subject].add(row["object"])
    return dependencies

def getLeaves(subjects, dependencies): #blank nodes in subjects without dependencies
    """Determines leafs in blank node dependencies.

    Example input: 
        {"ex:s": {_:blankNode1, _:blankNode2}, _:blankNode1 : {_:blankNode3}}
    Example output:
        {_:blankNode1}
    """ 

    leafs = set()
    for subject in subjects:
        if(tUtil.isBlankNode(subject) and (not subject in dependencies)):
            leafs.add(subject)
    return leafs 

def updateDependencies(objectValue, dependencies):
    updates = [k for k,v in dependencies.items() if objectValue in v]
    for u in updates:
        dependencies[u].remove(objectValue)
        if not dependencies[u]:
            del dependencies[u] 

def resolveDependencies(subjects, dependencies):
    """Convert a nested subjects map using blank nodes (as objects)
    to a nested subject map  without blank nodes (as objects)
    """ 
    while dependencies: #these are direct dependencies

        leaves = getLeaves(subjects, dependencies) 
        handled = set()
        for subject, predicates in subjects.items():
            for predicate in predicates.keys():
                objects = []
                #we iterate over predicates[predicate] (I)
                #however, elements in predicates[predicates] are to be modified  (II)
                #so, all (possibly modified) elements are collected in 'objects' (III)
                #so that we can set predicates[predicates] = 'objects' later (IV)

                for object in predicates[predicate]:  #(I)

                    if(not tUtil.validObject(subject,predicate,object)):
                        continue

                    objectValue = object.get("object")
                    if tUtil.isBlankNode(objectValue): #note blank nodes are used as subjects in triples 
                        if objectValue in leaves: 
                            object = {"object": subjects[objectValue]} #replace blank node with the structure it describes (II)
                            handled.add(objectValue) #mark blank node as handled
                            updateDependencies(objectValue, dependencies) 

                    objects.append(object) #(III)
                objects.sort(key=lambda k: str(k))
                predicates[predicate] = objects #(IV)
        for subject in handled: #delete all handled blank nodes
            del subjects[subject]

def translate(thin):
    """Convert a list of thin rows to a nested subjects map:
    From
        [{"subject": "ex:s", "predicate": "ex:p", "object": "ex:o"}]
    to
        {"ex:s": {"ex:p": [{"object": "ex:o"}]}}
    """ 

    subjects = tripels2dictionary(thin) 
    dependencies = blankNodeDependencies(thin) 
    resolveDependencies(subjects, dependencies)

    subjects = specialCase.handleAllDisjointClasses(subjects)
    subjects = specialCase.handleEquivalence(subjects)
    subjects = specialCase.handleAnnotations(subjects)
    subjects = specialCase.handleReification(subjects) 

    return subjects


if __name__ == "__main__":
    log("THIN ROWS:")
    [log(row) for row in thin]
    print("DONE THIN ROWS")

    subjects = translate(thin)
    print("SUBJECTS:")
    print(pformat(subjects))
