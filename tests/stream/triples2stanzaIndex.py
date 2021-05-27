import csv
import os 
import json
import sys
from copy import deepcopy
from pprint import pformat

#TODO handle blank node subjects (for, e.g. disjoint classes axioms)

DEBUG=True
def log(message):
    if DEBUG:
        print(message, file=sys.stderr) 

with open("obi_core.nt") as fh:
    triples = list(csv.DictReader(fh, delimiter=" "))

def isBlankNode(o):
    return o and isinstance(o, str) and o.startswith("_:")


def triples2index(triples):
    """Converts a triple store into an index system where
    each subject is assigned a unique ID and
    triples with the same subject are stored in a file named after the corresponding ID."""

    path = os.getcwd() + "/index"
    os.mkdir(path) 

    #assign subjects an id
    id = 1
    subject2id = {}

    for t in triples:
        triple = t["subject"] + " " + t["predicate"] + " " + t["object"] + "\n"
        subject = t["subject"]

        #initialise
        if not subject in subject2id:
            subject2id[subject] = id
            f = open(path + "/" + str(id), "a")
            f.write("subject" + " " + "predicate" + " " + "object" + "\n")
            f.close() 
            id += 1 

        #update subject index
        subjectId = subject2id[subject]
        f = open(path + "/" + str(subjectId), "a")
        f.write(triple)
        f.close() 

    #store subject2id map
    f = open(path + "/subject2id", "a")
    f.write("subject" + " " + "id" + "\n")
    for key, value in subject2id.items():
        f.write(str(key) + " " + str(value) + "\n")
    f.close()

    return subject2id

def getSubject2indexMap(index):
    """Takes a path 'index' to indexed triple store and
    collectes all dependencies of indexed subjects in a 'dependency' file"""

    #get subject2id map from index 
    subject2id = {}
    with open(index + "/subject2id") as fh:
        subject2idFile = list(csv.DictReader(fh, delimiter=" "))
    for r in subject2idFile:
        subject = r["subject"]
        id = r["id"]
        subject2id[subject] = id

    return subject2id

def getDependencies(index, subject, subject2id):
    dependencies = set()
    toVisit = set()
    toVisit.add(subject2id[subject])

    while toVisit:
        id = toVisit.pop()
        with open(index + "/" + id) as fh:
            subjectTriples = list(csv.DictReader(fh, delimiter=" "))
            for t in subjectTriples:
                object = t["object"]
                if(isBlankNode(object)):
                    if(not object in dependencies):
                        toVisit.add(subject2id[object])
                    dependencies.add(subject2id[object])

    return dependencies 

def getSubject2type(index):
    subject2id = getSubject2indexMap(index)
    subject2types = {}
    for s, id in subject2id.items():
            with open(index + "/" + id) as fh:
                triples = list(csv.DictReader(fh, delimiter=" "))
                subject2types[s] = []
                for t in triples: 
                    if(t["predicate"] == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" or t["predicate"] == "rdf:type"):
                        subject2types[s].append(t["object"])


    return subject2types

def index2stanza(index): 
    """Get all dependency blank nodes and merge them into one file
    also attach a stanza which is the ID of a subject"""

    subject2id = getSubject2indexMap(index) 
    subject2types = getSubject2type(index)

    outputPath = index + "/stanza"
    os.mkdir(outputPath) 

    for s in subject2id:
        dependencies = getDependencies(path, s, subject2id)
        dependencies.add(subject2id[s])#add root subject 

        id = subject2id[s]
        stanzaTriples = set()
        for d in dependencies:
            with open(index + "/" + d) as fh:
                triples = list(csv.DictReader(fh, delimiter=" "))
                for t in triples: 
                    triple = id + " " +  t["subject"] + " " + t["predicate"] + " " + t["object"] + "\n"
                    stanzaTriples.add(triple)

                    #get types of 'leaf subjects' for this stanza
                    object = t["object"] 
                    if(object in subject2types):
                        for type in subject2types[object]:
                            typeTriple = id + " " +  object + " " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" + " " + type + "\n"
                            stanzaTriples.add(typeTriple) 

        #write stanza
        stanzaFile = open(outputPath + "/" + id , "a")
        stanzaFile.write("stanza subject predicate object\n") 
        for t in stanzaTriples:
            stanzaFile.write(t) 
        stanzaFile.close()

#TODO: include blank nodes that are associated with disjoint classes, etc..
def getRootSubjects(subject2id):
    for s, id in subject2id.items():
        if(not isBlankNode(s)):
            print(s + ":" + str(id))
        
if __name__ == "__main__":
    path = os.getcwd() + "/index"
    #triples2index(triples)

    #subject2id = getSubject2indexMap(path)
    #getRootSubjects(subject2id)

    index2stanza(path) 
