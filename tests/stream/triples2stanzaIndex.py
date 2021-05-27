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

#with open("obi_core.nt") as fh:
with open("obi.nt") as fh:
    triples = list(csv.DictReader(fh, delimiter=" "))

def isBlankNode(o):
    return o and isinstance(o, str) and o.startswith("_:")


#TODO there is a bug in here
def triples2index(triples, path):
    """Converts a triple store into an index system. 
    - Each subject is assigned a unique ID 
    - Triples with the same subject are stored in a file named after the corresponding ID
    - The file 'subject2id' specifies the mapping from subjects to the index id."""

    #path = os.getcwd() + "/index"
    path = path + "/index"
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
    """Takes a path 'index' to an indexed of triple store. 
    Returns a map as specified by the subject2id file of the index."""

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
    """Takes a path 'index' to an indexed of triple store and a subject.
    Returns all (transitive) blank node dependencies of the subject as a set.
    """
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
    """Takes a path 'index' to an indexed of triple store. 
    Returns a map from subjects to a list of their rdf:types.
    """
    subject2id = getSubject2indexMap(index)
    subject2types = {}
    for s, id in subject2id.items():
            with open(index + "/" + id) as fh:
                triples = list(csv.DictReader(fh, delimiter=" "))
                subject2types[s] = []
                for t in triples: 
                    #TODO: commit to abbreviated namespeces, i.e. 'rdf:type'?
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
        dependencies = getDependencies(index, s, subject2id)
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
def getRootSubjects(index):
    """Takes a path to an 'index' and returns a set of all root subjects.
    (A root subject is a subject that can be associated with an OWL axiom or the subject of a thick triple."""
    subject2id = getSubject2indexMap(index)
    rootSubjects = set()
    for s, id in subject2id.items():
        if(not isBlankNode(s)):
            rootSubjects.add(str(id))
        else: #check special cases
            with open(index + "/" + id) as fh:
                subjectTriples = list(csv.DictReader(fh, delimiter=" "))
                for t in subjectTriples:
                    predicate = t["predicate"]
                    object = t["object"]
                    #check disjoint classes case
                    if(predicate == "rdf:type" and object == "owl:AllDisjointClasses"):
                        rootSubjects.add(str(id)) 

    return rootSubjects
        
if __name__ == "__main__":
    path = os.getcwd()
    triples2index(triples, path)

    indexPath = os.getcwd() + "/index"
    index2stanza(indexPath) 

    rootSubjects = getRootSubjects(indexPath)
    with open(indexPath + "/rootSubjects", "a") as fh:
        for r in rootSubjects:
            fh.write(r + "\n")

    #subject2id = getSubject2indexMap(path)
    #getRootSubjects(subject2id)

