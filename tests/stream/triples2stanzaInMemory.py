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
with open("obiTab.nt") as fh:
    #NOTE: using ¬ as a quotechar is a workaround!
    #The default quote char is a double quote.
    #This leads the DictReader to read triples incorrectly
    #if a triple does not use matching quotes.
    #For example:
    #A B "C
    #C D E"
    #will be parsed as ONE triple: A B "C D E"
    #setting the quote char to a char that does not occur in the file solves this issue
    #it would be nice to set 'quotechar=None' (if you know how to do this - please let me know)
    triples = list(csv.DictReader(fh, delimiter="\t", quotechar="¬"))
    #triples = list(csv.DictReader(fh, delimiter="\t"))

def isBlankNode(o):
    return o and isinstance(o, str) and o.startswith("_:")

def getSubject2triples(triples):
    """Returns a map from subjects 's' to a list of all triples
    of the form 's p o', i.e., with 's' as a subject."""

    subject2triples = {}

    for t in triples:
        subject = t["subject"]

        #initialise
        if not subject in subject2triples:
            subject2triples[subject] = []

        subject2triples[subject].append(t) 

    return subject2triples

def getDependencies(subject, subject2triples):
    """Returns all (transitive) blank node dependencies of the subject as a set."""
    dependencies = set()
    toVisit = set()
    toVisit.add(subject)

    while toVisit:
        s = toVisit.pop()
        for t in subject2triples[s]:
            object = t["object"]
            if(isBlankNode(object)):
                if(not object in dependencies):
                    toVisit.add(object)
                dependencies.add(object)

    return dependencies 

def getSubject2type(triples):
    """Returns a map from subjects to a list of their rdf:types."""
    subject2types = {}
    for t in triples: 
        s = t["subject"] 
        if(t["predicate"] == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" or t["predicate"] == "rdf:type"):
            if not s in subject2types: 
                subject2types[s] = []
            subject2types[s].append(t["object"]) 

    return subject2types

def getSubject2stanza(subject2triples, subject2types): 
    """Get all dependency blank nodes and merge them into one file
    also attach a stanza which is the ID of a subject"""

    subject2stanza={}

    for s in subject2triples:
        dependencies = getDependencies(s, subject2triples)

        stanzaTriples = []
        for t in subject2triples[s]:
            stanzaTriples.append(t)

        for d in dependencies: 
            for t in subject2triples[d]: 
                
                stanzaTriples.append(t)#add dependency triple

                #get types of 'leaf subjects' for this stanza
                object = t["object"] 
                if(object in subject2types):
                    for type in subject2types[object]:
                        typeTriple = object + "\t" + "rdf:type" + "\t" + type 
                        stanzaTriples.append(typeTriple) 
        subject2stanza[s] = stanzaTriples

    return subject2stanza

#TODO: include blank nodes that are associated with disjoint classes, etc..
def getRootSubjects(subject2triples):
    """Takes a map from subject to its corresponding triples and returns a set of all root subjects.
    (A root subject is a subject that can be associated with an OWL axiom or the subject of a thick triple.)"""
    rootSubjects = set()
    for s in subject2triples:
        if(not isBlankNode(s)):
            rootSubjects.add(s)
        else: #check special cases
            for t in subject2triples[s]:
                predicate = t["predicate"]
                object = t["object"]
                #check disjoint classes case
                if(predicate == "rdf:type" and object == "owl:AllDisjointClasses"):
                    rootSubjects.add(s) 

    return rootSubjects
        
if __name__ == "__main__":

    subject2triples = getSubject2triples(triples) 
    subject2types = getSubject2type(triples) 
    subject2stanza = getSubject2stanza(subject2triples, subject2types) 
    #count = 10
    #i = 0
    #for s in subject2stanza:
    #    i += 1
    #    if(i > count):
    #        break

    #    print("Subject is")
    #    print(s)
    #    for t in subject2stanza[s]:
    #        print(t) 

    #rootSubjects = getRootSubjects(subject2triples)

