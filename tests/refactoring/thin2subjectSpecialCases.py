from copy import deepcopy
import translationUtil as tUtil
import sys

DEBUG=True
def log(message):
    if DEBUG:
        print(message, file=sys.stderr) 

#TODO: this one is a bit tricky
#An EquivalentClassesAxiom(CE1, ..., CEn)
#is encoded via the following triples:
#
#T(CE1) owl:equivalentClass T(CE2) .
#...
#T(CEn-1) owl:equivalentClass T(CEn) .
#
#So, we need to find all triples with 'owl:equivalentClass' 
#and work out chains

#_:x rdf:type owl:AllDisjointClasses .
#_:x owl:members T(SEQ CE1 ... CEn) . 
# ->
#{"_x": {"owl:AllDisjointClasses": [{"object": {"owl:members": { "rdf:first : ... 

def generateBlankNode(blankNodes):
    gen = "_:genid"
    genID = 1
    while gen + str(genID) in blankNodes:
        genID += 1
    freshBlankNode = gen + str(genID)
    blankNodes.add(freshBlankNode)
    return freshBlankNode

#NOTE: this way of building RDF lists has 'first' and 'rest' the other way around
#(this is not wrong - but it might look strange when printed because 'rest' is shown before 'first')
def createRDFList(arguments):
    rdfList = {}
    rdfList["object"] = "rdf:nil" 
    for a in arguments:
        if(rdfList.get("object") == "rdf:nil"):
            rdfList = {"rdf:rest" : [rdfList]}#rdf:nil is already wrapped with an 'object'
        else: 
            rdfList = {"rdf:rest" : [{"object": rdfList}]}
        rdfList["rdf:first"] =  [{"object": a}]
    return rdfList

def handleEquivalence(subjects):
    remove = set()
    subjects_copy = {}

    #initialise blank nodes
    #because we are going to introduce new blank nodes 
    #and need to make sure that we introduce fresh ones
    blankNodes = set() 
    for subject in sorted(subjects.keys()):
        if(tUtil.isBlankNode(subject)):
            blankNodes.add(subject)

    representative2equivalence = {}#the representative will be a fresh blank node
    class2representative = {}


    visited = set()
    for subject in sorted(subjects.keys()):
        if not subjects_copy.get(subject):
            subjects_copy[subject] = deepcopy(subjects[subject])

    for subject in sorted(subjects.keys()):
        toVisit = set()

        if(subjects_copy[subject].get("owl:equivalentClass")):
            equivalenceClass = set()#each subject will have its own equivalenceClass

            if(not subject in visited):#if this subject has not been handled
                toVisit.add(subject)

            while toVisit: 
                s = toVisit.pop()
                visited.add(s)#don't visit this subject again
                equivalenceClass.add(s)#create this subject's equivalence class
                if(subjects_copy[s].get("owl:equivalentClass")):
                    for v in subjects_copy[s]["owl:equivalentClass"]: 
                        if(v.get("object")):#get all stated equivalence classes
                            #if(not isinstance(v.get("object"), dict)):
                            namedClass = str(v.get("object"))
                            equivalenceClass.add(namedClass)
                            if((not namedClass in visited) and (not isinstance(v.get("object"),dict) )):
                                toVisit.add(namedClass)

            if(len(equivalenceClass) > 2): #these are equivalences
                blankNode = generateBlankNode(blankNodes)
                rdfList = createRDFList(equivalenceClass)
                #axiom = {}
                subjects_copy[blankNode] = {"owl:equivalentClass": [{"object" : rdfList}]}
                for e in equivalenceClass: 
                    #if(subjects_copy[e].get("owl:equivalentClass")):
                        remove.add(e)

    for t in remove:
        print("REMOVE")
        print(subjects_copy[t])
        del subjects_copy[t]
    return subjects_copy

                #toVisit.update(set(subjects_copy[subject]["owl:equivalentClass"]))
                #print(toVisit)


                #found = False
                ##check whether subject occurs anywhere
                #for rep, eqs in representative2equivalence.items():
                #    if subject in eqs:
                #        found = True
                #        #add all objects to the corresponding equivalence class
                #        for c in subjects_copy[subject]["owl:equivalentClass"]:
                #            eqs.add(c)

                #        break
                #if(not found): 
                #    rep = generateBlankNode(blankNodes)
                #    representative2equivalence[rep] = set()



            #for e in subjects_copy[subject].get("owl:equivalentClass"):
            #    print("LOOK")
            #    print(e)


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
                    #remove.add(subject)

                    #del subjects_copy[subject]["rdf:type"]
                    #del subjects_copy[subject]["owl:members"]

                    #adding "owl:members" somewhat unnecessary
                    #but this is how allDisjointClasses axioms are encoded
                    memberMap = {}
                    memberMap["owl:members"] = members
                    object = {}
                    object["object"] = memberMap
                    objectList = []
                    objectList.append(object)
                    allDisjointMap = {}
                    allDisjointMap["owl:AllDisjointClasses"] = objectList
                    subjects_copy[subject] = allDisjointMap 

    for t in remove:
        print("REMOVE")
        print(subjects_copy[t])
        del subjects_copy[t]

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
