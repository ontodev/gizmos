def firstObject(predicates, predicate):
    """Given a prediate map, return the first 'object'."""
    if predicates.get(predicate):
        for obj in predicates[predicate]:
            if obj.get("object"):
                return obj["object"]

def validObject(s,p,o):
    if o:
        return True
    log("Bad object: <{} {} {}>".format(s, p, o))
    return False

def isBlankNode(o):
    return o and isinstance(o, str) and o.startswith("_:")
