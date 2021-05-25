import json
import csv
import thin2subjects
from pprint import pformat

with open("testData/allClassExpressions.tsv") as fh:
    thin = list(csv.DictReader(fh, delimiter="\t"))

def translate(subjects):
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
                #if result.get("object") and not isinstance(result["object"], str):
                    #result["object"] = json.dumps(result["object"])
                #NOTE: subjects with multiple 'objects' are put into different rows
                #this means that 'objects' need to be self-contained
                #(this requires extra care for things like 'DisjointClasses' that 
                #are encoded using blank nodes as a *subject*
                rows.append(json.dumps(result))
    return rows

if __name__ == "__main__":

    subjects = thin2subjects.translate(thin)
    print("SUBJECTS:")
    print(pformat(subjects))
    #renderSubjects(subjects)
    print("#############################################")

    thick = translate(subjects)
    print("THICK ROWS:")
    [print(row) for row in thick]
    print("#############################################")
