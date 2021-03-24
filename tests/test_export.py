from gizmos.export import export_terms
from util import test_db, create_db, compare_graphs


def get_diff(actual_lines, expected_lines):
    removed = list(set(expected_lines) - set(actual_lines))
    added = list(set(actual_lines) - set(expected_lines))
    removed = [f"---\t{x}" for x in removed if x != '']
    added = [f"+++\t{x}" for x in added if x != '']
    return removed + added


def test_export(create_db):
    tsv = export_terms(test_db, ["OBI:0100046"], ["CURIE", "label", "definition"], "tsv")
    actual_lines = tsv.split("\n")

    expected_lines = []
    with open("tests/resources/obi-export.tsv", "r") as f:
        for line in f:
            expected_lines.append(line.strip())

    diff = get_diff(actual_lines, expected_lines)
    if diff:
        print("The actual and expected outputs differ:")
        print()
        for line in diff:
            print(line)
    assert not diff


def test_export_no_predicates(create_db):
    tsv = export_terms(test_db, ["OBI:0100046"], None, "tsv", default_value_format="CURIE")
    actual_lines = tsv.split("\n")
    actual_lines = [x.strip() for x in actual_lines]

    expected_lines = []
    with open("tests/resources/obi-export-all.tsv", "r") as f:
        for line in f:
            expected_lines.append(line.strip())

    diff = get_diff(actual_lines, expected_lines)
    if diff:
        print("The actual and expected outputs differ:")
        print()
        for line in diff:
            print(line)
    assert not diff
