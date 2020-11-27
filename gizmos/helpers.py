import logging

# Misc. utility methods for gizmos


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_terms(term_list, terms_file):
    """Get a list of terms from a list and/or a file from args."""
    terms = term_list or []
    if terms_file:
        with open(terms_file, "r") as f:
            terms_from_file = [x.strip() for x in f.readlines()]
            terms.extend(terms_from_file)
    return terms
