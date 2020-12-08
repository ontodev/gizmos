import logging
import re


def add_labels(cur):
    """Create a temporary labels table. If a term does not have a label, the label is the ID."""
    # Create a tmp labels table
    cur.execute("CREATE TABLE tmp.labels(term TEXT PRIMARY KEY, label TEXT)")

    # Add all terms with label
    cur.execute(
        """INSERT OR IGNORE INTO tmp.labels SELECT subject, value
           FROM statements WHERE predicate = 'rdfs:label'"""
    )
    # Update remaining with their ID as their label
    cur.execute("INSERT OR IGNORE INTO tmp.labels SELECT DISTINCT subject, subject FROM statements")
    cur.execute(
        "INSERT OR IGNORE INTO tmp.labels SELECT DISTINCT predicate, predicate FROM statements"
    )


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_ids(cur, id_or_labels):
    """Create a list of IDs from a list of IDs or labels."""
    ids = []
    for id_or_label in id_or_labels:
        cur.execute(f"SELECT term FROM labels WHERE label = '{id_or_label}'")
        res = cur.fetchone()
        if res:
            ids.append(res["term"])
        else:
            # Make sure this exists as an ID
            cur.execute(f"SELECT label FROM labels WHERE term = '{id_or_label}'")
            res = cur.fetchone()
            if res:
                ids.append(id_or_label)
            else:
                logging.warning(f" '{id_or_label}' does not exist in database")
    return ids


def get_terms(term_list, terms_file):
    """Get a list of terms from a list and/or a file from args."""
    terms = term_list or []
    if terms_file:
        with open(terms_file, "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                if not line.strip():
                    continue
                m = re.match(r"(.+)\s#.+", line)
                if m:
                    terms.append(m.group(1).strip())
                else:
                    terms.append(line.strip())
    return terms
