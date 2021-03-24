import logging
import psycopg2
import re
import sqlite3

from configparser import ConfigParser


def add_labels(cur):
    """Create a temporary labels table. If a term does not have a label, the label is the ID."""
    # Create a tmp labels table
    cur.execute("CREATE TABLE tmp_labels(term TEXT PRIMARY KEY, label TEXT)")
    if isinstance(cur, sqlite3.Cursor):
        # Add all terms with label
        cur.execute(
            """INSERT OR IGNORE INTO tmp_labels SELECT subject, value
               FROM statements WHERE predicate = 'rdfs:label'"""
        )
        # Update remaining with their ID as their label
        cur.execute(
            "INSERT OR IGNORE INTO tmp_labels SELECT DISTINCT subject, subject FROM statements"
        )
        cur.execute(
            "INSERT OR IGNORE INTO tmp_labels SELECT DISTINCT predicate, predicate FROM statements"
        )
    else:
        # Do the same for a psycopg2 Cursor
        cur.execute(
            """INSERT INTO tmp_labels
               SELECT subject, value FROM statements WHERE predicate = 'rdfs:label'
               ON CONFLICT (term) DO NOTHING"""
        )
        cur.execute(
            """INSERT INTO tmp_labels
               SELECT DISTINCT subject, subject FROM statements
               ON CONFLICT (term) DO NOTHING"""
        )
        cur.execute(
            """INSERT INTO tmp_labels
               SELECT DISTINCT predicate, predicate FROM statements
               ON CONFLICT (term) DO NOTHING"""
        )


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_connection(file):
    """Given a file ending in .db or .ini, create a database connection."""
    if file.endswith(".db"):
        # Always SQLite
        logging.info("Initializing SQLite connection")
        return sqlite3.connect(file)
    elif file.endswith(".ini"):
        # Always PostgreSQL (for now)
        config_parser = ConfigParser()
        config_parser.read(file)
        if config_parser.has_section("postgresql"):
            params = {}
            for param in config_parser.items("postgresql"):
                params[param[0]] = param[1]
        else:
            logging.error(
                "Unable to create database connection; missing [postgresql] section from " + file
            )
            return None
        logging.info("Initializing PostgreSQL connection")
        return psycopg2.connect(**params)
    logging.error(
        "Either a database file or a config file must be specified with a .db or .ini extension"
    )
    return None


def get_ids(cur, id_or_labels):
    """Create a list of IDs from a list of IDs or labels."""
    ids = []
    for id_or_label in id_or_labels:
        cur.execute(f"SELECT term FROM tmp_labels WHERE label = '{id_or_label}'")
        res = cur.fetchone()
        if res:
            ids.append(res[0])
        else:
            # Make sure this exists as an ID
            cur.execute(f"SELECT label FROM tmp_labels WHERE term = '{id_or_label}'")
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
