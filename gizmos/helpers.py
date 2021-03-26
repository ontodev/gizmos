import logging
import psycopg2
import re
import sqlite3

from configparser import ConfigParser
from rdflib import Graph


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


def escape(curie):
    """Escape illegal characters in the local ID portion of a CURIE"""
    prefix = curie.split(":")[0]
    local_id = curie.split(":")[1]
    local_id_fixed = re.sub(r"(?<!\\)([~!$&'()*+,;=/?#@%])", r"\\\1", local_id)
    return f"{prefix}:{local_id_fixed}"


def escape_qnames(cur, table):
    """Update CURIEs with illegal QName characters in the local ID by escaping those characters."""
    for keyword in ["stanza", "subject", "predicate", "object"]:
        cur.execute(
            f"""SELECT DISTINCT {keyword} FROM {table}
                WHERE {keyword} NOT LIKE '<%>' AND {keyword} NOT LIKE '_:%'"""
        )
        for row in cur.fetchall():
            curie = row[0]
            escaped = escape(curie)
            if curie != escaped:
                cur.execute(
                    f"UPDATE {table} SET {keyword} = '{escaped}' WHERE {keyword} = '{curie}'"
                )


def get_ancestors(cur, term_id):
    """Return a set of ancestors for a given term ID."""
    cur.execute(
        f"""WITH RECURSIVE ancestors(node) AS (
                VALUES ('{term_id}')
                UNION
                 SELECT object AS node
                FROM statements
                WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                  AND object = '{term_id}'
                UNION
                SELECT object AS node
                FROM statements, ancestors
                WHERE ancestors.node = statements.stanza
                  AND statements.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                  AND statements.object NOT LIKE '_:%'
            )
            SELECT * FROM ancestors""",
    )
    return set([x[0] for x in cur.fetchall()])


def get_children(cur, term_id):
    """Return a set of children for a given term ID."""
    cur.execute(
        f"""SELECT DISTINCT stanza FROM statements
            WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND object = '{term_id}'""",
    )
    return set([x[0] for x in cur.fetchall()])


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


def get_descendants(cur, term_id):
    """Return a set of descendants for a given term ID."""
    cur.execute(
        f"""WITH RECURSIVE descendants(node) AS (
                VALUES ('{term_id}')
                UNION
                 SELECT stanza AS node
                FROM statements
                WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                  AND stanza = '{term_id}'
                UNION
                SELECT stanza AS node
                FROM statements, descendants
                WHERE descendants.node = statements.object
                  AND statements.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
            )
            SELECT * FROM descendants""",
    )
    return set([x[0] for x in cur.fetchall()])


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


def get_parents(cur, term_id):
    """Return a set of parents for a given term ID."""
    cur.execute(
        f"""SELECT DISTINCT object FROM statements
            WHERE stanza = '{term_id}' AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
            AND object NOT LIKE '_:%'"""
    )
    return set([x[0] for x in cur.fetchall()])


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


def get_ttl(cur, table):
    """Get the given table as lines of Turtle (the lines are returned as a list)."""
    # Get ttl lines
    cur.execute(
        f"""WITH literal(value, escaped) AS (
              SELECT DISTINCT
                value,
                replace(replace(replace(value, '\\', '\\\\'), '"', '\\"'), '
            ', '\\n') AS escaped
              FROM {table}
            )
            SELECT
              '@prefix ' || prefix || ': <' || base || '> .'
            FROM prefix
            UNION ALL
            SELECT DISTINCT
               subject
            || ' '
            || predicate
            || ' '
            || coalesce(
                 object,
                 '"' || escaped || '"^^' || datatype,
                 '"' || escaped || '"@' || language,
                 '"' || escaped || '"'
               )
            || ' .'
            FROM {table} LEFT JOIN literal ON {table}.value = literal.value;"""
    )
    lines = []
    for row in cur.fetchall():
        line = row[0]
        if not line:
            continue
        # Replace newlines
        line = line.replace("\n", "\\n")
        lines.append(line)

    return "\n".join(lines)


def ttl_to_json(cur, ttl):
    # Create a Graph object from the TTL string
    graph = Graph()
    graph.parse(data=ttl, format="turtle")

    # Create the context with prefixes
    cur.execute("SELECT DISTINCT prefix, base FROM prefix;")
    context = {}
    for row in cur.fetchall():
        context[row[0]] = {"@id": row[1], "@type": "@id"}
    return graph.serialize(format="json-ld", context=context, indent=4).decode("utf-8")
