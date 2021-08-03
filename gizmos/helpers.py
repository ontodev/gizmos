import logging
import os
import re

from configparser import ConfigParser
from rdflib import Graph
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from typing import Union


def add_labels(conn: Connection):
    """Create a temporary labels table. If a term does not have a label, the label is the ID."""
    # Create a tmp labels table
    with conn.begin():
        conn.execute("CREATE TABLE tmp_labels(term TEXT PRIMARY KEY, label TEXT)")
        if str(conn.engine.url).startswith("sqlite"):
            # Add all terms with label
            conn.execute(
                """INSERT OR IGNORE INTO tmp_labels SELECT subject, value
                   FROM statements WHERE predicate = 'rdfs:label'"""
            )
            # Update remaining with their ID as their label
            conn.execute(
                "INSERT OR IGNORE INTO tmp_labels SELECT DISTINCT subject, subject FROM statements"
            )
            conn.execute(
                """INSERT OR IGNORE INTO tmp_labels
                   SELECT DISTINCT predicate, predicate FROM statements"""
            )
        else:
            # Do the same for a psycopg2 Cursor
            conn.execute(
                """INSERT INTO tmp_labels
                   SELECT subject, value FROM statements WHERE predicate = 'rdfs:label'
                   ON CONFLICT (term) DO NOTHING"""
            )
            conn.execute(
                """INSERT INTO tmp_labels
                   SELECT DISTINCT subject, subject FROM statements
                   ON CONFLICT (term) DO NOTHING"""
            )
            conn.execute(
                """INSERT INTO tmp_labels
                   SELECT DISTINCT predicate, predicate FROM statements
                   ON CONFLICT (term) DO NOTHING"""
            )


def escape(curie) -> str:
    """Escape illegal characters in the local ID portion of a CURIE"""
    prefix = curie.split(":")[0]
    local_id = curie.split(":")[1]
    local_id_fixed = re.sub(r"(?<!\\)([~!$&'()*+,;=/?#@%])", r"\\\1", local_id)
    return f"{prefix}:{local_id_fixed}"


def escape_qnames(conn: Connection, table: str):
    """Update CURIEs with illegal QName characters in the local ID by escaping those characters."""
    for keyword in ["stanza", "subject", "predicate", "object"]:
        results = conn.execute(
            f"""SELECT DISTINCT {keyword} FROM {table}
                WHERE {keyword} NOT LIKE '<%%>' AND {keyword} NOT LIKE '_:%%'"""
        )
        for res in results:
            curie = res[keyword]
            escaped = escape(curie)
            if curie != escaped:
                query = sql_text(
                    f"UPDATE {table} SET {keyword} = :escaped WHERE {keyword} = :curie"
                )
                conn.execute(query, escaped=escaped, curie=curie)


def get_connection(path: str) -> Union[Connection, None]:
    """"""
    if path.endswith(".db"):
        abspath = os.path.abspath(path)
        db_url = "sqlite:///" + abspath
        engine = create_engine(db_url)
        return engine.connect()
    elif path.endswith(".ini"):
        config_parser = ConfigParser()
        config_parser.read(path)
        if config_parser.has_section("postgresql"):
            params = {}
            for param in config_parser.items("postgresql"):
                params[param[0]] = param[1]
        else:
            logging.error(
                "Unable to create database connection; missing [postgresql] section from " + path
            )
            return None
        pg_user = params.get("user")
        if not pg_user:
            logging.error(
                "Unable to create database connection: missing 'user' parameter from " + path
            )
            return None
        pg_pw = params.get("password")
        if not pg_pw:
            logging.error(
                "Unable to create database connection: missing 'password' parameter from " + path
            )
            return None
        pg_db = params.get("database")
        if not pg_db:
            logging.error(
                "Unable to create database connection: missing 'database' parameter from " + path
            )
            return None
        pg_host = params.get("host", "127.0.0.1")
        pg_port = params.get("port", "5432")
        db_url = f"postgresql+psycopg2://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(db_url)
        return engine.connect()
    logging.error(
        "Either a database file or a config file must be specified with a .db or .ini extension"
    )
    return None


def get_ids(conn: Connection, id_or_labels: list) -> list:
    """Create a list of IDs from a list of IDs or labels."""
    ids = []
    for id_or_label in id_or_labels:
        query = sql_text("SELECT term FROM tmp_labels WHERE label = :id_or_label")
        res = conn.execute(query, id_or_label=id_or_label).fetchone()
        if res:
            ids.append(res["term"])
        else:
            # Make sure this exists as an ID
            query = sql_text("SELECT label FROM tmp_labels WHERE term = :id_or_label")
            res = conn.execute(query, id_or_label=id_or_label).fetchone()
            if res:
                ids.append(id_or_label)
            else:
                logging.warning(f" '{id_or_label}' does not exist in database")
    return ids


def get_terms(term_list: list, terms_file: str) -> list:
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


def get_ttl(conn: Connection, table: str) -> str:
    """Get the given table as lines of Turtle (the lines are returned as a list)."""
    # Get ttl lines
    results = conn.execute(
        f"""WITH literal(value, escaped) AS (
              SELECT DISTINCT
                value,
                replace(replace(replace(value, '\\', '\\\\'), '"', '\\"'), '
            ', '\\n') AS escaped
              FROM {table}
            )
            SELECT
              '@prefix ' || prefix || ': <' || base || '> .' AS line
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
    for res in results:
        line = res["line"]
        if not line:
            continue
        # Replace newlines
        line = line.replace("\n", "\\n")
        lines.append(line)

    return "\n".join(lines)


def ttl_to_json(conn: Connection, ttl: str) -> str:
    # Create a Graph object from the TTL string
    graph = Graph()
    graph.parse(data=ttl, format="turtle")

    # Create the context with prefixes
    results = conn.execute("SELECT DISTINCT prefix, base FROM prefix;")
    context = {}
    for res in results:
        context[res["prefix"]] = {"@id": res["base"], "@type": "@id"}
    return graph.serialize(format="json-ld", context=context, indent=4).decode("utf-8")
