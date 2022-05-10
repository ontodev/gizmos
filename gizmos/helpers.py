import logging
import os
import re

from configparser import ConfigParser
from rdflib import Graph
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Union

MAX_SQL_VARS = os.environ.get("MAX_SQL_VARS") or 999
TOP_LEVELS = {
    "ontology": "Ontology",
    "owl:Class": "Class",
    "owl:AnnotationProperty": "Annotation Property",
    "owl:DataProperty": "Data Property",
    "owl:ObjectProperty": "Object Property",
    "owl:Individual": "Individual",
    "rdfs:Datatype": "Datatype",
}


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


def get_children(conn: Connection, term_id: str, statements="statements"):
    query = sql_text(
        f"""SELECT DISTINCT subject FROM {statements}
            WHERE predicate IN ('rdfs:subClassOf', 'owl:subPropertyOf') AND object = :parent"""
    )
    results = conn.execute(query, parent=term_id)
    if term_id in TOP_LEVELS or term_id == "owl:Thing":
        # also get terms with no parent
        query = sql_text(
            f"""SELECT DISTINCT subject FROM {statements} 
            WHERE subject NOT IN 
                (SELECT subject FROM {statements}
                 WHERE predicate IN ('rdfs:subClassOf', 'owl:subPropertyOf')
                 AND object != 'owl:Thing')
            AND subject IN 
                (SELECT subject FROM {statements} 
                 WHERE predicate = 'rdf:type'
                 AND object = :term_id AND subject NOT LIKE '_:%%'
                 AND subject NOT IN ('owl:Thing', 'rdf:type'));"""
        )
        results = conn.execute(query, term_id=term_id)
    return [x["subject"] for x in results]


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


def get_descendants(conn: Connection, term_id: str, statements: str = "statements") -> set:
    """Return a set of descendants for a given term ID."""
    query = sql_text(
        f"""WITH RECURSIVE descendants(node) AS (
            VALUES (:term_id)
            UNION
             SELECT stanza AS node
            FROM {statements}
            WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND stanza = :term_id
            UNION
            SELECT stanza AS node
            FROM {statements}, descendants
            WHERE descendants.node = {statements}.object
              AND {statements}.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
        )
        SELECT * FROM descendants"""
    )
    results = conn.execute(query, term_id=term_id)
    return set([x[0] for x in results])


def get_entity_type(conn: Connection, term_id: str, statements="statements") -> str:
    """Get the OWL entity type for a term."""
    query = sql_text(
        f"""SELECT object FROM {statements} WHERE stanza = :term_id
            AND subject = :term_id AND predicate = 'rdf:type'"""
    )
    results = list(conn.execute(query, term_id=term_id))
    if len(results) > 1:
        for res in results:
            if res["object"] in TOP_LEVELS:
                return res["object"]
        return "owl:Individual"
    elif len(results) == 1:
        entity_type = results[0]["object"]
        if entity_type == "owl:NamedIndividual":
            entity_type = "owl:Individual"
        return entity_type
    else:
        entity_type = None
        query = sql_text(
            f"SELECT predicate FROM {statements} WHERE stanza = :term_id AND subject = :term_id"
        )
        results = conn.execute(query, term_id=term_id)
        preds = [row["predicate"] for row in results]
        if "rdfs:subClassOf" in preds:
            return "owl:Class"
        elif "rdfs:subPropertyOf" in preds:
            return "owl:AnnotationProperty"
        if not entity_type:
            query = sql_text(f"SELECT predicate FROM {statements} WHERE object = :term_id")
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                return "owl:Class"
            elif "rdfs:subPropertyOf" in preds:
                return "owl:AnnotationProperty"
    return "owl:Class"


def get_ids(
    conn: Connection, id_or_labels: list = None, id_type="subject", statements="statements"
) -> dict:
    """Get a list of IDs from a list of IDs or labels.

    :param conn: SQL database connection
    :param id_or_labels: list of ID or labels as strings
    :param id_type: the column to use as the target of the query (subject, predicate, or object)
                    based on the type being retrieved.
    :param statements: name of RDFTab statements table"""
    query = f'SELECT DISTINCT {id_type} FROM "{statements}"'
    term_labels = {}
    if id_or_labels:
        # Search for the given IDs
        query += f" WHERE {id_type} IN :id_or_labels"
        query = sql_text(query).bindparams(bindparam("id_or_labels", expanding=True))
        term_ids = [res[id_type] for res in conn.execute(query, id_or_labels=id_or_labels).fetchall()]
        # Remove found matches from the input set - any remaining were passed as labels
        id_or_labels = list(set(id_or_labels) - set(term_ids))
        if id_or_labels:
            # Get the IDs for terms passed as labels
            query = sql_text(
                f"""SELECT DISTINCT subject, value FROM "{statements}"
                WHERE predicate = 'rdfs:label' AND value IN :id_or_labels"""
            ).bindparams(bindparam("id_or_labels", expanding=True))
            term_labels = {res["subject"]: res["value"] for res in conn.execute(query, id_or_labels=id_or_labels).fetchall()}
            id_or_labels = list(set(id_or_labels) - set(term_labels.values()))
            if id_or_labels:
                # Some terms could not be found
                logging.warning(f"{len(id_or_labels)} terms do not exist in '{statements}': " + ", ".join(id_or_labels))
    else:
        # Get all terms of given id_type from table
        term_ids = [res[id_type] for res in conn.execute(sql_text(query)).fetchall()]
    if term_ids:
        # Get the labels of any terms passed as CURIEs
        query = sql_text(
            f"""SELECT DISTINCT subject, value FROM "{statements}"
            WHERE predicate = 'rdfs:label' AND subject IN :terms"""
        ).bindparams(bindparam("terms", expanding=True))
        term_labels.update({res["subject"]: res["value"] for res in conn.execute(query, terms=term_ids)})
        # Add the 'label' for remaining IDs - just the CURIE
        rem_ids = list(set(term_ids) - set(term_labels.keys()))
        term_labels.update({x: x for x in rem_ids})
    return term_labels


def get_parent_child_pairs(
    conn: Connection, term_id: str, statements="statements",
):
    query = sql_text(
        f"""WITH RECURSIVE ancestors(parent, child) AS (
        VALUES (:term_id, NULL)
        UNION
        -- The children of the given term:
        SELECT object AS parent, subject AS child
        FROM {statements}
        WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND object = :term_id
        UNION
        --- Children of the children of the given term
        SELECT object AS parent, subject AS child
        FROM {statements}
        WHERE object IN (SELECT subject FROM {statements}
                         WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                         AND object = :term_id)
          AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
        UNION
        -- The non-blank parents of all of the parent terms extracted so far:
        SELECT object AS parent, subject AS child
        FROM {statements}, ancestors
        WHERE ancestors.parent = {statements}.stanza
          AND {statements}.predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND {statements}.object NOT LIKE '_:%%'
      )
      SELECT * FROM ancestors"""
    )
    results = conn.execute(query, term_id=term_id).fetchall()
    return [[x["parent"], x["child"]] for x in results]


def get_parents(conn: Connection, term_id: str, statements: str = "statements") -> set:
    """Return a set of parents for a given term ID."""
    query = sql_text(
        f"""SELECT DISTINCT object FROM {statements} WHERE stanza = :term_id
            AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf') AND object NOT LIKE '_:%%'"""
    )
    results = conn.execute(query, term_id=term_id)
    return set([x["object"] for x in results])


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
