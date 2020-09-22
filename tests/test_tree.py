import csv
import gizmos.tree
import html5lib
import os
import sqlite3
import sys

from argparse import ArgumentParser
from pyRdfa.parse import parse_one_node
from pyRdfa.state import ExecutionContext
from pyRdfa.options import Options
from rdflib import Graph
from rdflib.compare import to_isomorphic, graph_diff

RESPATH = "tests/resources"


def generate_insert_stmt(row):
    stan = row.get("stanza")
    if not stan:
        stan = "NULL"
    else:
        stan = '"{}"'.format(stan.replace("'", "''").replace('"', "'"))

    subj = row.get("subject")
    if not subj:
        subj = "NULL"
    else:
        subj = '"{}"'.format(subj.replace("'", "''").replace('"', "'"))

    pred = row.get("predicate")
    if not pred:
        pred = "NULL"
    else:
        pred = '"{}"'.format(pred.replace("'", "''").replace('"', "'"))

    obj = row.get("object")
    if not obj:
        obj = "NULL"
    else:
        obj = '"{}"'.format(obj.replace("'", "''").replace('"', "'"))

    val = row.get("value")
    if not val:
        val = "NULL"
    else:
        val = '"{}"'.format(val.replace("'", "''").replace('"', "'"))

    typ = row.get("type")
    if not typ:
        typ = "NULL"
    else:
        typ = '"{}"'.format(typ.replace("'", "''").replace('"', "'"))

    lang = row.get("lang")
    if not lang:
        lang = "NULL"
    else:
        lang = '"{}"'.format(lang.replace("'", "''").replace('"', "'"))

    return (
        "INSERT INTO statements "
        "(stanza, subject,predicate,object,value,datatype,language) "
        "VALUES ({},{},{},{},{},{},{});".format(stan, subj, pred, obj, val, typ, lang)
    )


def generate_insert_prefix(row):
    prefix = row.get("prefix")
    prefix = "NULL" if not prefix else prefix
    prefix = '"{}"'.format(prefix.replace("'", "''").replace('"', "'"))

    base = row.get("base")
    base = "NULL" if not base else base
    base = '"{}"'.format(base.replace("'", "''").replace('"', "'"))

    return "INSERT INTO prefix (prefix, base) VALUES ({},{});".format(prefix, base)


def create_db(db):
    with open(f"{RESPATH}/prefix.tsv") as pfile, open(f"{RESPATH}/statements.tsv") as sfile:
        prefix_rows = csv.DictReader(pfile, delimiter="\t")
        statement_rows = csv.DictReader(sfile, delimiter="\t")
        with sqlite3.connect(db) as conn:
            cur = conn.cursor()

            cur.executescript("DROP TABLE IF EXISTS prefix;")
            cur.executescript(
                "CREATE TABLE prefix (" "  prefix TEXT PRIMARY KEY," "  base TEXT NOT NULL" ");"
            )

            cur.executescript("DROP TABLE IF EXISTS statements;")
            cur.executescript(
                "CREATE TABLE statements ("
                "  stanza TEXT,"
                "  subject TEXT,"
                "  predicate TEXT,"
                "  object TEXT,"
                "  value TEXT,"
                "  datatype TEXT,"
                "  language TEXT"
                ");"
            )

            for row in prefix_rows:
                insert_prefix = generate_insert_prefix(row)
                cur.executescript(insert_prefix)

            for row in statement_rows:
                insert_stmt = generate_insert_stmt(row)
                cur.executescript(insert_stmt)


def test_tree():
    parser = ArgumentParser()
    parser.add_argument("-g", "--generate", action="store_true", help="Generate new reference file")
    parser.add_argument(
        "-t", "--term", metavar="TERM_ID", required=True, help="Run the unit test on the given term"
    )
    args = parser.parse_args()

    if args.term not in ("OBI:0000666", "OBI:0000793", "OBI:0100046"):
        print(
            "The test db currently only contains: 'OBI:0000666', 'OBI:0000793', 'OBI:0100046'",
            file=sys.stderr,
        )
        sys.exit(1)

    db = f"{RESPATH}/obi.db"
    print(f"Creating {db} ...", file=sys.stderr)
    termid = args.term
    create_db(db)

    print(f"Generating RDFa for {termid} ...", file=sys.stderr)
    treename = os.path.splitext(os.path.basename(db))[0]
    with sqlite3.connect(db) as conn:
        conn.row_factory = gizmos.tree.dict_factory
        cur = conn.cursor()
        html = gizmos.tree.terms2rdfa(cur, treename, [termid])

    # Create the DOM document element
    parser = html5lib.HTMLParser(tree=html5lib.treebuilders.getTreeBuilder("dom"))
    dom = parser.parse(html)

    # get the DOM tree
    top = dom.documentElement

    # Create the initial state (from pyRdfa)
    graph = Graph()
    options = Options(
        output_default_graph=True,
        output_processor_graph=True,
        space_preserve=True,
        transformers=[],
        embedded_rdf=True,
        vocab_expansion=False,
        vocab_cache=True,
        vocab_cache_report=False,
        refresh_vocab_cache=False,
        check_lite=False,
        experimental_features=True,
    )
    state = ExecutionContext(
        top, graph, base="http://purl.obolibrary.org/obo/", options=options, rdfa_version="1.1",
    )

    # Add the RDFa to the RDFLib graph (recursive)
    parse_one_node(top, graph, None, state, [])

    def dump_ttl_sorted(graph):
        for line in sorted(graph.serialize(format="ttl").splitlines()):
            if line:
                print(line.decode("ascii"))

    reference = f"{RESPATH}/obi-tree-{termid}.ttl"
    if args.generate:
        print(f"Generating new reference file: {reference} ...", file=sys.stderr)
        graph.serialize(format="ttl", destination=reference)
    else:
        print(
            f"Comparing generated tree with known good reference: {reference} ...", file=sys.stderr
        )
        expected_graph = Graph()
        expected_graph.parse(reference, format="ttl")

        graph_iso = to_isomorphic(graph)
        expected_graph_iso = to_isomorphic(expected_graph)
        if graph_iso == expected_graph_iso:
            print("Success", file=sys.stderr)
            sys.exit(0)
        else:
            _, in_first, in_second = graph_diff(graph_iso, expected_graph_iso)
            print("The expected and generated graphs differ.", file=sys.stderr)
            print("----- Contents of generated graph not in expected graph -----", file=sys.stderr)
            dump_ttl_sorted(in_first)
            print("----- Contents of expected graph not in generated graph -----", file=sys.stderr)
            dump_ttl_sorted(in_second)
            sys.exit(1)


if __name__ == "__main__":
    test_tree()
