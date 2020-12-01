# gizmos
Utilities for ontology development

### Testing

For development, we recommend installing and testing using:
```
python3 -m pip install -e .
python3 setup.py pytest
```

There are some dependencies that are test-only (e.g., will not be listed in the project requirements). If you try and run `pytest` alone, it may fail due to missing dependencies.

## Modules

Each `gizmos` module uses a SQL database version of an RDF or OWL ontology to create outputs. All SQL database inputs should be created from OWL using [rdftab](https://github.com/ontodev/rdftab.rs) to ensure they are in the right format. The database is specified by `-d`/`--database`.

### `gizmos.export`

The `export` module creates a table (default TSV) output containing the terms and their predicates written to stdout.
```
python3 -m gizmos.export -d [path-to-database] -t [term] > [output-tsv]
```

The `term` should be the CURIE or label of your desired term, and you can include more than one `-t` option. Mulitple terms can also be specified with `-T <file>`/`--terms <file>` with each CURIE or label on one line.

You can specify a format other than TSV by using the `-f <format>`/`--format <format>` option. The following formats are supported:
* TSV
* CSV
* HTML \*

\* This is full HTML page. If you just want the content without `<html>` and `<body>` tags, include `-c`/`--content-only`.

By default, headers are included. The headers are the predicate labels. If you wish to not include headers, include `-n`/`--no-headers`.

You can also specify the subset of predicates you wish to include using the `-p <term>`/`--predicate <term>` option. The `term` should be the term CURIE or the term label. Whatever you input will be used as the header for that column. The values in the column will be string values (for literal annotations) or IRIs (for objects and IRI annotations). If you want to use CURIEs instead of full IRIs, include `-V CURIE`/`--values CURIE` or, for labels, `-V label`/`--values label`.

For more fine grained control of how objects are output, you can include value formats in the predicate label as such: `label [format]` (e.g., `rdfs:subClassOf [CURIE]`). The following formats are supported:
* `label`: label when available, or the CURIE otherwise
* `CURIE`: the CURIE
* `IRI`: the full IRI

Any time the predicate doesn't have a value format, the value format will be the `-V` value format (IRI when not included). Note that the value formats above can also be used in `-p` and `-P`.

If an ontology term has more than one value for a given predicate, it will be returned as a pipe-separated list. You can specify a different character to split multiple values on with `-s <char>`/`--split <char>`, for example `-s ", "` for a comma-separated list.

If you have many predicates to include, you can use `-P <file>`/`--predicates <file>` for a list of predicates (CURIE or label), each on one line.

### `gizmos.extract`

The `extract` module creates a TTL or JSON-LD file containing the term, predicates, and ancestors written to stdout.
```
python3 extract.py -d [path-to-database] -t [term] > [output-ttl]
```

For JSON-LD, you must include `-f JSON-LD`/`--format JSON-LD`.

The term or terms as CURIEs or labels are specified with `-t <term>`/`--term <term>`. You may also specify multiple terms to extract with `-T <file>`/`--terms <file>` where the file contains a list of CURIEs to extract.

The output contains the specified term and all its ancestors up to `owl:Thing`. If you don't wish to include the ancestors of the term/terms, include the `-n`/`--no-hierarchy` flag.

You may also specify which predicates you would like to include with `-p <term>`/`--predicate <term>` or `-P <file>`/`--predicates <file>`, where the file contains a list of predicate CURIEs or labels. Otherwise, the output includes all predicates. Since this extracts a hierarchy, unless you include the `-n` flag, `rdfs:subClassOf` will always be included.

### `gizmos.tree`

The `tree` module produces a CGI tree browser for a given term contained in a SQL database.

Usage in the command line:
```
python3 -m gizmos.tree [path-to-database] [term] > [output-html]
```

The `term` should be a CURIE with a prefix already defined in the `prefix` table of the database. If the `term` is not included, the output will show a tree starting at `owl:Thing`.

This can be useful when writing scripts that return trees from different databases.

If you provide the `-s`/`--include-search` flag, a search bar will be included in the page. This search bar uses [typeahead.js](https://twitter.github.io/typeahead.js/) and expects the output of `gizmos.search`. The URL for the fetching the data for [Bloodhound](https://github.com/twitter/typeahead.js/blob/master/doc/bloodhound.md) is `?text=[search-text]&format=json`, or `?db=[db]&text=[search-text]&format=json` if the `-d` flag is also provided. The `format=json` is provided as a flag for use in scripts. See the CGI Example below for details on implementation.

The title displayed in the HTML output is the database file name. If you'd like to override this, you can use the `-t <title>`/`--title <title>` option. This is full HTML page. If you just want the content without `<html>` and `<body>` tags, include `-c`/`--content-only`.

#### Tree Links

The links in the tree return query strings with the ID of the term to browse all the terms in the tree:
```
?id=FOO:123
```

If you provide the `-d`/`--include-db` flag, you will also get the `db` parameter in the query string. The value of this parameter is the base name of the database file.
```
?db=bar&id=FOO:123
```

Alternatively, if your script expects a different format than query strings (or different parameter names), you can use the `-H`/`--href` option and pass a python-esqe formatting string, e.g. `-H "./{curie}"` or `-H "?curie={curie}"`. When you click on the `FOO:123` term, the link will direct to `./FOO:123` or `?curie=FOO:123`, respectively, instead of `?id=FOO:123`.

The formatting string must contain `{curie}`, and optionally contain `{db}`. Any other text enclosed in curly brackets will be ignored. This should not be used with the `-d` flag.

#### Predicates

When displaying a term, `gizmos.tree` will display all predicate-value pairs listed in alphabetical order by predicate label on the right-hand side of the window. You can define which predicates to include with the `-p`/`--predicate` and `-P`/`--predicates` options.

You can pass one or more predicate CURIEs in the command line using `-p`/`--predicate`. These will appear in the order that you pass:
```
python3 -m gizmos.tree foo.db foo:123 -p rdfs:label -p rdfs:comment > bar.html
```

You can also pass a text file containing a list of predicate CURIEs (one per line) using `-P`/`--predicates`:
```
python3 -m gizmos.tree foo.db foo:123 -P predicates.txt > bar.html
```

You can specify to include the remaining predicates with `*`. The `*` can appear anywhere in the list, so you can choose to include certain predicates last:
```
rdfs:label
*
rdfs:comment
```

The `*` character also works on the command line, but must be enclosed in quotes:
```
python3 -m gizmos.tree foo.db foo:123 -p rdfs:label -p "*" > bar.html
```

#### CGI Script Example

A simple, single-database setup. Note that `foo.db` must exist.

```bash
# Create a phony URL from QUERY_STRING env variable
URL="http://example.com?${QUERY_STRING}"

# Retrieve the ID using urlp
ID=$(urlp --query --query_field=id "${URL}")

# Generate the tree view
if [[ ${ID} ]]; then
    python3 -m gizmos.tree foo.db ${ID}
else
    python3 -m gizmos.tree foo.db
fi
```

A more complex example with multiple databases and a search bar. Note that the `build/` directory containing all database files must exist.

```bash
# Create a phony URL from QUERY_STRING env variable
URL="http://example.com?${QUERY_STRING}"

# Retrieve values using urlp
ID=$(urlp --query --query_field=id "${URL}")
DB=$(urlp --query --query_field=db "${URL}")

# These parameters are used exclusively for gizmos.search
FORMAT=$(urlp --query --query_field=format "${URL}")
TEXT=$(urlp --query --query_field=text "${URL}")

if [ ${FORMAT} == "json" ]; then
    # Call gizmos.search to return names JSON for typeahead search
    if [[ ${TEXT} ]]; then
        python3 -m gizmos.search build/${DB}.db ${TEXT}
    else
        python3 -m gizmos.search build/${DB}.db
    fi
else
    # Generate the tree view with database query parameter and search bar
    if [[ ${ID} ]]; then
        python3 -m gizmos.tree build/${DB}.db ${ID} -d -s
    else
        python3 -m gizmos.tree build/${DB}.db -d -s
    fi
fi
```
