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

### `gizmos.tree`

The `tree` module produces a CGI tree browser for a given term contained in a SQL database. The SQL database should be created from OWL using [rdftab](https://github.com/ontodev/rdftab.rs) to ensure it is in the right format.

Usage in the command line:
```
python3 -m gizmos.tree [path-to-database] [term] > [output-html]
```

The `term` should be a CURIE with a prefix already defined in the `prefix` table of the database. If the `term` is not included, the output will show a tree starting at `owl:Thing`.

The links in the tree return query strings with the ID of the term:
```
?id=FOO:123
```

If you provide the `-d`/`--include-db` flag, you will also get the `db` parameter in the query string. The value of this parameter is the base name of the database file.
```
?db=bar&id=FOO:123
```

This can be useful when writing scripts that return trees from different databases.

If you provide the `-s`/`--include-search` flag, a search bar will be included in the page. This search bar uses [typeahead.js](https://twitter.github.io/typeahead.js/) and expects the output of `gizmos.names`. The URL for the fetching the data for [Bloodhound](https://github.com/twitter/typeahead.js/blob/master/doc/bloodhound.md) is `?text=[search-text]&format=json`, or `?db=[db]&text=[search-text]&format=json` if the `-d` flag is also provided. The `format=json` is provided as a flag for use in scripts. See the CGI Example below for details on implementation.

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

# These parameters are used exclusively for gizmos.names
FORMAT=$(urlp --query --query_field=format "${URL}")
TEXT=$(urlp --query --query_field=text "${URL}")

if [ ${FORMAT} == "json" ]; then
    # Call gizmos.names to return names JSON for typeahead search
    if [[ ${TEXT} ]]; then
        python3 -m gizmos.names build/${DB}.db ${TEXT}
    else
        python3 -m gizmos.names build/${DB}.db
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
