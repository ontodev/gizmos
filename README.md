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

If you provide the `-i`/`--include-db` flag, you will also get the `db` parameter in the query string. The value of this parameter is the base name of the database file.
```
?db=bar&id=FOO:123
```

This can be useful when writing scripts that return trees from different databases.
