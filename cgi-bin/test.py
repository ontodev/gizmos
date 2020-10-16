#!/usr/bin/env python3

import cgi
import gizmos.tree
import gizmos.search

fields = cgi.FieldStorage()
db = fields.getvalue("db", "build/obi.db")
id = fields.getvalue("id")
text = fields.getvalue("text")

if text:
    gizmos.search.search(db, text)
else:
    gizmos.tree.tree(db, id, include_db=False, include_search=True)
