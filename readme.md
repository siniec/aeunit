
# aeunit

Package for testing App Engine in golang without using the dev server. A faster addition to appengine/aetest

## datastore

An in memory datastore

### Not supported / TODOS

* multiple filters (support for only one at the moment)
* slice values (filter, order ++)
* End(), Project(), Distinct() operators on datastore.Query
* more