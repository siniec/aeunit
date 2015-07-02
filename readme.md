
# aeunit

Package for testing App Engine in golang without using the dev server. A faster addition to appengine/aetest

## datastore

An in memory datastore

## run tests

    goapp test -v ./...

### Not supported / TODOS

* slice values (order ++)
* End(), Project(), Distinct() operators on datastore.Query
* more
