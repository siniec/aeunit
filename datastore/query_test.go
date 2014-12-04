package datastore

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"reflect"
	"testing"
)

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func getKeys(c appengine.Context, kind string, intIDs ...int64) []*datastore.Key {
	keys := make([]*datastore.Key, len(intIDs))
	for i, id := range intIDs {
		keys[i] = datastore.NewKey(c, kind, "", id, nil)
	}
	return keys
}

func keysAndObjs(c appengine.Context, kind string, n int) ([]*datastore.Key, []Thing) {
	keys := make([]*datastore.Key, n)
	objs := make([]Thing, n)
	for i := 0; i < n; i++ {
		keys[i] = datastore.NewKey(c, kind, "", int64(i+1), nil)
		objs[i] = thing(i + 1)
	}
	return keys, objs
}

func reverse(objs []Thing) []Thing {
	n := len(objs)
	os := make([]Thing, n)
	for i := range objs {
		os[i] = objs[n-i-1]
	}
	return os
}

func expect(c appengine.Context, q *datastore.Query, expected []Thing) string {
	actual := make([]Thing, 0)
	_, err := q.GetAll(c, &actual)
	if err != nil {
		return fmt.Sprintf("GetAll() returned error %v", err)
	}
	if len(actual) != len(expected) {
		return fmt.Sprintf("GetAll() returned wrong number of entities (got %d, want %d). Got %v. Want %v", len(actual), len(expected), actual, expected)
	} else {
		for i := range expected {
			if !reflect.DeepEqual(actual[i], expected[i]) {
				return fmt.Sprintf("GetAll() returned wrong entity at index %d. Got %v, want %v", i, actual, expected)
			}
		}
	}
	return ""
}

func TestDatastoreQueryOnKind(t *testing.T) {
	c := newContext()
	keys := []*datastore.Key{datastore.NewKey(c, "KindA", "", 1, nil), datastore.NewKey(c, "KindB", "", 1, nil)}
	objs := []Thing{thing(1), thing(2)}
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)

	q := datastore.NewQuery("KindA")
	expected := []Thing{objs[0]}
	if e := expect(c, q, expected); e != "" {
		t.Error(e)
	}

	q = datastore.NewQuery("KindB")
	expected = []Thing{objs[1]}
	if e := expect(c, q, expected); e != "" {
		t.Error(e)
	}
}

func TestDatastoreQueryLimit(t *testing.T) {
	c := newContext()
	keys, objs := keysAndObjs(c, "Kind", 20)
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)
	for i := 5; i <= 25; i += 5 { // iterate to above len(objs) to see if we handle limit > store entity count
		n := min(len(objs), i)
		expected := objs[:n]
		q := datastore.NewQuery("Kind").Limit(i)
		if e := expect(c, q, expected); e != "" {
			t.Errorf("Limit(%d)", i, e)
		}
	}

	// Negative limit
	expected := objs
	q := datastore.NewQuery("Kind").Limit(-1)
	if e := expect(c, q, expected); e != "" {
		t.Errorf("Limit(-1): %s", e)
	}

	// Limit = 0
	expected = make([]Thing, 0)
	q = datastore.NewQuery("Kind").Limit(0)
	if e := expect(c, q, expected); e != "" {
		t.Errorf("Limit(0): %s", e)
	}
}

func TestDatastoreQueryOffset(t *testing.T) {
	c := newContext()
	keys, objs := keysAndObjs(c, "Kind", 5)
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)
	for i := range keys { // iterate to above len(objs) to see if we handle limit > store entity count
		expected := objs[i:]
		q := datastore.NewQuery("Kind").Offset(i)
		if e := expect(c, q, expected); e != "" {
			t.Errorf("Offset(%d): %s", i, e)
		}
	}

	expected := make([]Thing, 0)
	q := datastore.NewQuery("Kind").Offset(len(objs))
	if e := expect(c, q, expected); e != "" {
		t.Errorf("Offset(len(objs)): %s", e)
	}
}

func TestDatastoreQueryOrder(t *testing.T) {

	// to implement:
	// - order on multiple properties
	// - if ordering is equal, orders on key
	// - slice fields?
	// - order on geopoint, reference

	// Order on int, string, double
	c := newContext()
	// Scramble the keys. If the keys and objs are in the same order, we'll get the correct result by ordering on key
	// The end result of key->obj refs is this:
	// obj|key 1|4, 2|5, 3|1, 4|3, 5|2. keys asc: 4,5,1,3,2
	keys := getKeys(c, "Kind", 1, 4, 2, 5, 3)
	objs := []Thing{thing(3), thing(1), thing(5), thing(2), thing(4)}
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)

	expected := []Thing{thing(1), thing(2), thing(3), thing(4), thing(5)}

	// Ascending
	fields := []string{"IntProp", "StrProp", "DblProp"}
	for _, field := range fields {
		q := datastore.NewQuery("Kind").Order(field)
		if e := expect(c, q, expected); e != "" {
			t.Errorf("Order by %s: %s", field, e)
			continue
		}
	}

	//Descending
	expected = reverse(expected)
	for _, field := range fields {
		q := datastore.NewQuery("Kind").Order("-" + field)
		if e := expect(c, q, expected); e != "" {
			t.Errorf("Order by -%s: %s", field, e)
		}
	}

	// Order on bool
	c = newContext()
	keys = getKeys(c, "Kind", 1, 2)
	objs = []Thing{thing(1), thing(2)}
	objs[0].BoolProp = false
	objs[1].BoolProp = true
	_, err = datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)

	// Asc
	q := datastore.NewQuery("Kind").Order("BoolProp")
	if e := expect(c, q, objs); e != "" {
		t.Errorf("Order by BoolProp: %s", e)
	}

	// Desc
	expected = []Thing{objs[1], objs[0]}
	q = datastore.NewQuery("Kind").Order("-BoolProp")
	if e := expect(c, q, expected); e != "" {
		t.Errorf("Order by -BoolProp: %s", e)
	}

	// Order on non-existant field
	c = newContext()
	keys = getKeys(c, "Kind", 1, 2)
	objs = []Thing{thing(1), thing(2)}
	_, err = datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)
	q = datastore.NewQuery("Kind").Order("NonExistantField")
	expected = []Thing{}
	if e := expect(c, q, expected); e != "" {
		t.Errorf("Order by NonExistantField: %s", e)
	}
}

func TestDatastoreQueryFilter(t *testing.T) {
	c := newContext()
	keys, objs := keysAndObjs(c, "Kind", 10)
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)

	type f struct {
		op  string
		val interface{}
	}
	tests := []struct {
		filters  []f
		expected []Thing
	}{
		// Non existant field
		{[]f{f{"NonExistantField=", 1}}, []Thing{}},
		// One field
		{[]f{f{"IntProp=", 0}}, []Thing{}},
		{[]f{f{"IntProp=", 1}}, []Thing{thing(1)}},
		{[]f{f{"IntProp<", 2}}, []Thing{thing(1)}},
		{[]f{f{"IntProp>", 9}}, []Thing{thing(10)}},
		{[]f{f{"IntProp<=", 2}}, []Thing{thing(1), thing(2)}},
		{[]f{f{"IntProp>=", 9}}, []Thing{thing(9), thing(10)}},
		// One field, multiple conditions
		{[]f{f{"IntProp>", 3}, f{"IntProp=", 1}}, []Thing{}},
		{[]f{f{"IntProp>", 3}, f{"IntProp<", 5}}, []Thing{thing(4)}},
		// Multiple fields, multiple conditions
		// Note that DblProp is equal to IntProp + 0.5
		{[]f{f{"IntProp=", 1}, f{"DblProp=", 1.5}}, []Thing{thing(1)}},
		{[]f{f{"IntProp=", 1}, f{"DblProp>", 1.5}}, []Thing{}},
		{[]f{f{"IntProp>", 2}, f{"DblProp<", 5.5}}, []Thing{thing(3), thing(4)}},
	}

	for _, test := range tests {
		q := datastore.NewQuery("Kind")
		for _, filter := range test.filters {
			q = q.Filter(filter.op, filter.val)
		}
		if e := expect(c, q, test.expected); e != "" {
			t.Errorf("Filter %v: %s", test.filters, e)
		}
	}
}

func TestDatastoreQueryStart(t *testing.T) {
	c := newContext()
	keys, objs := keysAndObjs(c, "Kind", 5)
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)

	q := datastore.NewQuery("Kind")
	var cursor *datastore.Cursor
	for i := 0; i < len(keys); i++ {
		obj := Thing{}
		if cursor != nil {
			q = q.Start(*cursor)
		}
		iter := q.Run(c)
		_, err := iter.Next(&obj)
		if err == datastore.Done {
			t.Errorf("Next() returned Done after %d iterations. Want %d", i, len(keys))
			t.FailNow()
		}
		if err != nil {
			t.Errorf("Next() returned error %v", err)
			t.FailNow()
		}
		if !reflect.DeepEqual(obj, objs[i]) {
			t.Errorf("Next() returned wrong object. Got %v, want %v", obj, objs[i])
		}
		cs, _ := iter.Cursor()
		cursor = &cs
	}
}

func TestDatastoreQueryAncestor(t *testing.T) {
	c := newContext()
	g1 := datastore.NewKey(c, "Kind", "G1", 0, nil)
	g1p1 := datastore.NewKey(c, "Kind", "G1.P1", 0, g1)
	g1p1c1 := datastore.NewKey(c, "Kind", "G1.P1.C1", 0, g1p1)
	g1p1c2 := datastore.NewKey(c, "Kind", "G1.P1.C2", 0, g1p1)
	g1p2 := datastore.NewKey(c, "Kind", "G1.P2", 0, g1)
	g1p2c1 := datastore.NewKey(c, "Kind", "G1.P2.C1", 0, g1p2)
	g2 := datastore.NewKey(c, "Kind", "G2", 0, nil)
	g2p1 := datastore.NewKey(c, "Kind", "G2.P1", 0, g2)
	keys := []*datastore.Key{g1, g1p1, g1p1c1, g1p1c2, g1p2, g1p2c1, g2, g2p1}
	objs := make([]Thing, len(keys))
	for i, key := range keys {
		objs[i] = Thing{StrProp: key.StringID()}
	}
	_, err := datastore.PutMulti(c, keys, objs)
	PanicIfErr(err)

	nodes := func(ks ...*datastore.Key) []Thing {
		os := make([]Thing, len(ks))
		for i, k := range ks {
			for j := range keys {
				if keys[j].Equal(k) {
					os[i] = objs[j]
				}
			}
		}
		return os
	}

	tests := []struct {
		ancestor *datastore.Key
		expected []Thing
	}{
		{g1p1c1, nodes(g1p1c1)},
		{g1p1c2, nodes(g1p1c2)},
		{g1p2c1, nodes(g1p2c1)},
		{g2p1, nodes(g2p1)},
		{g1p1, nodes(g1p1, g1p1c1, g1p1c2)},
		{g1p2, nodes(g1p2, g1p2c1)},
		{g2, nodes(g2, g2p1)},
		{g1, nodes(g1, g1p1, g1p1c1, g1p1c2, g1p2, g1p2c1)},
	}

	for _, test := range tests {
		q := datastore.NewQuery("Kind").Ancestor(test.ancestor)
		if e := expect(c, q, test.expected); e != "" {
			t.Errorf("Ancestor(%v): %s", test.ancestor, e)
		}
	}

}
