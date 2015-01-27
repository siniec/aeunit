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
	type obj struct {
		I  int64
		F  float64
		Is []int64
		Fs []float64
	}
	type f struct {
		op  string
		val interface{}
	}
	tests := []struct {
		name    string
		objs    []obj // object stored in datastore
		filters []f
		want    []int // indexes of the objects from .objs we want returned by the query
	}{
		// Non existant field
		{
			name:    "Non existant field",
			objs:    []obj{obj{I: 1, F: 1}},
			filters: []f{f{"NonExistantField=", 1}},
			want:    nil,
		},
		// // One field
		{
			name:    "=",
			objs:    []obj{obj{I: 0}, obj{I: 1}, obj{I: 2}},
			filters: []f{f{"I=", 1}},
			want:    []int{1},
		},
		{
			name:    "<",
			objs:    []obj{obj{I: 0}, obj{I: 1}, obj{I: 2}},
			filters: []f{f{"I<", 2}},
			want:    []int{0, 1},
		},
		{
			name:    "<=",
			objs:    []obj{obj{I: 0}, obj{I: 1}, obj{I: 2}},
			filters: []f{f{"I<=", 1}},
			want:    []int{0, 1},
		},
		{
			name:    "<",
			objs:    []obj{obj{I: 0}, obj{I: 1}, obj{I: 2}},
			filters: []f{f{"I>", 0}},
			want:    []int{1, 2},
		},
		{
			name:    "<=",
			objs:    []obj{obj{I: 0}, obj{I: 1}, obj{I: 2}},
			filters: []f{f{"I>=", 1}},
			want:    []int{1, 2},
		},
		// // One field, multiple conditions
		{
			name:    "I=1 && I=2",
			objs:    []obj{obj{I: 1}, obj{I: 2}},
			filters: []f{f{"I=", 1}, f{"I=", 2}},
			want:    nil,
		},
		{
			name:    "1 < I < 4",
			objs:    []obj{obj{I: 0}, obj{I: 1}, obj{I: 2}, obj{I: 3}, obj{I: 4}, obj{I: 5}},
			filters: []f{f{"I>", 1}, f{"I<", 4}},
			want:    []int{2, 3},
		},
		// Multiple fields, multiple conditions
		{
			name:    "I = 1, F = 2.5",
			objs:    []obj{obj{I: 1, F: 2.4}, obj{I: 1, F: 2.5}, obj{I: 2, F: 2.5}},
			filters: []f{f{"I=", 1}, f{"F=", 2.5}},
			want:    []int{1},
		},
		{
			name:    "1 < I < 4, F = 2.5",
			objs:    []obj{obj{I: 1, F: 2.5}, obj{I: 2, F: 2.5}, obj{I: 3, F: 2.5}, obj{I: 3, F: 2.4}},
			filters: []f{f{"I>", 1}, f{"I<", 4}, f{"F=", 2.5}},
			want:    []int{1, 2},
		},
		// Slice field(s)
		{
			name: "Is = 2",
			objs: []obj{
				obj{Is: []int64{-1, 0, 1}}, // don't want
				obj{Is: []int64{0, 1, 2}},  // want
				obj{Is: []int64{1, 2, 3}},  // want
				obj{Is: []int64{2, 3, 4}},  // want
				obj{Is: []int64{3, 4, 5}}}, // don't want
			filters: []f{f{"Is=", 2}},
			want:    []int{1, 2, 3},
		},
		{
			name: "Is > 2",
			objs: []obj{
				obj{Is: []int64{-1, 0, 1}}, // don't want
				obj{Is: []int64{0, 1, 2}},  // don't want
				obj{Is: []int64{1, 2, 3}},  // want
				obj{Is: []int64{2, 3, 4}},  // want
				obj{Is: []int64{3, 4, 5}}}, // want
			filters: []f{f{"Is>", 2}},
			want:    []int{2, 3, 4},
		},
		// "Special" slice cases (non intuitive/unexpected behaviours):
		// see https://cloud.	google.com/appengine/docs/go/datastore/queries#Go_Filters
		// Multiple equality filters on slice property matches if at least one slice value matches *one* of the filters
		{
			name: "Is = 2, Is = 3",
			objs: []obj{
				obj{Is: []int64{-1, 0, 1}}, // don't want
				obj{Is: []int64{0, 1, 2}},  // want (2)
				obj{Is: []int64{1, 2, 3}},  // want (2 and 3)
				obj{Is: []int64{2, 3, 4}},  // want (2 and 3)
				obj{Is: []int64{3, 4, 5}},  // want (3)
				obj{Is: []int64{4, 5, 6}}}, // don't want
			filters: []f{f{"Is=", 2}, f{"Is=", 3}},
			want:    []int{1, 2, 3, 4}, // want entities with slice containing 2 or 3 or both 2 and 3
		},
		{
			// "If a query has multiple inequality filters on a given property, an entity will match the query only if
			// at least one of its individual values for the property satisfies all of the filters"
			name: "2 > Is > 4",
			objs: []obj{
				obj{Is: []int64{0, 1, 2}},  // don't want
				obj{Is: []int64{1, 2, 3}},  // want
				obj{Is: []int64{2, 3, 4}},  // want
				obj{Is: []int64{3, 4, 5}},  // want
				obj{Is: []int64{4, 5, 6}}}, // don't want
			filters: []f{f{"Is>", 2}, f{"Is<", 4}},
			want:    []int{1, 2, 3}, // want entities with slice containing 3
		},

		//TODO: key equality
	}

	for _, test := range tests {
		c := newContext()
		keys := make([]*datastore.Key, len(test.objs))
		for i := range test.objs {
			keys[i] = datastore.NewKey(c, "Obj", "", int64(i+1), nil)
		}
		if _, err := datastore.PutMulti(c, keys, test.objs); err != nil {
			t.Errorf("Error saving objects (should not happen): %v", err)
			continue
		}

		q := datastore.NewQuery("Obj")
		for _, filter := range test.filters {
			q = q.Filter(filter.op, filter.val)
		}
		var got []obj
		_, err := q.GetAll(c, &got)
		if err != nil {
			t.Errorf("Test %s: GetAll() returned error %v", test.name, err)
		}
		if len(got) != len(test.want) {
			t.Errorf("Test %s: GetAll() returned wrong number of entities (got %d, want %d).\nGot %v", test.name, len(got), len(test.want), got)
		} else {
			for i, wantIndex := range test.want {
				want := test.objs[wantIndex]
				if !reflect.DeepEqual(got[i], want) {
					t.Errorf("Test %s: GetAll() returned wrong entity at index %d.\nGot  %v\nWant %v", test.name, i, got[i], want)
				}
			}
		}
		// TODO: check keys
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
