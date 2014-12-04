package datastore

import (
	"appengine/datastore"
	pb "appengine_internal/datastore"
	"code.google.com/p/goprotobuf/proto"
	"testing"
)

func TestCompareProtoRef(t *testing.T) {
	c := newContext()

	p := func(kind, stringID string, intID int64) *datastore.Key {
		parent := datastore.NewKey(c, kind, stringID, intID, nil)
		return datastore.NewKey(c, "Kind", "", 1, parent)
	}

	pk := datastore.NewKey(c, "Kind", "parent", 0, nil)
	var tests = []struct {
		key1 *datastore.Key
		key2 *datastore.Key
		res  int
	}{
		// Kind
		{datastore.NewKey(c, "aaaa", "", 1, nil), datastore.NewKey(c, "bbbb", "", 1, nil), -1},
		{datastore.NewKey(c, "bbbb", "", 1, nil), datastore.NewKey(c, "aaaa", "", 1, nil), 1},
		// IntID
		{datastore.NewKey(c, "Kind", "", 1, nil), datastore.NewKey(c, "Kind", "", 1, nil), 0},
		{datastore.NewKey(c, "Kind", "", 1, nil), datastore.NewKey(c, "Kind", "", 2, nil), -1},
		// StringID
		{datastore.NewKey(c, "Kind", "a", 0, nil), datastore.NewKey(c, "Kind", "a", 0, nil), 0},
		{datastore.NewKey(c, "Kind", "a", 0, nil), datastore.NewKey(c, "Kind", "b", 0, nil), -1},
		// Numeric IDs precede string IDs
		{datastore.NewKey(c, "Kind", "", 1, nil), datastore.NewKey(c, "Kind", "a", 0, nil), -1},
		// When same ancestor: Kind, then IntID/StringID
		{datastore.NewKey(c, "Kind", "", 1, pk), datastore.NewKey(c, "Kind", "", 1, pk), 0},
		{datastore.NewKey(c, "aaaa", "", 1, pk), datastore.NewKey(c, "bbbb", "", 1, pk), -1},
		{datastore.NewKey(c, "Kind", "a", 0, pk), datastore.NewKey(c, "Kind", "a", 0, pk), 0},
		{datastore.NewKey(c, "Kind", "", 1, pk), datastore.NewKey(c, "Kind", "", 2, pk), -1},
		{datastore.NewKey(c, "Kind", "a", 0, pk), datastore.NewKey(c, "Kind", "b", 0, pk), -1},
		{datastore.NewKey(c, "Kind", "", 1, pk), datastore.NewKey(c, "Kind", "a", 0, pk), -1},
		// When different ancestor: on ancestor
		{p("Kind", "", 1), p("Kind", "", 1), 0},
		{p("aaaa", "", 1), p("bbbb", "", 1), -1},
		{p("Kind", "a", 0), p("Kind", "a", 0), 0},
		{p("Kind", "", 1), p("Kind", "", 2), -1},
		{p("Kind", "a", 0), p("Kind", "b", 0), -1},
		{p("Kind", "", 1), p("Kind", "a", 0), -1},
		//Different length ancestor path, but same common path
		{
			datastore.NewKey(c, "Parent", "", 1, datastore.NewKey(c, "Grandparent", "", 1, nil)),
			datastore.NewKey(c, "Child", "", 1, datastore.NewKey(c, "Parent", "", 1, datastore.NewKey(c, "Grandparent", "", 1, nil))),
			-1,
		},
	}
	for i, test := range tests {
		proto1 := keyToProto(c.FullyQualifiedAppID(), test.key1)
		proto2 := keyToProto(c.FullyQualifiedAppID(), test.key2)
		want := test.res
		actual := compareProtoRef(proto1, proto2)
		if want != actual {
			t.Errorf("Test %d failed. Got %d, wanted %d", i, want, actual)
			continue
		}
		if test.res != 0 {
			actual := compareProtoRef(proto2, proto1)
			want = test.res * -1
			if actual != want {
				t.Errorf("Test %d in reverse failed. Got %d, wanted %d", i, want, actual)
			}
		}
	}
}

func TestComparePropertyValue(t *testing.T) {
	int1, int2, fals, tru, str1, str2, dbl1, dbl2 := int64(1), int64(2), false, true, "a", "b", 1.0, 2.0

	var tests = []struct {
		val1 *pb.PropertyValue
		val2 *pb.PropertyValue
		res  int
	}{
		{&pb.PropertyValue{Int64Value: &int1}, &pb.PropertyValue{Int64Value: &int1}, 0},
		{&pb.PropertyValue{Int64Value: &int1}, &pb.PropertyValue{Int64Value: &int2}, -1},
		{&pb.PropertyValue{StringValue: &str1}, &pb.PropertyValue{StringValue: &str1}, 0},
		{&pb.PropertyValue{StringValue: &str1}, &pb.PropertyValue{StringValue: &str2}, -1},
		{&pb.PropertyValue{DoubleValue: &dbl1}, &pb.PropertyValue{DoubleValue: &dbl1}, 0},
		{&pb.PropertyValue{DoubleValue: &dbl1}, &pb.PropertyValue{DoubleValue: &dbl2}, -1},
		{&pb.PropertyValue{BooleanValue: &fals}, &pb.PropertyValue{BooleanValue: &fals}, 0},
		{&pb.PropertyValue{BooleanValue: &fals}, &pb.PropertyValue{BooleanValue: &tru}, -1},

		// Extra checks
		{&pb.PropertyValue{BooleanValue: &tru}, &pb.PropertyValue{BooleanValue: &tru}, 0},
	}

	for _, test := range tests {
		want := test.res
		actual, valid := comparePropertyValue(test.val1, test.val2)
		if !valid {
			t.Errorf("Valid was false. %v, %v", test.val1, test.val2)
		}
		if want != actual {
			t.Errorf("Got %d, wanted %d. %v, %v", actual, want, test.val1, test.val2)
			continue
		}
		if test.res != 0 {
			actual, valid := comparePropertyValue(test.val2, test.val1)
			if !valid {
				t.Errorf("Valid was false. %v, %v", test.val1, test.val2)
			}
			want = test.res * -1
			if actual != want {
				t.Errorf("Reverse comp failed. Got %d, wanted %d. %v, %v", actual, want, test.val1, test.val2)
			}
		}
	}
	// Different value types returns false second parameter. Use the
	for i := 0; i < 6; i += 2 {
		val1 := tests[i].val1
		for j := 0; j < len(tests); j += 2 {
			if j == i {
				continue
			}
			val2 := tests[j].val1
			if _, valid := comparePropertyValue(val1, val2); valid {
				t.Errorf("Expected valid = false for %v vs %v", val1, val2)
			}
		}
	}
}

func keyToProto(defaultAppID string, k *datastore.Key) *pb.Reference {
	appID := k.AppID()
	n := 0
	for i := k; i != nil; i = i.Parent() {
		n++
	}
	e := make([]*pb.Path_Element, n)
	for i := k; i != nil; i = i.Parent() {
		n--
		e[n] = &pb.Path_Element{
			Type: proto.String(i.Kind()),
		}
		// At most one of {Name,Id} should be set.
		// Neither will be set for incomplete keys.
		if i.StringID() != "" {
			e[n].Name = proto.String(i.StringID())
		} else if i.IntID() != 0 {
			e[n].Id = proto.Int64(i.IntID())
		}
	}
	var namespace *string
	// if k.Namespace() != "" {
	// 	namespace = proto.String(k.Namespace())
	// }
	return &pb.Reference{
		App:       proto.String(appID),
		NameSpace: namespace,
		Path: &pb.Path{
			Element: e,
		},
	}
}
