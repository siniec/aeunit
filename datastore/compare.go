package datastore

import (
	pb "appengine_internal/datastore"
)

func comparePropertyValue(val1, val2 *pb.PropertyValue) (int, bool) {
	var d int64
	switch {
	case val1.Int64Value != nil && val2.Int64Value != nil:
		d = val1.GetInt64Value() - val2.GetInt64Value()
	case val1.BooleanValue != nil && val2.BooleanValue != nil:
		switch {
		case val1.GetBooleanValue() && val2.GetBooleanValue(), !val1.GetBooleanValue() && !val2.GetBooleanValue():
			d = 0
		case val1.GetBooleanValue():
			d = 1
		case val2.GetBooleanValue():
			d = -1
		}
	case val1.StringValue != nil && val2.StringValue != nil:
		switch {
		case val1.GetStringValue() == val2.GetStringValue():
			d = 0
		case val1.GetStringValue() > val2.GetStringValue():
			d = 1
		default:
			d = -1
		}
	case val1.DoubleValue != nil && val2.DoubleValue != nil:
		switch {
		case val1.GetDoubleValue() == val2.GetDoubleValue():
			d = 0
		case val1.GetDoubleValue() > val2.GetDoubleValue():
			d = 1
		default:
			d = -1
		}
	case val1.Pointvalue != nil && val2.Pointvalue != nil,
		val1.Uservalue != nil && val2.Uservalue != nil,
		val1.Referencevalue != nil && val2.Referencevalue != nil:
		panic("Not implemented logic for comparing point, user, reference value")
	default:
		return 0, false
	}
	switch {
	case d < 0:
		d = -1
	case d > 0:
		d = 1
	}
	return int(d), true
}

// compares two pb.References. Returns -1 if ref1 is less than ref2, 0 if equal
func compareProtoRef(ref1, ref2 *pb.Reference) int {
	// From https://cloud.google.com/appengine/docs/go/datastore/queries
	// Elements of the ancestor path are compared similarly: by kind (string), then by key name or numeric ID.
	// Kinds and key names are strings and are ordered by byte value; numeric IDs are integers and are ordered numerically.
	// If entities with the same parent and kind use a mix of key name strings and numeric IDs, those with numeric IDs precede those with key names.
	el1 := ref1.GetPath().GetElement()
	el2 := ref2.GetPath().GetElement()
	n1 := len(el1)
	n2 := len(el2)
	for i := 0; ; i++ {
		if i == n1 || i == n2 {
			break
		}
		d := compareProtoRefPathElem(el1[i], el2[i])
		if d != 0 {
			return d
		}
	}
	d := n1 - n2
	switch {
	case d < 0:
		return -1
	case d > 0:
		return 1
	default:
		return 0
	}
}

func compareProtoRefPathElem(e1, e2 *pb.Path_Element) int {
	// Kind
	if e1.GetType() > e2.GetType() {
		return 1
	} else if e1.GetType() < e2.GetType() {
		return -1
	}

	// StringID
	if e1.GetName() > e2.GetName() {
		return 1
	} else if e1.GetName() < e2.GetName() {
		return -1
	}

	// IntID
	if e1.GetId() > e2.GetId() {
		return 1
	} else if e1.GetId() < e2.GetId() {
		return -1
	}
	return 0
}
