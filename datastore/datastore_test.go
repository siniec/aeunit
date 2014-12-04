package datastore

import (
	"appengine"
	"appengine/datastore"
	"appengine_internal"
	pb "appengine_internal/base"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

type Thing struct {
	IntProp  int
	StrProp  string
	BoolProp bool
	DblProp  float64
}

func (this Thing) String() string {
	s := ""
	switch {
	case this.IntProp != 0:
		s += fmt.Sprintf("IntProp: %d. ", this.IntProp)
	case this.StrProp != "":
		s += fmt.Sprintf("StrProp: %s. ", this.StrProp)
	case this.DblProp != 0.0:
		s += fmt.Sprintf("DblProp: %v. ", this.DblProp)
	}
	s += fmt.Sprintf("BoolProp: %v", this.BoolProp)
	return s
}

func thing(seq int) Thing {
	return Thing{
		IntProp:  seq,
		StrProp:  "Thing" + strconv.Itoa(seq),
		BoolProp: seq%2 == 0,
		DblProp:  float64(seq) + 0.5,
	}
}

func newContext() appengine.Context {
	return &testContext{
		ds: New(),
	}
}

func TestDatastorePutGet(t *testing.T) {
	c := newContext()
	key1 := datastore.NewKey(c, "Kind1", "", 1, nil)
	key2 := datastore.NewKey(c, "Kind2", "", 1, nil)
	key3 := datastore.NewKey(c, "Kind3", "name", 0, nil)

	type T struct {
		Name         string
		PutKeys      []*datastore.Key
		Puts         []Thing
		PutErr       error
		GetKeys      []*datastore.Key
		Gets         []Thing
		GetErr       error
		ExpectedGets []Thing
	}
	tests := []T{
		T{"Empty key slice", []*datastore.Key{}, []Thing{}, nil, []*datastore.Key{}, []Thing{}, nil, nil},
		T{
			"Multiple kinds, string/int IDs",
			[]*datastore.Key{key1, key2, key3},
			[]Thing{thing(1), thing(2), thing(3)},
			nil,
			nil, // same as put keys
			[]Thing{Thing{}, Thing{}, Thing{}},
			nil,
			nil, // expect the same as what we put
		},
		T{
			"NoSuchEntity",
			[]*datastore.Key{key1},
			[]Thing{thing(1)},
			nil,
			[]*datastore.Key{key1, key2},
			[]Thing{Thing{}, Thing{}},
			appengine.MultiError{nil, datastore.ErrNoSuchEntity},
			[]Thing{thing(1), Thing{}},
		},
		T{
			"Same key properties, different parent",
			[]*datastore.Key{datastore.NewKey(c, "Kind1", "", 1, key1), datastore.NewKey(c, "Kind1", "", 1, key2)},
			[]Thing{thing(1), thing(2)},
			nil,
			nil, // same as put keys
			[]Thing{Thing{}, Thing{}},
			nil,
			[]Thing{thing(1), thing(2)},
		},
	}

	for i, test := range tests {
		ctx := newContext()

		keys, err := datastore.PutMulti(ctx, test.PutKeys, test.Puts)
		if test.PutErr != nil {
			if !reflect.DeepEqual(test.PutErr, err) {
				t.Errorf("Test %d, %s: Expected put error. Got %v", i, test.Name, err)
				continue
			}
		} else {
			if err != nil {
				t.Errorf("Test %d, %s: Put returned error: %v", i, test.Name, err)
				continue
			}
		}

		for i, key := range keys {
			if !key.Equal(test.PutKeys[i]) {
				t.Errorf("Test %d, %s: Put returned wrong key at index %d. Expected %v, was %v", i, test.Name, test.PutKeys[i], key)
			}
		}

		getKeys := test.GetKeys
		if getKeys == nil {
			getKeys = test.PutKeys
		}
		err = datastore.GetMulti(ctx, getKeys, test.Gets)
		if test.GetErr != nil {
			if !reflect.DeepEqual(test.GetErr, err) {
				t.Errorf("Test %d, %s: Expected get error. Got %v", i, test.Name, err)
				continue
			}
		} else {
			if err != nil {
				t.Errorf("Test %d, %s: Get returned error: %v", i, test.Name, err)
				continue
			}
		}

		expectedGets := test.ExpectedGets
		if expectedGets == nil {
			expectedGets = test.Puts
		}

		if len(expectedGets) != len(test.Gets) {
			t.Errorf("Test %d, %s: Wrong number of objects returned. Expected %d. Got %d", i, test.Name, len(test.ExpectedGets), len(test.Gets))
		} else {
			for j := range test.ExpectedGets {
				if !reflect.DeepEqual(test.ExpectedGets[j], test.Gets[j]) {
					t.Errorf("Test %d, %s: Object on index %d was not as expected", i, test.Name, j)
				}
			}
		}
	}
}

func TestDatastoreDelete(t *testing.T) {
	c := newContext()

	key1 := datastore.NewKey(c, "Kind1", "", 1, nil)
	key2 := datastore.NewKey(c, "Kind2", "", 1, nil)
	key3 := datastore.NewKey(c, "Kind3", "name", 0, nil)

	thing1 := thing(1)
	thing2 := thing(2)
	thing3 := thing(3)

	var err error
	_, err = datastore.Put(c, key1, &thing1)
	PanicIfErr(err)
	_, err = datastore.Put(c, key2, &thing2)
	PanicIfErr(err)
	_, err = datastore.Put(c, key3, &thing3)
	PanicIfErr(err)

	// Delete one entity
	err = datastore.Delete(c, key2)
	if err != nil {
		t.Errorf("Error returned when deleting entity: %v", err)
		t.FailNow()
	}

	// Check that it is gone
	err = datastore.Get(c, key2, &Thing{})
	if err != datastore.ErrNoSuchEntity {
		t.Errorf("Expected ErrNoSuchEntity after deleting entity. Was %v", err)
	}

	// Delete again, should return no error, even though the entity doesn't exist anymore
	err = datastore.Delete(c, key2)
	if err != nil {
		t.Errorf("Error returned when deleting entity not in datastore: %v", err)
		t.FailNow()
	}

	// Ensure that the two other objects are still present and unchanged
	var obj Thing

	obj = Thing{}
	if err = datastore.Get(c, key1, &obj); err != nil {
		if err == datastore.ErrNoSuchEntity {
			t.Errorf("datastore.Delete deleted more entities than we wanted")
		} else {
			t.Errorf("Error when getting entity: %v", err)
		}
		t.FailNow()
	} else {
		if !reflect.DeepEqual(obj, thing1) {
			t.Errorf("datastore.Delete changed an unrelated entity")
		}
	}

	obj = Thing{}
	if err = datastore.Get(c, key3, &obj); err != nil {
		if err == datastore.ErrNoSuchEntity {
			t.Errorf("datastore.Delete deleted more entities than we wanted")
		} else {
			t.Errorf("Error when getting entity: %v", err)
		}
		t.FailNow()
	} else {
		if !reflect.DeepEqual(obj, thing3) {
			t.Errorf("datastore.Delete changed an unrelated entity")
		}
	}
}

func TestDatastoreAllocateIDs(t *testing.T) {
	// AllocatedIDs should start at 1 and increment the IDs sequentially,
	// regardless of Kind or Parent

	c := newContext()

	low, high, err := datastore.AllocateIDs(c, "Kind", nil, 5)
	if err != nil {
		t.Errorf("Error returned :%v", err)
	}
	if low != 1 {
		t.Errorf("Expected low to be %d. Was %d", 1, low)
	}
	if high != 6 {
		t.Errorf("Expected high to be %d. Was %d", 6, high)
	}

	low, high, err = datastore.AllocateIDs(c, "Kind2", nil, 10)
	if err != nil {
		t.Errorf("Error returned :%v", err)
	}
	if low != 6 {
		t.Errorf("Expected low to be %d. Was %d", 6, low)
	}
	if high != 16 {
		t.Errorf("Expected high to be %d. Was %d", 16, high)
	}
}

func TestDatastoreTransactionRevert(t *testing.T) {

	var c appengine.Context
	var err error
	var o Thing
	var obj Thing

	c = newContext()
	key := datastore.NewKey(c, "Kind", "", 1, nil)

	// Rollback: put is reverted
	c = newContext()
	obj = thing(1)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if _, err := datastore.Put(c, key, &obj); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
		return errors.New("Test error")
	}, nil)
	err = datastore.Get(c, key, &obj)
	if err != datastore.ErrNoSuchEntity {
		if err == nil {
			t.Errorf("Put was not reverted. Object was saved even though transaction was rolled back")
		} else {
			t.Errorf("Unexpected error returned for get after put: %v", err)
		}
	}

	// Rollback: delete is reverted
	c = newContext()
	obj = thing(1)
	_, err = datastore.Put(c, key, &obj)
	PanicIfErr(err)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if err := datastore.Delete(c, key); err != nil {
			t.Errorf("Delete returned error: %v", err)
		}
		return errors.New("Test error")
	}, nil)
	o = Thing{}
	err = datastore.Get(c, key, &o)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			t.Errorf("Delete was not reverted. Object was deleted even though transaction was rolled back")
		} else {
			t.Errorf("Unexpected error returned for get after delete: %v", err)
		}
	} else {
		if !reflect.DeepEqual(o, obj) {
			t.Errorf("Delete was rolled back, but the object was changed in datastore")
		}
	}
}

func TestDatastoreTransactionCommit(t *testing.T) {
	var c appengine.Context
	var err error
	var o Thing
	var obj Thing

	c = newContext()
	key := datastore.NewKey(c, "Kind", "", 1, nil)

	// Commit: put is applied
	c = newContext()
	obj = thing(1)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if _, err := datastore.Put(c, key, &obj); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
		return nil
	}, nil)
	o = Thing{}
	err = datastore.Get(c, key, &o)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			t.Errorf("Put was not applied. Object was not saved")
		} else {
			t.Errorf("Unexpected error returned for get after put: %v", err)
		}
	} else {
		if !reflect.DeepEqual(o, obj) {
			t.Errorf("Put was applied, but, but the object saved was not as expected")
		}
	}

	// Commit: delete is applied
	c = newContext()
	obj = thing(1)
	_, err = datastore.Put(c, key, &obj)
	PanicIfErr(err)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if err := datastore.Delete(c, key); err != nil {
			t.Errorf("Delete returned error: %v", err)
		}
		return nil
	}, nil)
	err = datastore.Get(c, key, &obj)
	if err != datastore.ErrNoSuchEntity {
		if err == nil {
			t.Errorf("Delete was not applied. Object was not deleted")
		} else {
			t.Errorf("Unexpected error returned for get after delete: %v", err)
		}
	}

	// Commit: delete after put on same key results in deletion of key
	c = newContext()
	obj = thing(1)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if _, err := datastore.Put(c, key, &obj); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
		if err := datastore.Delete(c, key); err != nil {
			t.Errorf("Delete returned error: %v", err)
		}
		return nil
	}, nil)
	err = datastore.Get(c, key, &obj)
	if err != datastore.ErrNoSuchEntity {
		if err == nil {
			t.Errorf("Delete was not applied after put. Object was not deleted")
		} else {
			t.Errorf("Unexpected error returned for get after delete: %v", err)
		}
	}

	// Commit: put after delete on same key results in updated entity for that key
	c = newContext()
	obj = thing(1)
	_, err = datastore.Put(c, key, &obj)
	PanicIfErr(err)
	override := thing(2)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if err := datastore.Delete(c, key); err != nil {
			t.Errorf("Delete returned error: %v", err)
		}

		if _, err := datastore.Put(c, key, &override); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
		return nil
	}, nil)
	o = Thing{}
	err = datastore.Get(c, key, &o)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			t.Errorf("Put was not applied. Object was deleted instead of being overwritten")
		} else {
			t.Errorf("Unexpected error returned for get after put: %v", err)
		}
	} else {
		if !reflect.DeepEqual(o, override) {
			if reflect.DeepEqual(o, obj) {
				t.Errorf("Neither Delete nor Put was applied. Object is as before transaction finished")
			} else {
				t.Errorf("Updated object not as expected")
			}
		}
	}
}

//"isolation"
func TestDatastoreTransactionIsolation(t *testing.T) {
	var c appengine.Context
	var err error
	var obj Thing

	c = newContext()
	key := datastore.NewKey(c, "Kind", "", 1, nil)

	// In transaction: entity put inside transaction is not returned by get inside transaction
	c = newContext()
	obj = thing(1)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if _, err := datastore.Put(c, key, &obj); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
		err := datastore.Get(c, key, &obj)
		if err != datastore.ErrNoSuchEntity {
			if err == nil {
				t.Errorf("Get after Put inside transaction returned the entity that was put")
			} else {
				t.Errorf("Unexpected error returned for get: %v", err)
			}
		}
		return nil
	}, nil)

	// In transaction: entity deleted inside transaction is still gettable inside transaction
	c = newContext()
	obj = thing(1)
	_, err = datastore.Put(c, key, &obj)
	PanicIfErr(err)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		if err := datastore.Delete(c, key); err != nil {
			t.Errorf("Delete returned error: %v", err)
		}
		o := Thing{}
		err := datastore.Get(c, key, &o)
		if err != nil {
			if err == datastore.ErrNoSuchEntity {
				t.Errorf("Get after Delete inside transaction did not return the entity that was deleted")
			} else {
				t.Errorf("Unexpected error returned for get: %v", err)
			}
		} else {
			if !reflect.DeepEqual(o, obj) {
				t.Errorf("Get after Delete inside transaction returned object, but it was not as expected")
			}
		}
		return nil
	}, nil)

	// In transaction: getting entity updated inside transaction returns the old version of the entity
	c = newContext()
	obj = thing(1)
	_, err = datastore.Put(c, key, &obj)
	PanicIfErr(err)
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		o := thing(2)
		if _, err := datastore.Put(c, key, &o); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
		o2 := Thing{}
		err := datastore.Get(c, key, &o2)
		if err != nil {
			t.Errorf("Unexpected error returned for get: %v", err)
		} else {
			if !reflect.DeepEqual(o2, obj) {
				if reflect.DeepEqual(o2, o) {
					t.Errorf("Get of entity already put inside transaction returned the updated entity. Should return the old")
				} else {
					t.Errorf("Get of entity already put inside transaction returned unexpected entity")
				}
			}
		}
		return nil
	}, nil)
}

type testContext struct {
	ds *InMemoryDatastore
}

func (this *testContext) Debugf(s string, v ...interface{}) {
	fmt.Printf(s, v...)
	fmt.Println()
}
func (this *testContext) Infof(s string, v ...interface{})     { this.Debugf(s, v...) }
func (this *testContext) Warningf(s string, v ...interface{})  { this.Debugf(s, v...) }
func (this *testContext) Errorf(s string, v ...interface{})    { this.Debugf(s, v...) }
func (this *testContext) Criticalf(s string, v ...interface{}) { this.Debugf(s, v...) }
func (this *testContext) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	switch {
	case service == "__go__":
		if method == "GetNamespace" || method == "GetDefaultNamespace" {
			s := ""
			outStr := out.(*pb.StringProto)
			outStr.Value = &s
		}
		return nil
	case service == "datastore_v3":
		return this.ds.Call(method, in, out, opts)
	default:
		return fmt.Errorf("Unknown service: %s", service)
	}
}
func (this *testContext) FullyQualifiedAppID() string { return "dev~aeunit" }
func (this *testContext) Request() interface{}        { panic("Request() is not implemented") }

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
