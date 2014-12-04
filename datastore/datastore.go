package datastore

import (
	"appengine_internal"
	pb "appengine_internal/datastore"
	"fmt"
)

type entityDictEntity struct {
	Key *pb.Reference
	Obj *pb.EntityProto
}

type entityDict struct {
	dict          map[string]entityDictEntity
	isTransaction bool
}

func newEntityDict(t bool) *entityDict {
	return &entityDict{
		dict:          make(map[string]entityDictEntity),
		isTransaction: t,
	}
}

func (this *entityDict) Put(key *pb.Reference, obj *pb.EntityProto) {
	this.dict[getDictKey(key)] = entityDictEntity{key, obj}
}

func (this *entityDict) Delete(key *pb.Reference) {
	k := getDictKey(key)
	if this.isTransaction {
		this.dict[k] = entityDictEntity{key, nil}
	} else {
		delete(this.dict, k)
	}
}

func (this *entityDict) Get(key *pb.Reference) *pb.EntityProto {
	return this.dict[getDictKey(key)].Obj
}

func (this *entityDict) Entities() []entityDictEntity {
	entities := make([]entityDictEntity, 0)
	for _, e := range this.dict {
		entities = append(entities, e)
	}
	return entities
}

func getDictKey(key *pb.Reference) string {
	return key.String()
}

type InMemoryDatastore struct {
	entities    *entityDict
	idCounter   int64
	thCounter   uint64 // counter for transaction handles
	transaction *pb.Transaction
	tEntities   map[uint64]*entityDict
}

func New() *InMemoryDatastore {
	return &InMemoryDatastore{
		entities:  newEntityDict(false),
		idCounter: int64(1),
		tEntities: make(map[uint64]*entityDict),
	}
}

func (this *InMemoryDatastore) Call(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	switch method {
	case "Put":
		req := in.(*pb.PutRequest)
		res := out.(*pb.PutResponse)
		return this.PutMulti(req, res)
	case "Get":
		req := in.(*pb.GetRequest)
		res := out.(*pb.GetResponse)
		return this.GetMulti(req, res)
	case "Delete":
		req := in.(*pb.DeleteRequest)
		res := out.(*pb.DeleteResponse)
		return this.DeleteMulti(req, res)
	case "AllocateIds":
		req := in.(*pb.AllocateIdsRequest)
		res := out.(*pb.AllocateIdsResponse)
		return this.AllocateIDs(req, res)
	case "BeginTransaction":
		req := in.(*pb.BeginTransactionRequest)
		t := out.(*pb.Transaction)
		return this.BeginTransaction(req, t)
	case "Rollback":
		t := in.(*pb.Transaction)
		return this.Rollback(t)
	case "Commit":
		t := in.(*pb.Transaction)
		res := out.(*pb.CommitResponse)
		return this.Commit(t, res)
	case "RunQuery":
		q := in.(*pb.Query)
		res := out.(*pb.QueryResult)
		return this.RunQuery(q, res)
	case "Next":
		req := in.(*pb.NextRequest)
		res := out.(*pb.QueryResult)
		return this.Next(req, res)
	default:
		return fmt.Errorf("aeunit datastore: Unknown method %s", method)
	}
}

func (this *InMemoryDatastore) Close() error {
	return nil
}

func (this *InMemoryDatastore) PutMulti(req *pb.PutRequest, res *pb.PutResponse) error {
	// TODO (siniec): fix incomplete keys
	keys := make([]*pb.Reference, len(req.Entity))
	for i, entity := range req.Entity {
		keys[i] = entity.Key
		dict := this.getDict(req.Transaction)
		dict.Put(entity.Key, entity)
	}
	res.Key = keys
	return nil
}

func (this *InMemoryDatastore) GetMulti(req *pb.GetRequest, res *pb.GetResponse) error {
	entities := make([]*pb.GetResponse_Entity, len(req.Key))
	for i, key := range req.Key {
		dict := this.getDict(nil) // pass nil: get only reads from the "non transactional" entity store
		entity := dict.Get(key)
		entities[i] = &pb.GetResponse_Entity{
			Entity: entity,
			Key:    key,
		}
	}
	res.Entity = entities
	return nil
}

func (this *InMemoryDatastore) DeleteMulti(req *pb.DeleteRequest, res *pb.DeleteResponse) error {
	for _, key := range req.Key {
		dict := this.getDict(req.Transaction)
		dict.Delete(key)
	}
	return nil
}

func (this *InMemoryDatastore) AllocateIDs(req *pb.AllocateIdsRequest, res *pb.AllocateIdsResponse) error {
	count := req.GetSize()
	low := this.idCounter
	this.idCounter += count
	high := this.idCounter - 1 // datastore expects the returned range to be inclusive in both ends
	res.Start = &low
	res.End = &high
	return nil
}

func (this *InMemoryDatastore) BeginTransaction(req *pb.BeginTransactionRequest, t *pb.Transaction) error {
	handle := this.thCounter
	t.Handle = &handle
	this.thCounter += 1
	this.tEntities[handle] = newEntityDict(true)
	return nil
}

func (this *InMemoryDatastore) Rollback(t *pb.Transaction) error {
	delete(this.tEntities, t.GetHandle())
	return nil
}

func (this *InMemoryDatastore) Commit(t *pb.Transaction, res *pb.CommitResponse) error {
	dict := this.getDict(t)
	for _, entity := range dict.Entities() {
		if entity.Obj != nil {
			this.entities.Put(entity.Key, entity.Obj)
		} else {
			this.entities.Delete(entity.Key)
		}
	}
	return nil
}

// getDict returns the entityDict for either the given transaction or the default dict
func (this *InMemoryDatastore) getDict(t *pb.Transaction) *entityDict {
	if t == nil {
		return this.entities
	} else {
		return this.tEntities[t.GetHandle()]
	}
}
