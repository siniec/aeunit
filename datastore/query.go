package datastore

import (
	pb "appengine_internal/datastore"
	"fmt"
	"sort"
)

func (this *InMemoryDatastore) RunQuery(q *pb.Query, res *pb.QueryResult) error {

	if err := nonsupported(q); err != "" {
		return fmt.Errorf("aeunit datastore: internal error. Not implemented logic for %s", err)
	}

	kind := q.GetKind()
	es := make([]*pb.EntityProto, 0)
	for _, de := range this.entities.Entities() {
		if entityProtoKind(de.Obj) == kind {
			es = append(es, de.Obj)
		}
	}
	s := newSortableEntities(es, q.GetOrder())
	s.Filter(q.GetFilter())
	s.Ancestor(q.Ancestor)
	s.SortableBy(q.GetOrder())
	sort.Sort(s)
	s.CursorOffset(q.CompiledCursor)
	s.Offset(q.Offset)
	s.Limit(q.Limit)
	res.Result = s.protos

	// RunQuery returns all results, always
	f := false
	res.MoreResults = &f

	// We need to calculate the cursor before we limit the result
	res.CompiledCursor = &pb.CompiledCursor{}
	if s.cursor != nil {
		res.CompiledCursor.Position = &pb.CompiledCursor_Position{
			Key: s.cursor.GetKey(),
		}
	}

	return nil
}

func (this *InMemoryDatastore) Next(req *pb.NextRequest, res *pb.QueryResult) error {
	panic("aeunit datastore: Next is not implemented. RunQuery returns all results")
	return nil
}

type sortableEntities struct {
	cursor *pb.EntityProto
	protos []*pb.EntityProto
	compFn comparer
}

type comparer func(a, b *pb.EntityProto) int

func newSortableEntities(p []*pb.EntityProto, order []*pb.Query_Order) *sortableEntities {
	return &sortableEntities{
		protos: p,
		compFn: getCompFn(order),
	}
}

func getProperty(e *pb.EntityProto) []*pb.Property {
	props := e.GetProperty()
	if len(props) == 0 {
		props = e.GetRawProperty()
	}
	return props
}

func getPropValue(e *pb.EntityProto, name string) *pb.PropertyValue {
	props := getProperty(e)
	for _, p := range props {
		if p.GetName() == name {
			return p.GetValue()
		}
	}
	return nil
}

func getCompFn(order []*pb.Query_Order) comparer {
	if order == nil {
		return func(a, b *pb.EntityProto) int {
			return compareProtoRef(a.GetKey(), b.GetKey())
		}
	} else {
		if len(order) > 1 {
			panic("aeunit datastore: Ordering on multiple fields is not supported")
		}
		return func(a, b *pb.EntityProto) int {
			pn := order[0].GetProperty()
			asc := order[0].GetDirection() == pb.Query_Order_ASCENDING
			p1 := getPropValue(a, pn)
			p2 := getPropValue(b, pn)
			d, valid := comparePropertyValue(p1, p2)
			if !valid {
				panic("aeunit datastore: internal error. Invalid entity. Didn't have required property for comparing")
			}
			if !asc {
				d = d * -1
			}
			return d
		}
	}
}

func (this *sortableEntities) Len() int { return len(this.protos) }

func (this *sortableEntities) Swap(i, j int) {
	this.protos[i], this.protos[j] = this.protos[j], this.protos[i]
}
func (this *sortableEntities) Less(i, j int) bool {
	return -1 == this.compFn(this.protos[i], this.protos[j])
}

func (this *sortableEntities) Limit(l *int32) {
	if l != nil {
		lim := *l
		if int32(len(this.protos)) > lim {
			this.protos = this.protos[:lim]
		}
	}
	n := len(this.protos)
	if n > 0 {
		this.cursor = this.protos[n-1]
	}
}

func (this *sortableEntities) Offset(o *int32) {
	if o != nil {
		off := *o
		if off > 0 && int32(len(this.protos)) >= off {
			if i := off - 1; i >= 0 {
				this.cursor = this.protos[i]
			}
			this.protos = this.protos[off:]
		}
	}
}

func (this *sortableEntities) CursorOffset(c *pb.CompiledCursor) {
	if c != nil && c.Position != nil && c.Position.Key != nil {
		for i, ep := range this.protos {
			if compareProtoRef(ep.Key, c.Position.Key) == 0 {
				this.cursor = this.protos[i]
				// the provided cursor is non inclusive, so we skip over the element with that key
				this.protos = this.protos[i+1:]
			}
		}
	}
}

func (this *sortableEntities) SortableBy(order []*pb.Query_Order) {
	if len(order) == 0 {
		return
	}
	sortable := make([]*pb.EntityProto, 0, len(this.protos))
	for _, ep := range this.protos {
		hasAllProps := true
		for _, o := range order {
			props := getProperty(ep)
			hasProp := false
			for _, prop := range props {
				if prop.GetName() == o.GetProperty() {
					hasProp = true
					break
				}
			}
			if !hasProp {
				hasAllProps = false
				break
			}
		}
		if hasAllProps {
			sortable = append(sortable, ep)
		}
	}
	this.protos = sortable
}

func (this *sortableEntities) Filter(filters []*pb.Query_Filter) {
	if len(filters) == 0 {
		return
	}
	filtered := make([]*pb.EntityProto, 0, len(this.protos))
	for _, e := range this.protos {
		ok := true
		for _, filter := range filters {
			fp := filter.GetProperty()[0] // datastore always sets this to a slice with length 1
			ep := getPropValue(e, fp.GetName())
			if ep == nil {
				ok = false
				break
			}
			d, valid := comparePropertyValue(ep, fp.GetValue())
			if !valid {
				ok = false
				break
			}

			switch filter.GetOp() {
			case pb.Query_Filter_EQUAL:
				ok = d == 0
			case pb.Query_Filter_LESS_THAN:
				ok = d < 0
			case pb.Query_Filter_GREATER_THAN:
				ok = d > 0
			case pb.Query_Filter_LESS_THAN_OR_EQUAL:
				ok = d <= 0
			case pb.Query_Filter_GREATER_THAN_OR_EQUAL:
				ok = d >= 0
			default:
				panic("aeunit datastore: Unsupported query filter operation: " + filter.GetOp().String())
			}

			if !ok {
				break
			}
		}
		if ok {
			filtered = append(filtered, e)
		}
	}
	this.protos = filtered
}

func (this *sortableEntities) Ancestor(ref *pb.Reference) {
	if ref == nil {
		return
	}
	filtered := make([]*pb.EntityProto, 0, len(this.protos))
	for _, ep := range this.protos {
		if ep.EntityGroup == nil {
			continue
		}
		match := true
		for i, el := range ref.Path.Element {
			if len(ep.Key.Path.Element) <= i {
				match = false
				break
			} else {
				match = 0 == compareProtoRefPathElem(el, ep.Key.Path.Element[i])
				if !match {
					break
				}
			}
		}
		if match {
			filtered = append(filtered, ep)
		}
	}
	this.protos = filtered
}

func entityProtoKind(e *pb.EntityProto) string {
	return e.GetKey().GetPath().GetElement()[0].GetType()
}

func nonsupported(q *pb.Query) string {
	switch {
	case q.EndCompiledCursor != nil:
		return "End()"
	case len(q.PropertyName) > 0:
		return "Project()"
	case len(q.GroupByPropertyName) > 0, q.Distinct != nil:
		return "Distinct()"
	}
	return ""
}
