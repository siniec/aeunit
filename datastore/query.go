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

func getProperty(e *pb.EntityProto, name string) []*pb.Property {
	props := e.GetProperty()
	if len(props) == 0 {
		props = e.GetRawProperty()
	}
	var ps []*pb.Property
	for _, prop := range props {
		if prop.GetName() == name {
			ps = append(ps, prop)
		}
	}
	return ps
}

func getPropValue(e *pb.EntityProto, name string) *pb.PropertyValue {
	props := getProperty(e, name)
	if len(props) == 0 {
		return nil
	} else {
		return props[0].GetValue()
	}
}

func getPropValues(e *pb.EntityProto, name string) []*pb.PropertyValue {
	props := getProperty(e, name)
	var values []*pb.PropertyValue
	for _, p := range props {
		values = append(values, p.GetValue())
	}
	return values
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
			props := getProperty(ep, o.GetProperty())
			hasAllProps = hasAllProps && len(props) > 0
			if !hasAllProps {
				break
			}
		}
		if hasAllProps {
			sortable = append(sortable, ep)
		}
	}
	this.protos = sortable
}

func anyValueMatches(values []*pb.PropertyValue, filters []*pb.Query_Filter, matchOnAllFilters bool) bool {
	if len(values) == 0 {
		return false
	}
	for _, filter := range filters {
		var ok bool
		want := filter.GetProperty()[0].GetValue()
		for _, val := range values {
			d, valid := comparePropertyValue(val, want)
			if !valid {
				if matchOnAllFilters {
					return false
				} else {
					continue
				}
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
			if ok {
				// We found a match. No need to search through the rest of the values for a match
				break
			}
		}
		if ok && !matchOnAllFilters {
			// if we only need to match on one (any) filter and we already did, return true now
			return true
		}
		if !ok && matchOnAllFilters {
			// if we need to match on all filters, and this filter did not have a match, the match has failed
			return false
		}
	}
	return matchOnAllFilters
}

func (this *sortableEntities) Filter(filters []*pb.Query_Filter) {
	if len(filters) == 0 {
		return
	}

	any := make(map[string]bool)
	filtersPerProp := make(map[string][]*pb.Query_Filter)
	for _, filter := range filters {
		propName := filter.GetProperty()[0].GetName()
		filtersPerProp[propName] = append(filtersPerProp[propName], filter)
	}
	for propName, filters := range filtersPerProp {
		i := 0
		allEq := true
		for _, filter := range filters {
			if filter.GetOp() != pb.Query_Filter_EQUAL {
				allEq = false
				break
			} else {
				i++
			}
		}
		if i >= 2 && allEq {
			any[propName] = true
		}
	}

	filtered := make([]*pb.EntityProto, 0, len(this.protos))
	for _, entity := range this.protos {
		ok := true
		for propName, filters := range filtersPerProp {
			props := getProperty(entity, propName)
			multi := len(props) > 0 && props[0].GetMultiple()
			any := multi && any[propName]
			values := getPropValues(entity, propName)
			if ok = anyValueMatches(values, filters, !any); !ok {
				break
			}
		}
		if ok {
			filtered = append(filtered, entity)
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
