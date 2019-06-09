package index

import (
	"sort"
	"strings"

	mindex "github.com/lomik/go-carbon/tags/index/metric"
	tindex "github.com/lomik/go-carbon/tags/index/tag"
	tvindex "github.com/lomik/go-carbon/tags/index/tv"
)

type TagIndex struct {
	*tindex.Tree
	metrics     *stringID
	paths       *stringID
	tvs         *stringID
	path2Metric map[uint64]string
}

type TagValueInode = tvindex.TagValueInode
type TagInode = tindex.TagInode

type stringID struct {
	id     uint64
	str2ID map[string]uint64
	id2Str map[uint64]string
}

func newStringID() *stringID {
	return &stringID{str2ID: map[string]uint64{}, id2Str: map[uint64]string{}}
}

func (s *stringID) getID(str string) uint64 {
	if id, ok := s.str2ID[str]; ok {
		return id
	}
	id := s.id
	s.id2Str[id] = str
	s.str2ID[str] = id
	s.id++
	return id
}

func (s *stringID) getString(id uint64) string { return s.id2Str[id] }

func cmpString(a, b string) int {
	if a < b {
		return -1
	} else if a == b {
		return 0
	}
	return 1
}

func NewTagIndex() *TagIndex {
	ti := &TagIndex{
		Tree:        tindex.TreeNew(cmpString),
		metrics:     newStringID(),
		paths:       newStringID(),
		tvs:         newStringID(),
		path2Metric: map[uint64]string{},
	}
	return ti
}

func (ti *TagIndex) Insert(tag, val, metric, path string) {
	tagNode, ok := ti.Get(tag)
	if !ok {
		tagNode = &TagInode{
			Name:   tag,
			Values: tvindex.TreeNew(cmpString),
		}
		ti.Set(tag, tagNode)
	}
	valueNode, ok := tagNode.Values.Get(val)
	if !ok {
		id := ti.tvs.getID(val)
		valueNode = &TagValueInode{
			Value:   val,
			ID:      id,
			Metrics: mindex.TreeNew(cmpString),
		}
		tagNode.Values.Set(val, valueNode)
	}

	mid := ti.metrics.getID(metric)
	pid := ti.paths.getID(path)
	minode, ok := valueNode.Metrics.Get(metric)
	if !ok {
		valueNode.Metrics.Set(metric, &mindex.MetricInode{
			ID: mid, Metric: metric,
			PathIDs: []uint64{pid},
		})
	} else {
		var found bool
		for _, id := range minode.PathIDs {
			if id == pid {
				found = true
			}
		}
		if !found {
			minode.PathIDs = append(minode.PathIDs, pid)
		}
	}
	ti.path2Metric[pid] = metric
}

func (t *TagIndex) ListTags(filter string, limit int) []string {
	enum, err := t.SeekFirst()
	if err != nil {
		return nil
	}
	var result []string
	var count int
	for {
		node, _, err := enum.Next()
		if err != nil {
			break
		}
		result = append(result, node)
		count++
		if count > limit {
			break
		}
	}

	return result
}

type TagStat struct {
	Tag    string
	Values []TagStatValue
}

type TagStatValue struct {
	Count int
	Value string
}

func (t *TagIndex) StatTag(name string, valFilter string, limit int) *TagStat {
	ti, ok := t.Get(name)
	if !ok {
		return nil
	}
	enum, err := ti.Values.SeekFirst()
	if err != nil {
		return nil
	}

	var ts TagStat
	ts.Tag = name
	for {
		val, tv, err := enum.Next()
		if err != nil {
			break
		}
		if valFilter == "" || strings.HasPrefix(val, valFilter) {
			ts.Values = append(ts.Values, TagStatValue{Count: tv.Metrics.Len(), Value: val})
			if len(ts.Values) >= limit {
				break
			}
		}
	}

	return &ts
}

type TagValueExpr struct {
	Tag, Value string
	Op         Op
}

func NewTagValueExpr(expr string) *TagValueExpr {
	var sep Op
	switch {
	case strings.Contains(expr, string(OpNotMatch)):
		sep = OpNotMatch
	case strings.Contains(expr, string(OpMatch)):
		sep = OpMatch
	case strings.Contains(expr, string(OpNotEq)):
		sep = OpNotEq
	case strings.Contains(expr, string(OpEq)):
		sep = OpEq
	}
	vals := strings.Split(expr, string(sep))
	var tve TagValueExpr
	tve.Op = sep
	tve.Tag = vals[0]
	tve.Value = vals[1]
	return &tve
}

type Op string

const (
	OpEq       Op = "="
	OpNotEq    Op = "!="
	OpMatch    Op = "=~"
	OpNotMatch Op = "!=~"
)

type Metric struct {
	Name string
	Path string
}

func (t *TagIndex) ListMetrics(metricExpr *TagValueExpr, tves []*TagValueExpr, limit int) []Metric {
	var tvis [][]*TagValueInode
	for _, tve := range tves {
		tvis = append(tvis, t.findTagValue(tve))
	}

	var metrics []uint64

	joinedTvis := joinTagValueInodes(tvis)
	for _, tvis := range joinedTvis {
		sort.Slice(tvis, func(i, j int) bool { return tvis[i].Metrics.Len() < tvis[j].Metrics.Len() })

		// TODO(xhu): switch to skip list?
		recorder := make(map[uint64]int, tvis[0].Metrics.Len())

		for i, index := range tvis {
			enum, err := index.Metrics.SeekFirst()
			if err != nil {
				continue
			}
			for {
				_, minode, err := enum.Next()
				if err != nil {
					break
				}
				// TODO(xhu): exclude metrics that has tags (empty value in TagValueExpr)
				for _, id := range minode.PathIDs {
					if recorder[id] == i {
						recorder[id] += 1
					}
				}
			}
		}

		for m := range recorder {
			metrics = append(metrics, m)
		}
	}

	result := make([]Metric, 0, len(metrics))
	sort.Slice(metrics, func(i, j int) bool { return metrics[i] < metrics[j] })
	for i, metric := range metrics {
		if i > 0 && metrics[i-1] == metric { // skip duplicated metrics
			continue
		}

		metricStr := t.path2Metric[metric]
		if metricExpr == nil {
			result = append(result, Metric{Name: metricStr, Path: t.paths.getString(metric)})
			continue
		}

		switch metricExpr.Op {
		case OpEq:
			if metricExpr.Value == "" || metricStr == metricExpr.Value {
				result = append(result, Metric{Name: metricStr, Path: t.paths.getString(metric)})
			}
		case OpNotEq:
			if metricStr != metricExpr.Value {
				result = append(result, Metric{Name: metricStr, Path: t.paths.getString(metric)})
			}
		case OpMatch:
			// TODO(xhu)
		case OpNotMatch:
			// TODO(xhu)
		default:
			result = append(result, Metric{Name: metricStr, Path: t.paths.getString(metric)})
		}
	}

	return result
}

func (t *TagIndex) findTagValue(tve *TagValueExpr) []*TagValueInode {
	ti, ok := t.Get(tve.Tag)
	if !ok {
		return nil
	}

	var tvi []*TagValueInode
	enum, err := ti.Values.SeekFirst()
	if err != nil {
		return tvi
	}
	vid, ok := t.tvs.str2ID[tve.Value]
	for {
		_, tv, err := enum.Next()
		if err != nil {
			break
		}
		switch tve.Op {
		case OpEq:
			if !ok {
				break
			}
			if tv.ID == vid {
				tvi = append(tvi, tv)
			}
		case OpNotEq:
			if !ok {
				break
			}
			if tv.ID != vid {
				tvi = append(tvi, tv)
			}
		case OpMatch:
			// TODO(xhu)
		case OpNotMatch:
			// TODO(xhu)
		}
	}

	return tvi
}

func joinTagValueInodes(tvis [][]*TagValueInode) [][]*TagValueInode {
	if len(tvis) == 0 {
		return nil
	}

	jtvis := joinTagValueInodes(tvis[1:])

	var r [][]*TagValueInode
	for _, tvi := range tvis[0] {
		if len(jtvis) == 0 {
			r = append(r, []*TagValueInode{tvi})
		}
		for _, jtvi := range jtvis {
			r = append(r, append([]*TagValueInode{tvi}, jtvi...))
		}
	}

	return r
}
