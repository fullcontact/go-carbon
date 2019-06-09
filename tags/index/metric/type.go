package metric_index

import "fmt"

func (t *Tree) String() string {
	return fmt.Sprintf("metric{len: %d}", t.Len())
}

type MetricInode struct {
	ID      uint64
	Metric  string
	PathIDs []uint64
}
