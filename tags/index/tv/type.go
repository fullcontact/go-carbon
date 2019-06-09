package tag_value_index

import mindex "github.com/lomik/go-carbon/tags/index/metric"

type TagValueInode struct {
	Value string
	ID    uint64
	// metrics *btree.BTree // MetricInode
	Metrics *mindex.Tree
}
