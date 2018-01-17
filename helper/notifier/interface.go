package notifier

import (
	"context"
)

// should be something like singleton
type Notifier interface {
	// requires id from which you want to get metrics. 0 for "from the beginning"
	// returns
	// 1. id of last entry
	// 2. list of metrics
	// To compute that we received full set, you should do diff between len(return), id and compare thatn to newId.
	GetNewMetrics(ctx context.Context, id uint64) ([]string, uint64)
	Push(metric string)
}
