package notifier

// should be something like singleton
type Notifier interface {
	// requires id from which you want to get metrics. 0 for "from the beginning"
	// returns
	// 1. id of last entry
	// 2. list of metrics
	// 3. true if set is incomplete and we need to do a full scan
	GetNewMetrics(id int) (int, []string, bool)
	// NonBlocking version of previous call, returns same ID and empty string if there is nothing to return
	GetNewMetricsNonBlocking(id int) (int, []string, bool)
}
