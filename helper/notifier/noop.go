package notifier

import (
	"context"
	)

type Noop struct {
}

func NewNoop(size uint64) Notifier {
	r := &Noop{}
	return r
}

func (t *Noop) GetNewMetrics(ctx context.Context, id uint64) ([]string, uint64) {
	return nil, 0
}

func (t *Noop) Push(metric string) {
}
