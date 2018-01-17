package notifier

import (
	"context"

	"github.com/lomik/tail"
)

type Tail struct {
	tail tail.Tail
}

func NewTail(size uint64) Notifier {
	t := tail.New(size)
	tail := &Tail{
		tail: t,
	}
	return tail
}

func (t *Tail) GetNewMetrics(ctx context.Context, id uint64) ([]string, uint64) {
	m, newID := t.tail.Get(ctx, id, 0)
	res := make([]string, len(m))
	for i := range m {
		res[i] = m[i].(string)
	}

	return res, newID
}

func (t *Tail) Push(metric string) {
	t.tail.Push(metric)
}
