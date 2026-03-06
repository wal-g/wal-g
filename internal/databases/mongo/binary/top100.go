package binary

import (
	"container/heap"
	"context"

	"github.com/mongodb/mongo-tools/common/util"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"go.mongodb.org/mongo-driver/mongo"
)

const topK int = 100

type NsSizeHeap []models.CollStats

func (h NsSizeHeap) Len() int { return len(h) }
func (h NsSizeHeap) Less(i, j int) bool {
	return h[i].StorageStats.TotalSize < h[j].StorageStats.TotalSize
}
func (h NsSizeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *NsSizeHeap) Push(x any) {
	*h = append(*h, x.(models.CollStats))
}
func (h *NsSizeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func SortNamespacesWithHeap(ctx context.Context, cursor *mongo.Cursor) ([]string, int, error) {
	var counter int
	h := &NsSizeHeap{}
	heap.Init(h)

	for cursor.TryNext(ctx) {
		var doc models.CollStats
		if err := cursor.Decode(&doc); err != nil {
			return nil, 0, err
		}

		db, _ := util.SplitNamespace(doc.NS)
		if _, ok := systemDBs[db]; ok {
			continue
		}
		counter++

		if h.Len() < topK {
			heap.Push(h, doc)
		} else if doc.StorageStats.TotalSize > (*h)[0].StorageStats.TotalSize {
			heap.Pop(h)
			heap.Push(h, doc)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, 0, err
	}

	res := make([]string, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		res[i] = heap.Pop(h).(models.CollStats).NS
	}

	return res, counter, nil
}
