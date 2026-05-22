package binary

import (
	"container/heap"
	"context"
	"fmt"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestNewStorageMetadataCollector_Initialization(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	onComplete := func(routes *models.BackupRoutesInfo) error { return nil }

	collector := NewStorageMetadataCollector(svc, onComplete)

	assert.NotNil(t, collector)
	assert.NotNil(t, collector.pmc)
	assert.NotNil(t, collector.heap)
	assert.NotNil(t, collector.TarsChan)
	assert.NotNil(t, collector.ErrsChan)
	assert.Equal(t, 0, collector.heap.Len())
	assert.Equal(t, int64(0), collector.counter)
	assert.Nil(t, collector.top100Ns)
}

func TestHandleTop100Info_SystemDBsSkipped(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for db := range *common.SystemDBs() {
		nsInfo := &models.NsInfo{Ns: db + ".somecol"}
		nsInfo.StorageStats.TotalSize = 1000
		collector.handleTop100Info(nsInfo)
	}

	assert.Equal(t, int64(0), collector.counter, "system DBs should be skipped")
	assert.Equal(t, 0, collector.heap.Len(), "heap should be empty for system DBs")
}

func TestHandleTop100Info_FillHeapUnderTopK(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK-1; i++ {
		nsInfo := &models.NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(i * 100)
		collector.handleTop100Info(nsInfo)
	}

	assert.Equal(t, int64(topK-1), collector.counter)
	assert.Equal(t, topK-1, collector.heap.Len())
}

func TestHandleTop100Info_FillHeapExactlyTopK(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &models.NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(i * 100)
		collector.handleTop100Info(nsInfo)
	}

	assert.Equal(t, int64(topK), collector.counter)
	assert.Equal(t, topK, collector.heap.Len())
}

func TestHandleTop100Info_ReplaceSmallestWhenFull(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &models.NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(i + 1)
		collector.handleTop100Info(nsInfo)
	}

	bigNsInfo := &models.NsInfo{Ns: "userdb.bigcol"}
	bigNsInfo.StorageStats.TotalSize = 999999
	collector.handleTop100Info(bigNsInfo)

	assert.Equal(t, int64(topK+1), collector.counter)
	assert.Equal(t, topK, collector.heap.Len())

	assert.True(t, slices.ContainsFunc(*collector.heap, func(item CollStats) bool {
		return item.NS == "userdb.bigcol"
	}), "big collection should be in top-K heap")
}

func TestHandleTop100Info_SmallElementNotReplaced(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &models.NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(1000 * (i + 1))
		collector.handleTop100Info(nsInfo)
	}

	minBefore := (*collector.heap)[0].Size

	smallNsInfo := &models.NsInfo{Ns: "userdb.smallcol"}
	smallNsInfo.StorageStats.TotalSize = 1
	collector.handleTop100Info(smallNsInfo)

	assert.Equal(t, topK, collector.heap.Len())
	assert.Equal(t, minBefore, (*collector.heap)[0].Size)

	for _, item := range *collector.heap {
		assert.NotEqual(t, "userdb.smallcol", item.NS)
	}
}

func TestHandleTop100Info_EqualToMinNotReplaced(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &models.NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(100 * (i + 1))
		collector.handleTop100Info(nsInfo)
	}

	minBefore := (*collector.heap)[0].Size

	equalNsInfo := &models.NsInfo{Ns: "userdb.equalcol"}
	equalNsInfo.StorageStats.TotalSize = minBefore
	collector.handleTop100Info(equalNsInfo)

	assert.Equal(t, topK, collector.heap.Len())
}

func TestNsSizeHeap_IsMinHeap(t *testing.T) {
	h := &NsSizeHeap{}
	heap.Init(h)

	items := []CollStats{
		{NS: "db.col3", Size: 300},
		{NS: "db.col1", Size: 100},
		{NS: "db.col2", Size: 200},
	}

	for _, item := range items {
		heap.Push(h, item)
	}

	assert.Equal(t, int64(100), (*h)[0].Size, "min-heap: smallest element should be at root")
}

func TestNsSizeHeap_PopReturnsAscending(t *testing.T) {
	h := &NsSizeHeap{}
	heap.Init(h)

	sizes := []int64{500, 100, 300, 200, 400}
	for i, size := range sizes {
		heap.Push(h, CollStats{NS: fmt.Sprintf("db.col%d", i), Size: size})
	}

	prev := int64(0)
	for h.Len() > 0 {
		item := heap.Pop(h).(CollStats)
		assert.GreaterOrEqual(t, item.Size, prev, "pop should return elements in ascending order")
		prev = item.Size
	}
}

func TestNsSizeHeap_EmptyLen(t *testing.T) {
	h := &NsSizeHeap{}
	heap.Init(h)
	assert.Equal(t, 0, h.Len())
}

func TestNsSizeHeap_SingleElement(t *testing.T) {
	h := &NsSizeHeap{}
	heap.Init(h)

	heap.Push(h, CollStats{NS: "db.col1", Size: 42})
	assert.Equal(t, 1, h.Len())

	item := heap.Pop(h).(CollStats)
	assert.Equal(t, "db.col1", item.NS)
	assert.Equal(t, int64(42), item.Size)
	assert.Equal(t, 0, h.Len())
}

func TestNsSizeHeap_DuplicateSizes(t *testing.T) {
	h := &NsSizeHeap{}
	heap.Init(h)

	for i := 0; i < 5; i++ {
		heap.Push(h, CollStats{NS: fmt.Sprintf("db.col%d", i), Size: 100})
	}

	assert.Equal(t, 5, h.Len())
	for h.Len() > 0 {
		item := heap.Pop(h).(CollStats)
		assert.Equal(t, int64(100), item.Size)
	}
}
