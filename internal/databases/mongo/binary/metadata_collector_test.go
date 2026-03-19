package binary

import (
	"container/heap"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestNewStorageMetadataCollector_Initialization(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	onComplete := func(routes *models.BackupRoutesInfo) error { return nil }

	collector := NewStorageMetadataCollector(svc, onComplete)

	assert.NotNil(t, collector)
	assert.NotNil(t, collector.routes)
	assert.NotNil(t, collector.heap)
	assert.NotNil(t, collector.TarsChan)
	assert.NotNil(t, collector.ErrsChan)
	assert.Equal(t, 0, collector.heap.Len())
	assert.Equal(t, 0, collector.counter)
	assert.Nil(t, collector.top100Ns)
}

func TestHandleRoutesInfo_BasicCollection(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	nsInfo := &NsInfo{Ns: "testdb.testcol"}
	nsInfo.StorageStats.WiredTiger.URI = "statistics:table:test/collection-140-3211222180999241170"
	nsInfo.StorageStats.IndexDetails = map[string]struct {
		URI string `bson:"uri"`
	}{
		"_id_": {URI: "statistics:table:test/index-141-3211222180999241170"},
	}

	collector.handleRoutesInfo(nsInfo)

	dbInfo, ok := collector.routes.Databases["testdb"]
	assert.True(t, ok, "database 'testdb' should exist in routes")

	colInfo, ok := dbInfo["testcol"]
	assert.True(t, ok, "collection 'testcol' should exist in routes")

	assert.NotEmpty(t, colInfo.Paths.DBPath)
	assert.Contains(t, colInfo.IndexInfo, "_id_")
	assert.NotEmpty(t, colInfo.IndexInfo["_id_"].DBPath)
}

func TestHandleRoutesInfo_EmptyIndexURI(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	nsInfo := &NsInfo{Ns: "testdb.testcol"}
	nsInfo.StorageStats.WiredTiger.URI = "statistics:table:test/collection-140-3211222180999241170"
	nsInfo.StorageStats.IndexDetails = map[string]struct {
		URI string `bson:"uri"`
	}{
		"_id_": {URI: ""},
	}

	collector.handleRoutesInfo(nsInfo)

	colInfo := collector.routes.Databases["testdb"]["testcol"]
	assert.NotContains(t, colInfo.IndexInfo, "_id_", "index with empty URI should be skipped")
}

func TestHandleRoutesInfo_MultipleCollectionsSameDB(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	nsInfo1 := &NsInfo{Ns: "testdb.col1"}
	nsInfo1.StorageStats.WiredTiger.URI = "statistics:table:test/collection-140-3211222180999241170"

	nsInfo2 := &NsInfo{Ns: "testdb.col2"}
	nsInfo2.StorageStats.WiredTiger.URI = "statistics:table:test/collection-142-3211222180999241170"

	collector.handleRoutesInfo(nsInfo1)
	collector.handleRoutesInfo(nsInfo2)

	dbInfo := collector.routes.Databases["testdb"]
	assert.Contains(t, dbInfo, "col1")
	assert.Contains(t, dbInfo, "col2")
}

func TestHandleRoutesInfo_MultipleDatabases(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	nsInfo1 := &NsInfo{Ns: "db1.col1"}
	nsInfo1.StorageStats.WiredTiger.URI = "statistics:table:db1/collection-140-3211222180999241170"

	nsInfo2 := &NsInfo{Ns: "db2.col1"}
	nsInfo2.StorageStats.WiredTiger.URI = "statistics:table:db2/collection-142-3211222180999241170"

	collector.handleRoutesInfo(nsInfo1)
	collector.handleRoutesInfo(nsInfo2)

	assert.Contains(t, collector.routes.Databases, "db1")
	assert.Contains(t, collector.routes.Databases, "db2")
}

func TestHandleRoutesInfo_NoIndexDetails(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	nsInfo := &NsInfo{Ns: "testdb.testcol"}
	nsInfo.StorageStats.WiredTiger.URI = "statistics:table:testdb/collection-142-3211222180999241170"
	nsInfo.StorageStats.IndexDetails = nil

	collector.handleRoutesInfo(nsInfo)

	colInfo := collector.routes.Databases["testdb"]["testcol"]
	assert.Empty(t, colInfo.IndexInfo)
}

func TestHandleRoutesInfo_MultipleIndexes(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	nsInfo := &NsInfo{Ns: "testdb.testcol"}
	nsInfo.StorageStats.WiredTiger.URI = "statistics:table:testdb/collection-142-3211222180999241170"
	nsInfo.StorageStats.IndexDetails = map[string]struct {
		URI string `bson:"uri"`
	}{
		"_id_":     {URI: "statistics:table:testdb/index-144-3211222180999241170"},
		"name_1":   {URI: "statistics:table:testdb/index-146-3211222180999241170"},
		"empty_ix": {URI: ""},
	}

	collector.handleRoutesInfo(nsInfo)

	colInfo := collector.routes.Databases["testdb"]["testcol"]
	assert.Contains(t, colInfo.IndexInfo, "_id_")
	assert.Contains(t, colInfo.IndexInfo, "name_1")
	assert.NotContains(t, colInfo.IndexInfo, "empty_ix")
}

// --- Тесты handleTop100Info ---

func TestHandleTop100Info_SystemDBsSkipped(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for db := range systemDBs {
		nsInfo := &NsInfo{Ns: db + ".somecol"}
		nsInfo.StorageStats.TotalSize = 1000
		collector.handleTop100Info(nsInfo)
	}

	assert.Equal(t, 0, collector.counter, "system DBs should be skipped")
	assert.Equal(t, 0, collector.heap.Len(), "heap should be empty for system DBs")
}

func TestHandleTop100Info_FillHeapUnderTopK(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK-1; i++ {
		nsInfo := &NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(i * 100)
		collector.handleTop100Info(nsInfo)
	}

	assert.Equal(t, topK-1, collector.counter)
	assert.Equal(t, topK-1, collector.heap.Len())
}

func TestHandleTop100Info_FillHeapExactlyTopK(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(i * 100)
		collector.handleTop100Info(nsInfo)
	}

	assert.Equal(t, topK, collector.counter)
	assert.Equal(t, topK, collector.heap.Len())
}

func TestHandleTop100Info_ReplaceSmallestWhenFull(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(i + 1)
		collector.handleTop100Info(nsInfo)
	}

	bigNsInfo := &NsInfo{Ns: "userdb.bigcol"}
	bigNsInfo.StorageStats.TotalSize = 999999
	collector.handleTop100Info(bigNsInfo)

	assert.Equal(t, topK+1, collector.counter)
	assert.Equal(t, topK, collector.heap.Len())

	found := false
	for _, item := range *collector.heap {
		if item.NS == "userdb.bigcol" {
			found = true
			break
		}
	}
	assert.True(t, found, "big collection should be in top-K heap")
}

func TestHandleTop100Info_SmallElementNotReplaced(t *testing.T) {
	svc := &MongodService{Context: context.Background()}
	collector := NewStorageMetadataCollector(svc, func(routes *models.BackupRoutesInfo) error {
		return nil
	})

	for i := 0; i < topK; i++ {
		nsInfo := &NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(1000 * (i + 1))
		collector.handleTop100Info(nsInfo)
	}

	minBefore := (*collector.heap)[0].Size

	smallNsInfo := &NsInfo{Ns: "userdb.smallcol"}
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
		nsInfo := &NsInfo{Ns: fmt.Sprintf("userdb.col%d", i)}
		nsInfo.StorageStats.TotalSize = int64(100 * (i + 1))
		collector.handleTop100Info(nsInfo)
	}

	minBefore := (*collector.heap)[0].Size

	// Элемент равный минимуму — не должен заменять
	equalNsInfo := &NsInfo{Ns: "userdb.equalcol"}
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
