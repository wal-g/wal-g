package binary

import (
	"container/heap"
	"github.com/wal-g/tracelog"

	"github.com/mongodb/mongo-tools/common/util"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/partial"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type NsInfo struct {
	Ns           string `bson:"ns"`
	StorageStats struct {
		TotalSize  int64 `bson:"totalSize"`
		WiredTiger struct {
			URI string `bson:"uri"`
		} `bson:"wiredTiger"`
		IndexDetails map[string]struct {
			URI string `bson:"uri"`
		} `bson:"indexDetails"`
	} `bson:"storageStats"`
}

type StorageMetadataCollector struct {
	mongodService *MongodService
	routes        *models.BackupRoutesInfo
	heap          *NsSizeHeap
	counter       int
	top100Ns      *[]string
	TarsChan      chan internal.TarFileSets
	ErrsChan      chan error
	onComplete    func(routes *models.BackupRoutesInfo) error
}

func NewStorageMetadataCollector(
	mongodService *MongodService,
	onComplete func(routes *models.BackupRoutesInfo) error,
) *StorageMetadataCollector {
	h := &NsSizeHeap{}
	heap.Init(h)

	return &StorageMetadataCollector{
		mongodService: mongodService,
		routes:        models.NewBackupRoutesInfo(),
		heap:          h,
		TarsChan:      make(chan internal.TarFileSets),
		ErrsChan:      make(chan error),
		onComplete:    onComplete,
	}
}

func (smc *StorageMetadataCollector) GetStats() {
	pipeline := mongo.Pipeline{
		{{Key: "$_internalAllCollectionStats", Value: bson.D{
			{Key: "stats", Value: bson.D{
				{Key: "storageStats", Value: bson.D{}},
			}},
		}}},
	}

	cursor, err := smc.mongodService.MongoClient.Database(adminDB).Aggregate(smc.mongodService.Context, pipeline)
	if err != nil {
		smc.ErrsChan <- err
		return
	}
	defer cursor.Close(smc.mongodService.Context)

	for cursor.TryNext(smc.mongodService.Context) {
		var nsInfo NsInfo
		if err = cursor.Decode(&nsInfo); err != nil {
			smc.ErrsChan <- err
			return
		}
		tracelog.DebugLogger.Printf("NsInfo: %v", nsInfo)

		smc.handleTop100Info(&nsInfo)
		smc.handleRoutesInfo(&nsInfo)
	}

	if err := cursor.Err(); err != nil {
		smc.ErrsChan <- err
		return
	}

	top100Ns := make([]string, smc.heap.Len())
	for i := smc.heap.Len() - 1; i >= 0; i-- {
		top100Ns[i] = heap.Pop(smc.heap).(CollStats).NS
	}

	smc.top100Ns = &top100Ns

	tarsFileSet := <-smc.TarsChan
	if err := partial.EnrichWithTarPaths(smc.routes, tarsFileSet.Get()); err != nil {
		smc.ErrsChan <- err
		return
	}

	smc.ErrsChan <- smc.onComplete(smc.routes)
}

func (smc *StorageMetadataCollector) handleRoutesInfo(nsInfo *NsInfo) {
	db, col := util.SplitNamespace(nsInfo.Ns)

	indexInfo := make(models.IndexInfo)
	for index, details := range nsInfo.StorageStats.IndexDetails {
		if len(details.URI) == 0 {
			continue
		}
		indexInfo[index] = models.Paths{DBPath: convertToFile(localPathFromURI(details.URI))}
	}

	colInfo := models.CollectionInfo{
		Paths:     models.Paths{DBPath: convertToFile(localPathFromURI(nsInfo.StorageStats.WiredTiger.URI))},
		IndexInfo: indexInfo,
	}

	if _, ok := smc.routes.Databases[db]; !ok {
		smc.routes.Databases[db] = make(models.DBInfo)
	}

	smc.routes.Databases[db][col] = colInfo
}

func (smc *StorageMetadataCollector) handleTop100Info(nsInfo *NsInfo) {
	db, _ := util.SplitNamespace(nsInfo.Ns)
	if _, ok := systemDBs[db]; ok {
		return
	}
	smc.counter++

	if smc.heap.Len() < topK {
		heap.Push(smc.heap, CollStats{NS: nsInfo.Ns, Size: nsInfo.StorageStats.TotalSize})
	} else if nsInfo.StorageStats.TotalSize > (*smc.heap)[0].Size {
		heap.Pop(smc.heap)
		heap.Push(smc.heap, CollStats{NS: nsInfo.Ns, Size: nsInfo.StorageStats.TotalSize})
	}
}
