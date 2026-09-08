package binary

import (
	"container/heap"
	"context"

	"github.com/mongodb/mongo-tools/common/util"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type StorageMetadataCollector struct {
	mongodService *MongodService
	pmc           *PartialMetadataCollector
	heap          *NsSizeHeap
	counter       int64
	top100Ns      *[]string
	TarsChan      chan internal.TarFileSets
	ErrsChan      chan error
	onComplete    func(routes *models.BackupRoutesInfo) error
	systemDbs     *map[string]struct{}
}

func NewStorageMetadataCollector(
	mongodService *MongodService,
	onComplete func(routes *models.BackupRoutesInfo) error,
) *StorageMetadataCollector {
	h := &NsSizeHeap{}
	heap.Init(h)

	return &StorageMetadataCollector{
		mongodService: mongodService,
		pmc:           NewPartialMetadataCollector(),
		heap:          h,
		TarsChan:      make(chan internal.TarFileSets),
		ErrsChan:      make(chan error, 1),
		onComplete:    onComplete,
		systemDbs:     common.SystemDBs(),
	}
}

func (smc *StorageMetadataCollector) GetStats() {
	smc.ErrsChan <- smc.getStats()
}

func (smc *StorageMetadataCollector) Complete(ctx context.Context, tarFileSets internal.TarFileSets) error {
	select {
	case smc.TarsChan <- tarFileSets:
	case err := <-smc.ErrsChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-smc.ErrsChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (smc *StorageMetadataCollector) getStats() error {
	pipeline := mongo.Pipeline{
		{{Key: "$_internalAllCollectionStats", Value: bson.D{
			{Key: "stats", Value: bson.D{
				{Key: "storageStats", Value: bson.D{}},
			}},
		}}},
	}

	cursor, err := smc.mongodService.MongoClient.Database(adminDB).Aggregate(smc.mongodService.Context, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(smc.mongodService.Context)

	for cursor.TryNext(smc.mongodService.Context) {
		var nsInfo models.NsInfo
		if err = cursor.Decode(&nsInfo); err != nil {
			return err
		}

		smc.handleTop100Info(&nsInfo)
		smc.pmc.HandleNsInfo(&nsInfo)
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	top100Ns := make([]string, smc.heap.Len())
	for i := smc.heap.Len() - 1; i >= 0; i-- {
		top100Ns[i] = heap.Pop(smc.heap).(CollStats).NS
	}

	smc.top100Ns = &top100Ns

	var tarsFileSet internal.TarFileSets
	select {
	case tarsFileSet = <-smc.TarsChan:
	case <-smc.mongodService.Context.Done():
		return smc.mongodService.Context.Err()
	}
	if err := smc.pmc.EnrichWithTarPaths(tarsFileSet.Get()); err != nil {
		return err
	}

	return smc.onComplete(smc.pmc.GetRoutes())
}

func (smc *StorageMetadataCollector) handleTop100Info(nsInfo *models.NsInfo) {
	db, _ := util.SplitNamespace(nsInfo.Ns)
	if _, ok := (*smc.systemDbs)[db]; ok {
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
