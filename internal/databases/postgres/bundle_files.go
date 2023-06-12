package postgres

import (
	"archive/tar"
	"os"
	"sync"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/internal/walparser"
)

func newStatBundleFiles(fileStat RelFileStatistics) *StatBundleFiles {
	return &StatBundleFiles{fileStats: fileStat}
}

// StatBundleFiles contains the bundle files.
// Additionally, it calculates and stores the updates count for each added file
type StatBundleFiles struct {
	sync.Map
	fileStats RelFileStatistics
}

func (files *StatBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header,
	fileInfo os.FileInfo,
	isIncremented bool,
	corruptedBlocks []uint32,
	storeAllBlocks bool) {
	updatesCount := files.fileStats.getFileUpdateCount(tarHeader.Name)
	fileDescription := internal.BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime(),
		UpdatesCount: updatesCount}
	fileDescription.SetCorruptBlocks(corruptedBlocks, storeAllBlocks)
	files.AddFileDescription(tarHeader.Name, fileDescription)
}

func (files *StatBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	updatesCount := files.fileStats.getFileUpdateCount(tarHeader.Name)
	files.AddFileDescription(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: true, IsIncremented: false,
			MTime: fileInfo.ModTime(), UpdatesCount: updatesCount})
}

func (files *StatBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	updatesCount := files.fileStats.getFileUpdateCount(tarHeader.Name)
	files.AddFileDescription(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented,
			MTime: fileInfo.ModTime(), UpdatesCount: updatesCount})
}

func (files *StatBundleFiles) AddFileDescription(name string, backupFileDescription internal.BackupFileDescription) {
	files.Store(name, backupFileDescription)
}

func (files *StatBundleFiles) GetUnderlyingMap() *sync.Map {
	return &files.Map
}

type RelFileStatistics map[walparser.RelFileNode]PgRelationStat

func (relStat *RelFileStatistics) getFileUpdateCount(filePath string) uint64 {
	if relStat == nil {
		return 0
	}
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		return 0
	}
	fileStat, ok := (*relStat)[*relFileNode]
	if !ok {
		return 0
	}
	return fileStat.deletedTuplesCount + fileStat.updatedTuplesCount + fileStat.insertedTuplesCount
}

func newRelFileStatistics(queryRunner *PgQueryRunner) (RelFileStatistics, error) {
	result := make(map[walparser.RelFileNode]PgRelationStat)

	err := queryRunner.ForEachDatabase(func(currentRunner *PgQueryRunner, db PgDatabaseInfo) error {
		pgStatRows, err := currentRunner.getStatistics(db)
		if err != nil {
			return err
		}
		for relFileNode, statRow := range pgStatRows {
			result[relFileNode] = statRow
		}
		return nil
	})

	return result, err
}
