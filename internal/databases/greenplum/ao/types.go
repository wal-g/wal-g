package ao

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/walparser"
)

type RelStorageType byte

const (
	AppendOptimized RelStorageType = 'a'
	ColumnOriented  RelStorageType = 'c'
)

func NewRelFileMetadata(relNameMd5 string, storageType RelStorageType, eof, modCount int64) RelFileMetadata {
	return RelFileMetadata{
		relNameMd5:  relNameMd5,
		storageType: storageType,
		eof:         eof,
		modCount:    modCount,
	}
}

type RelFileMetadata struct {
	relNameMd5  string
	storageType RelStorageType
	eof         int64
	modCount    int64
}

// RelFileStorageMap indicates the storage type for the relfile.
type RelFileStorageMap map[walparser.BlockLocation]RelFileMetadata

func (storageMap RelFileStorageMap) Lookup(filePath string) (bool, RelFileMetadata, *walparser.BlockLocation) {
	relFileNode, err := postgres.GetRelFileNodeFrom(filePath)
	if err != nil {
		// Looks like this is not a relfile at all.
		return false, RelFileMetadata{}, nil
	}
	blockNo, err := postgres.GetRelFileIDFrom(filePath)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to parse blockNo for path %s: %v", filePath, err)
		return false, RelFileMetadata{}, nil
	}

	location := walparser.NewBlockLocation(relFileNode.SpcNode, relFileNode.DBNode, relFileNode.RelNode, uint32(blockNo))
	storageInfo, ok := storageMap[*location]
	if !ok {
		// Absence of the entry does not guarantee that the relfile is not append-optimized.
		// It may have been created after the backup start. Currently, we do not need to
		// detect an AO file with 100% precision, so it is OK.
		return false, RelFileMetadata{}, nil
	}

	return true, storageInfo, location
}
