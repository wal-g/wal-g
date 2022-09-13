package greenplum

import (
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/walparser"
)

type RelStorageType byte

const (
	AppendOptimized RelStorageType = 'a'
	ColumnOriented  RelStorageType = 'c'
)

type AoRelFileMetadata struct {
	relNameMd5  string
	storageType RelStorageType
	eof         int64
	modCount    int64
}

// AoRelFileStorageMap indicates the storage type for the relfile
type AoRelFileStorageMap map[walparser.BlockLocation]AoRelFileMetadata

func (storageMap *AoRelFileStorageMap) getAOStorageMetadata(filePath string) (bool, AoRelFileMetadata, *walparser.BlockLocation) {
	relFileNode, err := postgres.GetRelFileNodeFrom(filePath)
	if err != nil {
		// looks like this is not the relfile at all => false
		return false, AoRelFileMetadata{}, nil
	}
	blockNo, err := postgres.GetRelFileIDFrom(filePath)
	if err != nil {
		// same as above, but this is some unusual / unexpected error, better log it
		tracelog.WarningLogger.Printf("Failed to parse blockNo for path %s: %v", filePath, err)
		return false, AoRelFileMetadata{}, nil
	}

	location := walparser.NewBlockLocation(relFileNode.SpcNode, relFileNode.DBNode, relFileNode.RelNode, uint32(blockNo))
	storageInfo, ok := (*storageMap)[*location]
	if !ok {
		// Absence of the entry does not guarantee that the relfile is not append-optimized.
		// It may have been created after the backup start.
		// Currently, we do not need to detect an AO file with 100% precision, so it is OK.
		return false, AoRelFileMetadata{}, nil
	}

	return true, storageInfo, location
}

func NewAoRelFileStorageMap(queryRunner *GpQueryRunner) (AoRelFileStorageMap, error) {
	databases, err := queryRunner.GetDatabaseInfos()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get database names")
	}

	result := make(AoRelFileStorageMap)
	// collect info for each relFileNode
	for _, db := range databases {
		dbName := db.Name
		databaseOption := func(c *pgx.ConnConfig) error {
			c.Database = dbName
			return nil
		}

		dbConn, err := postgres.Connect(databaseOption)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to connect to database: %s\n'%v'\n", db.Name, err)
			continue
		}

		queryRunner, err := NewGpQueryRunner(dbConn)
		if err != nil {
			return nil, errors.Wrap(err, "failed to build query runner.")
		}
		rows, err := queryRunner.FetchAOStorageMetadata(db)
		if err != nil {
			tracelog.WarningLogger.Printf("failed to fetch storage types: %s\n'%v'\n", db.Name, err)
			continue
		}
		tracelog.InfoLogger.Printf("Successfully loaded AO/AOCS metadata about %d relations in database %s\n", len(rows), db.Name)
		for relFileLoc, metadata := range rows {
			result[relFileLoc] = metadata
		}
		err = dbConn.Close()
		tracelog.WarningLogger.PrintOnError(err)
	}
	return result, nil
}
