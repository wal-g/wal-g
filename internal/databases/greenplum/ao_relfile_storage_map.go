package greenplum

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewAoRelFileStorageMap(ctx context.Context, queryRunner *GpQueryRunner) (ao.RelFileStorageMap, error) {
	databases, err := queryRunner.GetDatabaseInfos(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get database names")
	}

	result := make(ao.RelFileStorageMap)
	// collect info for each relFileNode
	for _, db := range databases {
		dbName := db.Name
		databaseOption := func(c *pgx.ConnConfig) error {
			c.Database = dbName
			return nil
		}

		dbConn, err := postgres.Connect(ctx, databaseOption)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to connect to database: %s\n'%v'\n", db.Name, err)
			continue
		}

		queryRunner, err := NewGpQueryRunner(ctx, dbConn)
		if err != nil {
			return nil, errors.Wrap(err, "failed to build query runner.")
		}
		rows, err := queryRunner.FetchAOStorageMetadata(ctx, db)
		if err != nil {
			tracelog.WarningLogger.Printf("failed to fetch storage types: %s\n'%v'\n", db.Name, err)
			continue
		}
		tracelog.InfoLogger.Printf("Successfully loaded AO/AOCS metadata about %d relations in database %s\n", len(rows), db.Name)
		for relFileLoc, metadata := range rows {
			result[relFileLoc] = metadata
		}
		err = dbConn.Close(ctx)
		tracelog.WarningLogger.PrintOnError(err)
	}
	return result, nil
}
