package greenplum

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// NewPaxRelFileStorageMap walks every database on the segment, queries the PAX aux
// catalog (`pg_ext_aux.pg_pax_tables` / `pg_pax_blocks_*`), and assembles the master
// map of "this file is a PAX file" lookups for the file walker.
//
// PAX is a Cloudberry-only access method, so for plain Greenplum and unknown flavors
// the function short-circuits to an empty map without contacting the catalog.
func NewPaxRelFileStorageMap(ctx context.Context, queryRunner *GpQueryRunner) (pax.RelFileStorageMap, error) {
	versionStr, err := queryRunner.GetGreenplumVersion(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query greenplum version")
	}
	version := ParseVersionInfo(versionStr)
	if !version.IsCBDB() {
		tracelog.DebugLogger.Printf("Skipping PAX storage map: flavor=%s does not support PAX", NewFlavor(version.Type))
		return pax.RelFileStorageMap{}, nil
	}

	databases, err := queryRunner.GetDatabaseInfos(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get database names")
	}

	result := make(pax.RelFileStorageMap)
	for _, db := range databases {
		dbName := db.Name
		databaseOption := func(c *pgx.ConnConfig) error {
			c.Database = dbName
			return nil
		}
		dbConn, err := postgres.Connect(ctx, databaseOption)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to connect to database %s: %v", dbName, err)
			continue
		}

		entries, err := pax.FetchStorageMetadata(ctx, dbConn, db)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to fetch PAX storage metadata for %s: %v", dbName, err)
			closeErr := dbConn.Close(ctx)
			tracelog.WarningLogger.PrintOnError(closeErr)
			continue
		}
		tracelog.InfoLogger.Printf("Loaded PAX metadata for %d files in database %s", len(entries), dbName)

		for k, v := range entries {
			result[k] = v
		}

		closeErr := dbConn.Close(ctx)
		tracelog.WarningLogger.PrintOnError(closeErr)
	}
	return result, nil
}
