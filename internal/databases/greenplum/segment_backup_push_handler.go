package greenplum

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewSegBackupHandler(arguments postgres.BackupArguments) (*postgres.BackupHandler, error) {
	// Segments need special connection handling for GP utility mode
	// Get server info using GP segment connection
	pgInfo, _, err := GetSegmentServerInfo(false)
	if err != nil {
		return nil, err
	}

	bh := &postgres.BackupHandler{
		Arguments: arguments,
		PgInfo:    pgInfo,
	}

	// Continue with original segment-specific setup
	configureSegmentBackupHandler(bh)

	return bh, nil
}

// GetSegmentServerInfo gets Postgres server info using GP segment connection logic
func GetSegmentServerInfo(keepRunner bool) (pgInfo postgres.BackupPgInfo, runner *postgres.PgQueryRunner, err error) {
	tracelog.DebugLogger.Println("Initializing tmp connection to read GP segment info")
	tmpConn, err := ConnectSegment()
	if err != nil {
		return pgInfo, nil, err
	}

	// Use the postgres helper function with our GP-specific connection
	return postgres.GetPgServerInfoWithConnection(tmpConn, keepRunner)
}

func configureSegmentBackupHandler(bh *postgres.BackupHandler) {

	composerInitFunc := func(handler *postgres.BackupHandler) error {
		queryRunner := ToGpQueryRunner(handler.Workers.QueryRunner)
		relStorageMap, err := NewAoRelFileStorageMap(queryRunner)
		if err != nil {
			return err
		}

		maker, err := NewGpTarBallComposerMaker(relStorageMap, bh.Arguments.Uploader, handler.CurBackupInfo.Name)
		if err != nil {
			return err
		}

		return bh.Workers.Bundle.SetupComposer(maker)
	}

	bh.SetComposerInitFunc(composerInitFunc)

	if bh.PgInfo.PgVersion < 100000 {
		tracelog.DebugLogger.Printf("Query runner version is %d, disabling concurrent backups", bh.PgInfo.PgVersion)
		bh.Arguments.EnablePreventConcurrentBackups()
	}
}
