package greenplum

import (
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewSegBackupHandler(arguments postgres.BackupArguments) (*postgres.BackupHandler, error) {
	bh, err := postgres.NewBackupHandler(arguments)
	if err != nil {
		return nil, err
	}

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

	return bh, err
}
