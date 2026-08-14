package greenplum

import (
	"context"
	"sync/atomic"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewSegBackupHandler(ctx context.Context, arguments postgres.BackupArguments) (*postgres.BackupHandler, error) {
	bh, err := postgres.NewBackupHandler(ctx, arguments)
	if err != nil {
		return nil, err
	}

	// Filled by the composer at the end of the backup, read afterwards when the journal is written.
	sharedSize := new(atomic.Int64)

	composerInitFunc := func(ctx context.Context, handler *postgres.BackupHandler) error {
		queryRunner := ToGpQueryRunner(handler.Workers.QueryRunner)
		relStorageMap, err := NewAoRelFileStorageMap(ctx, queryRunner)
		if err != nil {
			return err
		}

		paxRelStorageMap, err := NewPaxRelFileStorageMap(ctx, queryRunner)
		if err != nil {
			return err
		}

		maker, err := NewGpTarBallComposerMaker(relStorageMap, paxRelStorageMap, bh.Arguments.Uploader,
			handler.CurBackupInfo.Name, sharedSize)
		if err != nil {
			return err
		}

		return bh.Workers.Bundle.SetupComposer(ctx, maker)
	}

	bh.SetComposerInitFunc(composerInitFunc)

	if bh.PgInfo.PgVersion < 100000 {
		tracelog.DebugLogger.Printf("Query runner version is %d, disabling concurrent backups", bh.PgInfo.PgVersion)
		bh.Arguments.EnablePreventConcurrentBackups()
	}

	return bh, err
}
