package greenplum

import (
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

		maker, err := NewGpTarBallComposerMaker(relStorageMap, bh.Workers.Uploader, handler.CurBackupInfo.Name)
		if err != nil {
			return err
		}

		return bh.Workers.Bundle.SetupComposer(maker)
	}

	bh.SetComposerInitFunc(composerInitFunc)

	return bh, err
}
