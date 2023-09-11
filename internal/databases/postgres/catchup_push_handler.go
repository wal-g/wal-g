package postgres

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func extendExcludedFiles() {
	for _, fname := range []string{"pg_hba.conf", "postgresql.conf"} {
		ExcludedFilenames[fname] = utility.Empty{}
	}
}

// HandleCatchupPush is invoked to perform a wal-g catchup-push
func HandleCatchupPush(ctx context.Context, pgDataDirectory string, fromLSN LSN) {
	uploader, err := internal.ConfigureUploader()
	tracelog.ErrorLogger.FatalOnError(err)

	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)

	fakePreviousBackupSentinelDto := BackupSentinelDto{
		BackupStartLSN: &fromLSN,
	}

	extendExcludedFiles()

	userData, err := internal.GetSentinelUserData()
	tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

	backupArguments := NewBackupArguments(
		uploader, pgDataDirectory, utility.CatchupPath, false,
		false, false, false,
		RegularComposer, NewCatchupDeltaBackupConfigurator(fakePreviousBackupSentinelDto),
		userData, false)

	backupConfig, err := NewBackupHandler(backupArguments)
	tracelog.ErrorLogger.FatalOnError(err)
	backupConfig.HandleBackupPush(ctx)
}
