package postgres

import (
	"github.com/spf13/viper"
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
func HandleCatchupPush(pgDataDirectory string, fromLSN uint64) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)

	fakePreviousBackupSentinelDto := BackupSentinelDto{
		BackupStartLSN: &fromLSN,
	}

	extendExcludedFiles()

	backupArguments := BackupArguments{
		isPermanent:         false,
		verifyPageChecksums: false,
		pgDataDirectory:     pgDataDirectory,
		forceIncremental:    true,
		backupsFolder:       utility.CatchupPath,
		tarBallComposerType: RegularComposer,
		userData:            viper.GetString(internal.SentinelUserDataSetting),
	}
	backupConfig, err := NewBackupHandler(backupArguments)
	tracelog.ErrorLogger.FatalOnError(err)
	backupConfig.checkPgVersionAndPgControl()
	backupConfig.prevBackupInfo.SentinelDto = fakePreviousBackupSentinelDto
	backupConfig.curBackupInfo.StartLSN = fromLSN
	backupConfig.createAndPushBackup()
}
