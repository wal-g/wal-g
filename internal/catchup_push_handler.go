package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/utility"
)

func extendExcludedFiles() {
	for _, fname := range []string{"pg_hba.conf", "postgresql.conf"} {
		ExcludedFilenames[fname] = utility.Empty{}
	}
}

// HandleCatchupPush is invoked to perform a wal-g catchup-push
func HandleCatchupPush(uploader *WalUploader, archiveDirectory string, fromLSN uint64) {
	archiveDirectory = utility.ResolveSymlink(archiveDirectory)
	checkPgVersionAndPgControl(archiveDirectory)

	fakePreviousBackupSentinelDto := BackupSentinelDto{
		BackupStartLSN: &fromLSN,
	}

	extendExcludedFiles()

	backupConfig := BackupConfig{
		uploader:                  uploader,
		archiveDirectory:          archiveDirectory,
		backupsFolder:             utility.CatchupPath,
		previousBackupName:        "",
		previousBackupSentinelDto: fakePreviousBackupSentinelDto,
		isPermanent:               false,
		forceIncremental:          true,
		incrementCount:            0,
		verifyPageChecksums:       false,
		storeAllCorruptBlocks:     false,
		tarBallComposerType:       RegularComposer,
		userData:                  viper.GetString(SentinelUserDataSetting),
	}

	createAndPushBackup(&backupConfig)
}
