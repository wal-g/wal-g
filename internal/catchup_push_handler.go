package internal

import (
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

	createAndPushBackup(
		uploader,
		archiveDirectory, utility.CatchupPath,
		"", fakePreviousBackupSentinelDto,
		false, true, 0,
		false, false,
	)
}
