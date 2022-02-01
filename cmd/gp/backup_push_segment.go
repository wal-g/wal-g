package gp

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/spf13/cobra"
)

const (
	segmentBackupPushShortDescription = "Makes segment backup and updates the backup state file"
)

var (
	// segBackupPushCmd represents the segBackupPush command
	segBackupPushCmd = &cobra.Command{
		Use:   "seg-backup-push backup_name backup_args --content-id=[content_id]",
		Short: segmentBackupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			backupName := args[0]
			backupArgs := args[1]

			stateUpdateInterval, err := internal.GetDurationSetting(internal.GPSegmentsUpdInterval)
			tracelog.ErrorLogger.FatalOnError(err)
			greenplum.NewSegBackupRunner(contentID, backupName, backupArgs, stateUpdateInterval).Run()
		},
	}
)

var contentID int

func init() {
	segBackupPushCmd.PersistentFlags().IntVar(&contentID, "content-id", 0, "segment content ID")
	_ = segBackupPushCmd.MarkFlagRequired("content-id")
	cmd.AddCommand(segBackupPushCmd)
}
