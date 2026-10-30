package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	backupCopyUsage            = "copy"
	backupCopyShortDescription = "copy specific or all backups"
	backupCopyLongDescription  = "Copy backup(s) from one storage to another without transforming payloads"

	backupNameFlag        = "backup-name"
	backupNameShorthand   = "b"
	backupNameDescription = "Copy specific backup"

	fromFlag        = "from"
	fromShorthand   = "f"
	fromDescription = "Storage config from where should copy backup"

	toFlag        = "to"
	toShorthand   = "t"
	toDescription = "Storage config to where should copy backup"

	withAllHistoryFlag        = "with-history"
	withAllHistoryShorthand   = "w"
	withAllHistoryDescription = "Synchronize WAL from the backup recovery point through the latest continuous archive"
)

var (
	copyOutput     string
	copyOutputFile string
	backupName     string
	fromConfigFile string
	toConfigFile   string
	withAllHistory = false

	backupCopyCmd = &cobra.Command{
		Use:   backupCopyUsage,
		Short: backupCopyShortDescription,
		Long:  backupCopyLongDescription,
		Args:  cobra.ExactArgs(0),
		Run:   runBackupCopy,
		PersistentPreRun: func(*cobra.Command, []string) {
			// do not check for any configured settings because wal-g copy uses the different
			// settings init process
		},
	}
)

func runBackupCopy(cmd *cobra.Command, args []string) {
	if cmd.Flags().Changed("output") {
		if copyOutput != "rclone" {
			tracelog.ErrorLogger.Fatalf("unsupported --output %q: expected rclone", copyOutput)
		}
		if copyOutputFile == "" {
			tracelog.ErrorLogger.Fatalf("--output-file must not be empty")
		}
		from, _, err := internal.StorageAndConfigFromFile(cmd.Context(), fromConfigFile)
		tracelog.ErrorLogger.FatalOnError(err)
		plan, err := postgres.BuildCopyPlan(cmd.Context(), from.RootFolder(), nil, backupName, withAllHistory)
		tracelog.ErrorLogger.FatalOnError(err)
		if err := plan.WriteRcloneFileList(copyOutputFile); err != nil {
			tracelog.ErrorLogger.Fatalf("write rclone file list %q: %v", copyOutputFile, err)
		}
		tracelog.InfoLogger.Printf("Wrote rclone file list to %q.", copyOutputFile)
		return
	}
	postgres.HandleCopy(cmd.Context(), fromConfigFile, toConfigFile, backupName, withAllHistory)
}

func init() {
	Cmd.AddCommand(backupCopyCmd)

	backupCopyCmd.Flags().StringVarP(&backupName, backupNameFlag, backupNameShorthand, "", backupNameDescription)
	backupCopyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	backupCopyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)
	backupCopyCmd.Flags().BoolVarP(&withAllHistory,
		withAllHistoryFlag,
		withAllHistoryShorthand,
		false,
		withAllHistoryDescription)

	backupCopyCmd.Flags().StringVar(&copyOutput, "output", "", "write a file list instead of copying: rclone")
	backupCopyCmd.Flags().StringVar(&copyOutputFile, "output-file", "", "path to the output file list")
	backupCopyCmd.MarkFlagsRequiredTogether("output", "output-file")
	backupCopyCmd.MarkFlagsOneRequired("to", "output")
	_ = backupCopyCmd.MarkFlagRequired(fromFlag)
}
