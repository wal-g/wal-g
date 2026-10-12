package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

var (
	uniformCopyOutput      string
	uniformCopyOutputFile  string
	uniformCopyBackupName  string
	uniformCopyFrom        string
	uniformCopyTo          string
	uniformCopyWithHistory bool
)

var uniformCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a MySQL backup and optionally synchronize binlog history",
	Args:  cobra.NoArgs,
	Run: func(command *cobra.Command, _ []string) {
		if command.Flags().Changed("output") {
			if uniformCopyOutput != "rclone" {
				tracelog.ErrorLogger.Fatalf("unsupported --output %q: expected rclone", uniformCopyOutput)
			}
			if uniformCopyOutputFile == "" {
				tracelog.ErrorLogger.Fatalf("--output-file must not be empty")
			}
			from, _, err := internal.StorageAndConfigFromFile(command.Context(), uniformCopyFrom)
			tracelog.ErrorLogger.FatalOnError(err)
			plan, err := db.BuildCopyPlan(command.Context(), from.RootFolder(), nil, uniformCopyBackupName, uniformCopyWithHistory, "")
			tracelog.ErrorLogger.FatalOnError(err)
			if err := plan.WriteRcloneFileList(uniformCopyOutputFile); err != nil {
				tracelog.ErrorLogger.Fatalf("write rclone file list %q: %v", uniformCopyOutputFile, err)
			}
			tracelog.InfoLogger.Printf("Wrote rclone file list to %q.", uniformCopyOutputFile)
			return
		}
		db.HandleCopy(command.Context(), uniformCopyFrom, uniformCopyTo, uniformCopyBackupName, uniformCopyWithHistory)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	uniformCopyCmd.Flags().StringVarP(&uniformCopyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	uniformCopyCmd.Flags().StringVarP(&uniformCopyFrom, "from", "f", "", "source storage configuration file")
	uniformCopyCmd.Flags().StringVarP(&uniformCopyTo, "to", "t", "", "destination storage configuration file")
	uniformCopyCmd.Flags().BoolVarP(
		&uniformCopyWithHistory, "with-history", "w", false,
		"synchronize binlog history through the latest archived entry")
	_ = uniformCopyCmd.MarkFlagRequired("from")
	uniformCopyCmd.Flags().StringVar(&uniformCopyOutput, "output", "", "write a file list instead of copying: rclone")
	uniformCopyCmd.Flags().StringVar(&uniformCopyOutputFile, "output-file", "", "path to the output file list")
	uniformCopyCmd.MarkFlagsRequiredTogether("output", "output-file")
	uniformCopyCmd.MarkFlagsOneRequired("to", "output")
	cmd.AddCommand(uniformCopyCmd)
}
