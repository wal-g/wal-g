package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	snapshotFetchShortDescription = "Prepares a snapshot backup for recovery"
	snapshotFetchLongDescription  = `Prepares a snapshot backup for recovery by creating backup_label file.

After restoring the snapshot data to the target directory, run this command
to create the necessary backup_label file that PostgreSQL needs for recovery.

The command will:
1. Fetch snapshot backup metadata from storage
2. Generate backup_label file with correct LSN and timeline information
3. Optionally generate tablespace_map if tablespaces were used
4. Optionally configure recovery settings

Example workflow:
  1. Restore snapshot: cp -a /snapshots/base_00000001... /var/lib/postgresql/data
  2. Prepare for recovery: wal-g snapshot-fetch base_00000001... /var/lib/postgresql/data
  3. Start PostgreSQL: pg_ctl start

The backup_label file is critical for PostgreSQL to understand:
  - Where to start WAL replay
  - The backup start checkpoint
  - The timeline
  - Tablespace locations (if any)`
)

var (
	// snapshotFetchCmd represents the snapshot-fetch command
	snapshotFetchCmd = &cobra.Command{
		Use:   "snapshot-fetch backup_name target_directory",
		Short: snapshotFetchShortDescription,
		Long:  snapshotFetchLongDescription,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			storage, err := internal.ConfigureStorage()
			tracelog.ErrorLogger.FatalOnError(err)
			folder := storage.RootFolder()

			backupName := args[0]
			targetDirectory := args[1]

			tracelog.InfoLogger.Printf("Fetching snapshot backup metadata for: %s", backupName)
			tracelog.InfoLogger.Printf("Target directory: %s", targetDirectory)

			handler := postgres.NewSnapshotFetchHandler(backupName, targetDirectory, folder)

			if snapshotSetupRecovery {
				// Setup with recovery configuration
				restoreCmd := "wal-g wal-fetch %f %p"
				if snapshotRestoreCommand != "" {
					restoreCmd = snapshotRestoreCommand
				}

				err = handler.HandleSnapshotFetchWithRecovery(
					cmd.Context(),
					restoreCmd,
					snapshotRecoveryTarget,
				)
			} else {
				// Just create backup_label
				err = handler.HandleSnapshotFetch(cmd.Context())
			}

			tracelog.ErrorLogger.FatalOnError(err)

			tracelog.InfoLogger.Println("Snapshot backup is ready for recovery")
			if !snapshotSetupRecovery {
				tracelog.InfoLogger.Println("Don't forget to configure restore_command in postgresql.conf or recovery.conf")
				tracelog.InfoLogger.Println("Example: restore_command = 'wal-g wal-fetch %f %p'")
			}
		},
	}

	snapshotSetupRecovery   bool
	snapshotRestoreCommand  string
	snapshotRecoveryTarget  string
)

func init() {
	Cmd.AddCommand(snapshotFetchCmd)

	snapshotFetchCmd.Flags().BoolVar(&snapshotSetupRecovery, "setup-recovery", false,
		"Automatically configure recovery settings (creates recovery.signal/recovery.conf)")
	snapshotFetchCmd.Flags().StringVar(&snapshotRestoreCommand, "restore-command", "",
		"Custom restore_command (default: 'wal-g wal-fetch %%f %%p')")
	snapshotFetchCmd.Flags().StringVar(&snapshotRecoveryTarget, "recovery-target", "",
		"Point-in-time recovery target (e.g., '2025-11-01 10:30:00')")
}

