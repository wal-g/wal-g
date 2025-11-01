package pg

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/utility"
)

const (
	snapshotPushShortDescription = "Creates a snapshot backup using filesystem snapshots"
	snapshotPermanentFlag        = "permanent"
	snapshotPermanentShorthand   = "p"
	snapshotAddUserDataFlag      = "add-user-data"
)

var (
	// snapshotPushCmd represents the snapshot-push command
	snapshotPushCmd = &cobra.Command{
		Use:   "snapshot-push db_directory",
		Short: snapshotPushShortDescription,
		Long: `Creates a snapshot backup using filesystem or cloud disk snapshots.

This command:
1. Calls pg_start_backup() to ensure database consistency
2. Executes a user-defined snapshot command (WALG_SNAPSHOT_COMMAND)
3. Calls pg_stop_backup()
4. Uploads backup metadata to storage

The snapshot command receives the following environment variables:
  - WALG_SNAPSHOT_NAME: The backup name
  - WALG_PG_DATA: The PostgreSQL data directory path
  - WALG_SNAPSHOT_START_LSN: The backup start LSN

Example snapshot command for AWS EBS:
  aws ec2 create-snapshot --volume-id vol-xxxxx --description "$WALG_SNAPSHOT_NAME"

WAL archiving must be properly configured for point-in-time recovery.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()

			storage, err := internal.ConfigureMultiStorage(true)
			tracelog.ErrorLogger.FatalfOnError("Failed to configure multi-storage: %v", err)

			rootFolder := multistorage.SetPolicies(storage.RootFolder(), policies.TakeFirstStorage)
			if targetStorage == "" {
				rootFolder, err = multistorage.UseFirstAliveStorage(rootFolder)
			} else {
				rootFolder, err = multistorage.UseSpecificStorage(targetStorage, rootFolder)
			}
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.InfoLogger.Printf("Snapshot backup will be pushed to storage: %v", multistorage.UsedStorages(rootFolder)[0])

			uploader, err := internal.ConfigureUploaderToFolder(rootFolder)
			tracelog.ErrorLogger.FatalOnError(err)

			dataDirectory := args[0]

			// Get snapshot command from config
			snapshotCommand, err := postgres.GetSnapshotCommand()
			tracelog.ErrorLogger.FatalOnError(err)

			// Get user data if provided
			if snapshotUserDataRaw == "" {
				snapshotUserDataRaw = viper.GetString(conf.SentinelUserDataSetting)
			}
			userData, err := internal.UnmarshalSentinelUserData(snapshotUserDataRaw)
			tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

			arguments := postgres.NewSnapshotBackupArguments(
				uploader,
				dataDirectory,
				utility.BaseBackupPath,
				snapshotPermanent,
				userData,
				snapshotCommand,
			)

			snapshotHandler, err := postgres.NewSnapshotBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)

			snapshotHandler.HandleSnapshotPush(cmd.Context())
		},
	}

	snapshotPermanent    bool
	snapshotUserDataRaw  string
)

func init() {
	Cmd.AddCommand(snapshotPushCmd)

	snapshotPushCmd.Flags().BoolVarP(&snapshotPermanent, snapshotPermanentFlag, snapshotPermanentShorthand,
		false, "Marks snapshot backup as permanent")
	snapshotPushCmd.Flags().StringVar(&snapshotUserDataRaw, snapshotAddUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files")
	snapshotPushCmd.Flags().StringVar(&targetStorage, "target-storage", "",
		targetStorageDescription)
}



