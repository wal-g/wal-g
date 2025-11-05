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
		Use:   "snapshot-push [db_directory]",
		Short: snapshotPushShortDescription,
		Long: `Creates a snapshot backup using filesystem or cloud disk snapshots.

This command:
1. Connects to PostgreSQL and gets the data directory (if not provided as argument)
2. Calls pg_start_backup() to ensure database consistency
3. Executes a user-defined snapshot command (WALG_SNAPSHOT_COMMAND)
4. Calls pg_stop_backup()
5. Uploads backup metadata to storage

The snapshot command receives the following environment variables:
  - WALG_SNAPSHOT_NAME: The backup name
  - WALG_PG_DATA: The PostgreSQL data directory path
  - WALG_SNAPSHOT_START_LSN: The backup start LSN
  - WALG_SNAPSHOT_START_WAL_FILE: The WAL file name at backup start

Example snapshot command for AWS EBS:
  aws ec2 create-snapshot --volume-id vol-xxxxx --description "$WALG_SNAPSHOT_NAME"

WAL archiving must be properly configured for point-in-time recovery.`,
		Args: cobra.MaximumNArgs(1),
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

			// Get data directory - from argument if provided, otherwise from PostgreSQL
			var dataDirectory string
			if len(args) > 0 {
				dataDirectory = args[0]
			} else {
				// Get data directory from PostgreSQL connection
				conn, err := postgres.Connect()
				if err != nil {
					tracelog.ErrorLogger.FatalOnError(err)
				}
				defer conn.Close(cmd.Context())
				
				err = conn.QueryRow(cmd.Context(), "SHOW data_directory").Scan(&dataDirectory)
				tracelog.ErrorLogger.FatalfOnError("Failed to get data directory from PostgreSQL: %v", err)
				tracelog.InfoLogger.Printf("Using data directory from PostgreSQL: %s", dataDirectory)
			}

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




