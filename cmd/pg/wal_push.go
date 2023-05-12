package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/utility"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		baseUploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		failover, err := internal.InitFailoverStorages()
		tracelog.ErrorLogger.FatalOnError(err)

		uploader, err := multistorage.NewUploader(baseUploader, failover)
		tracelog.ErrorLogger.FatalOnError(err)

		walUploader, err := postgres.ConfigureWalUploader(uploader)
		tracelog.ErrorLogger.FatalOnError(err)

		archiveStatusManager, err := internal.ConfigureArchiveStatusManager()
		if err == nil {
			walUploader.ArchiveStatusManager = asm.NewDataFolderASM(archiveStatusManager)
		} else {
			tracelog.ErrorLogger.PrintError(err)
			walUploader.ArchiveStatusManager = asm.NewNopASM()
		}

		PGArchiveStatusManager, err := internal.ConfigurePGArchiveStatusManager()
		if err == nil {
			walUploader.PGArchiveStatusManager = asm.NewDataFolderASM(PGArchiveStatusManager)
		} else {
			tracelog.ErrorLogger.PrintError(err)
			walUploader.PGArchiveStatusManager = asm.NewNopASM()
		}

		walUploader.ChangeDirectory(utility.WalPath)
		err = postgres.HandleWALPush(walUploader, args[0])
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(walPushCmd)
}
