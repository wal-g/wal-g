package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := postgres.ConfigureWalUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		archiveStatusManager, err := internal.ConfigureArchiveStatusManager()
		if err == nil {
			uploader.ArchiveStatusManager = asm.NewDataFolderASM(archiveStatusManager)
		} else {
			tracelog.ErrorLogger.PrintError(err)
			uploader.ArchiveStatusManager = asm.NewNopASM()
		}

		PGArchiveStatusManager, err := internal.ConfigurePGArchiveStatusManager()
		if err == nil {
			uploader.PGArchiveStatusManager = asm.NewDataFolderASM(PGArchiveStatusManager)
		} else {
			tracelog.ErrorLogger.PrintError(err)
			uploader.PGArchiveStatusManager = asm.NewNopASM()
		}

		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)
		err = postgres.HandleWALPush(uploader, args[0])
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(walPushCmd)
}
