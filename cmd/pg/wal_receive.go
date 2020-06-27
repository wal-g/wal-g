package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const WalReceiveShortDescription = "Receive WAL stream and push to storage"

// walReceiveCmd represents the walReceive command
var walReceiveCmd = &cobra.Command{
	Use:   "wal-receive tmp_filepath",
	Short: WalReceiveShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		archiveStatusManager, err := internal.ConfigureArchiveStatusManager()
		if err == nil {
			uploader.ArchiveStatusManager = internal.NewDataFolderASM(archiveStatusManager)
		} else {
			tracelog.ErrorLogger.PrintError(err)
			uploader.ArchiveStatusManager = internal.NewNopASM()
		}
		internal.HandleWALReceive(uploader, args[0])
	},
}

func init() {
	Cmd.AddCommand(walReceiveCmd)
}
