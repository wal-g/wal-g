package pg

import (
	"net"
	"os"
	"path"

	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const DaemonShortDescription = "Uploads a WAL file to storage"
const DaemonSocketName = "/tmp/wal-push.sock"

// daemonCmd represents the daemon command
var daemonCmd = &cobra.Command{
	Use:   "daemon wal_filepath",
	Short: DaemonShortDescription, // TODO : improve description
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

		_ = os.Remove(DaemonSocketName)
		l, err := net.Listen("unix", DaemonSocketName)
		if err != nil {
			tracelog.ErrorLogger.Fatal("listen error:", err)
		}
		for {
			fd, err := l.Accept()
			if err != nil {
				tracelog.ErrorLogger.Println("accept error:", err)
			}

			go postgres.HandleDaemon(fd, func(walFileName string) error {
				fullPath := path.Join(args[0], walFileName)
				tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
				uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)
				return postgres.HandleWALPush(uploader, fullPath)
			})
		}
	},
}

func init() {
	Cmd.AddCommand(daemonCmd)
}
