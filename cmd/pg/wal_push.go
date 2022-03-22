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

const WalPushShortDescription = "Uploads a WAL file to storage"
const WalPushSocketName = "/tmp/wal-push.sock"

func handleConn(c net.Conn, f func(string) error) {
	defer c.Close()
	buf := make([]byte, 512)
	nr, err := c.Read(buf)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message from client %s, err: %v\n", c.RemoteAddr(), err)
		_, _ = c.Write([]byte("READ_FAILED"))
		return
	}

	if nr < 24 {
		if nr > 0 {
			tracelog.ErrorLogger.Printf("Received incorrect message %s from %s", buf[0:nr], c.RemoteAddr())
		} else {
			tracelog.ErrorLogger.Printf("Received empty message from %s", c.RemoteAddr())
		}
		_, _ = c.Write([]byte("BAD_MSG"))
		return
	}

	data := buf[0:24]
	tracelog.InfoLogger.Printf("wal file name: %s\n", string(data))

	err = f(string(data))
	if err != nil {
		tracelog.ErrorLogger.Printf("wal-push failed: %v\n", err)
		_, _ = c.Write([]byte("FAIL"))
		return
	}

	_, err = c.Write([]byte("OK"))
	if err != nil {
		tracelog.ErrorLogger.Println("OK write fail: ", err)
	}
}

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

		if !daemonMode {
			tracelog.ErrorLogger.FatalOnError(postgres.HandleWALPush(uploader, args[0]))
			return
		}

		_ = os.Remove(WalPushSocketName)
		l, err := net.Listen("unix", WalPushSocketName)
		if err != nil {
			tracelog.ErrorLogger.Fatal("listen error:", err)
		}
		for {
			fd, err := l.Accept()
			if err != nil {
				tracelog.ErrorLogger.Println("accept error:", err)
			}

			go handleConn(fd, func(walFileName string) error {
				fullPath := path.Join(args[0], walFileName)
				tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
				return postgres.HandleWALPush(uploader, fullPath)
			})
		}
	},
}

var daemonMode = false

func init() {
	Cmd.AddCommand(walPushCmd)
	walPushCmd.Flags().BoolVar(&daemonMode, "daemon", false, "Run in daemon mode")
}
