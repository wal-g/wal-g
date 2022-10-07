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

func handleConn(c net.Conn, f func(string) error) {
	defer c.Close()
	buf := make([]byte, 512)
	nr, err := c.Read(buf)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read checking message from client %s, err: %v\n", c.RemoteAddr(), err)
		_, _ = c.Write([]byte("READ_FAILED"))
		return
	}
	if nr == 5 && string(buf[0:5]) == "CHECK" {
		_, _ = c.Write([]byte("CHECKOK"))
		tracelog.InfoLogger.Printf("Sucessful configuration check")
	} else {
		tracelog.ErrorLogger.Printf("Error on configuration check")
		return
	}
	n, err := c.Read(buf)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message with file from client %s, err: %v\n", c.RemoteAddr(), err)
		_, _ = c.Write([]byte("READ_FAILED"))
		return
	}

	if n < 24 {
		if n > 0 {
			tracelog.ErrorLogger.Printf("Received incorrect message %s from %s", buf[0:n], c.RemoteAddr())
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

			go handleConn(fd, func(walFileName string) error {
				fullPath := path.Join(args[0], walFileName)
				tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
				return postgres.HandleWALPush(uploader, fullPath)
			})
		}
	},
}

func init() {
	Cmd.AddCommand(daemonCmd)
}
