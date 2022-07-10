package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/sftp"
)

const (
	WalRestoreUsage            = "wal-restore target-pgdata source-pgdata"
	WalRestoreShortDescription = "Restores WAL segments from storage to the source to perform pg_rewind with the target."
	WalRestoreLongDescription  = "Restores the missing WAL segments that will be needed in source cluster to perform" +
		" pg_rewind with target cluster from storage. To run in remote mode, should be run from the source cluster. " +
		"If you mark the target cluster as remote you should specify requisites to connect by flags."
	RemoteFlag            = "remote"
	sshHostFlag           = "host-ssh"
	sshPortFlag           = "port-ssh"
	sshUsernameFlag       = "username-ssh"
	sshPasswordFlag       = "password-ssh"
	sshPrivateKeyPathFlag = "private-key-path-ssh"
)

// walRestoreCmd represents the walRestore command
var (
	walRestoreCmd = &cobra.Command{
		Use:   WalRestoreUsage,
		Short: WalRestoreShortDescription,
		Long:  WalRestoreLongDescription,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalfOnError("Error on configure external folder %s\n", err)

			sshRequisites := sftp.SSHRequisites{
				Host:           sshHost,
				Port:           sshPort,
				Username:       sshUsername,
				Password:       sshPassword,
				PrivateKeyPath: sshPrivateKeyPath,
			}

			postgres.HandleWALRestore(args[0], args[1], folder, remote, sshRequisites)
		},
	}
	remote            = false
	sshHost           = ""
	sshPort           = ""
	sshUsername       = ""
	sshPassword       = ""
	sshPrivateKeyPath = ""
)

func init() {
	Cmd.AddCommand(walRestoreCmd)

	walRestoreCmd.Flags().BoolVar(&remote, RemoteFlag, false, "Is target cluster remote")
	walRestoreCmd.Flags().StringVar(&sshHost, sshHostFlag, "", "Host of remote target cluster to connect by SSH")
	walRestoreCmd.Flags().StringVar(&sshPort, sshPortFlag, "22", "Port of remote target cluster to connect by SSH")
	walRestoreCmd.Flags().StringVar(&sshUsername, sshUsernameFlag, "", "Username for connect to remote cluster by SSH")
	walRestoreCmd.Flags().StringVar(&sshPassword, sshPasswordFlag, "", "Password for connect to remote cluster by SSH")
	walRestoreCmd.Flags().StringVar(&sshPrivateKeyPath, sshPrivateKeyPathFlag, "", "Path to private key for connect to remote cluster by SSH")
}
