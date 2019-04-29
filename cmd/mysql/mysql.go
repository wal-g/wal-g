package mysql

import (
	"fmt"
	"github.com/spf13/cobra"
	config "github.com/wal-g/wal-g/main"
	"os"
)

var MySQLShortDescription = "MySQL backup tool"

var cfgFile string

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var MySQLCmd = &cobra.Command{
	Use:   "mysql",
	Short: MySQLShortDescription, // TODO : improve description
	Version: WalgVersion + "\t" + GitRevision + "\t" + BuildDate + "\t" + "MySQL",
}

func Execute() {
	if err := MySQLCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(config.InitConfig)

	MySQLCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.wal-g.yaml)")
	MySQLCmd.InitDefaultVersionFlag()
}
