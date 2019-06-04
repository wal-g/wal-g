package mysql

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"os"
	"strings"
)

var MySQLShortDescription = "MySQL backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var MySQLCmd = &cobra.Command{
	Use:     "mysql",
	Short:   MySQLShortDescription, // TODO : improve description
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "MySQL"}, "\t"),
}

func Execute() {
	if err := MySQLCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	MySQLCmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	MySQLCmd.InitDefaultVersionFlag()
}
