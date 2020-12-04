package sqlserver

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
)

var ShortDescription = "SQLServer backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var cmd = &cobra.Command{
	Use:     "sqlserver",
	Short:   ShortDescription,
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "SQLServer"}, "\t"),
}

func Execute() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)
	cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	cmd.InitDefaultVersionFlag()
}
