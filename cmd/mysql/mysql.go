package mysql

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var ShortDescription = "MySQL backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var walgVersion = "devel"
var gitRevision = "devel"
var buildDate = "devel"

var cmd = &cobra.Command{
	Use:     "mysql",
	Short:   ShortDescription, // TODO : improve description
	Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "MySQL"}, "\t"),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		err := internal.AssertRequiredSettingsSet()
		if err != nil {
			tracelog.WarningLogger.PrintError(err)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main().
func Execute() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	internal.ConfigureSettings(internal.MYSQL)
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(cmd)
}
