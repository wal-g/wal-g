package mongo

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var DBShortDescription = "MongoDB backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var walgVersion = "devel"
var gitRevision = "devel"
var buildDate = "devel"

var cmd = &cobra.Command{
	Use:     "wal-g",
	Short:   DBShortDescription, // TODO : improve description
	Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "MongoDB"}, "\t"),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
		err = internal.ConfigureAndRunDefaultWebServer()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func Execute() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	internal.RequiredSettings[internal.MongoDBUriSetting] = true
	cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.wal-g.yaml)")
	cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(cmd)
}
