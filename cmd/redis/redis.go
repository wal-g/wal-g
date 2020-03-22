package redis

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
)

var ShortDescription = "Redis backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var Cmd = &cobra.Command{
	Use:     "redis",
	Short:   ShortDescription, // TODO : improve description
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "Redis"}, "\t"),
}

func Execute() {
	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure, internal.AssertRequiredSettingsSet)

	Cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	Cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(Cmd)
}
