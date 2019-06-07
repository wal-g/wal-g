package redis

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"os"
	"strings"
)

var RedisShortDescription = "Redis backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var RedisCmd = &cobra.Command{
	Use:     "redis",
	Short:   RedisShortDescription, // TODO : improve description
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "Redis"}, "\t"),
}

func Execute() {
	if err := RedisCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	RedisCmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	RedisCmd.InitDefaultVersionFlag()
}
