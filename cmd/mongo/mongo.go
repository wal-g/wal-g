package mongo

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	config "github.com/wal-g/wal-g/main"
)

var MongoDBShortDescription = "MongoDB backup tool"

var cfgFile string

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var MongoCmd = &cobra.Command{
	Use:     "wal-g",
	Short:   MongoDBShortDescription, // TODO : improve description
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "MongoDB"}, "\t"),
}

func Execute() {
	if err := MongoCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(config.InitConfig)

	MongoCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.wal-g.yaml)")
	MongoCmd.InitDefaultVersionFlag()
}
