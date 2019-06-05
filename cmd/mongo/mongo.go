package mongo

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
)

var MongoDBShortDescription = "MongoDB backup tool"

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
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	MongoCmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.wal-g.yaml)")
	MongoCmd.InitDefaultVersionFlag()
}
