package all

import (
	"fmt"
	"github.com/spf13/cobra"
	config "github.com/wal-g/wal-g/main"
	"os"
)

const WalgShortDescription = "Backup tool"

var cfgFile string

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var RootCmd = &cobra.Command{
	Use:     "wal-g",
	Short:   WalgShortDescription, // TODO : improve short and long descriptions
	Version: WalgVersion + "\t" + GitRevision + "\t" + BuildDate,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(config.InitConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.wal-g.yaml)")
	RootCmd.InitDefaultVersionFlag()
}