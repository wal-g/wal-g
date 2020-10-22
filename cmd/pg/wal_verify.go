package pg

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalVerifyUsage            = "wal-verify"
	WalVerifyShortDescription = "Verify WAL segment files integrity in storage."
	WalVerifyLongDescription  = "Walk backwards from current cluster segment through WAL segments in storage" +
		" and check for missing segments."

	useJsonOutputFlag        = "json"
	useJsonOutputDescription = "Show output in JSON format."
)

var (
	// walVerifyCmd represents the walVerify command
	walVerifyCmd = &cobra.Command{
		Use:   WalVerifyUsage,
		Short: WalVerifyShortDescription,
		Long:  WalVerifyLongDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			outputType := internal.WalVerifyTableOutput
			if useJsonOutput {
				outputType = internal.WalVerifyJsonOutput
			}
			outputWriter := internal.NewWalVerifyOutputWriter(outputType, os.Stdout)
			internal.HandleWalVerify(folder, internal.QueryCurrentWalSegment(), outputWriter)
		},
	}
	useJsonOutput bool
)

func init() {
	Cmd.AddCommand(walVerifyCmd)
	walVerifyCmd.Flags().BoolVar(&useJsonOutput, useJsonOutputFlag, false, useJsonOutputDescription)
}
