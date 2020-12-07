package pg

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalVerifyUsage            = "wal-verify"
	WalVerifyShortDescription = "Verify WAL storage folder. Available checks: integrity."
	WalVerifyLongDescription  = "Run a set of specified checks to ensure WAL storage health."

	useJsonOutputFlag        = "json"
	useJsonOutputDescription = "Show output in JSON format."

	checkIntegrityArg = "integrity"
)

var (
	availableChecks = map[string]internal.WalVerifyCheckType{
		checkIntegrityArg: internal.WalVerifyIntegrityCheck,
	}
	// walVerifyCmd represents the walVerify command
	walVerifyCmd = &cobra.Command{
		Use:   WalVerifyUsage,
		Short: WalVerifyShortDescription,
		Long:  WalVerifyLongDescription,
		Args:  checkArgs,
		Run: func(cmd *cobra.Command, checks []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			outputType := internal.WalVerifyTableOutput
			if useJsonOutput {
				outputType = internal.WalVerifyJsonOutput
			}
			outputWriter := internal.NewWalVerifyOutputWriter(outputType, os.Stdout)
			checkTypes := parseChecks(checks)
			internal.HandleWalVerify(checkTypes, folder, internal.QueryCurrentWalSegment(), outputWriter)
		},
	}
	useJsonOutput bool
)

func parseChecks(checks []string) []internal.WalVerifyCheckType {
	checkTypes := make([]internal.WalVerifyCheckType, 0, len(checks))
	for _, check := range checks {
		checkType, ok := availableChecks[check]
		if !ok {
			tracelog.ErrorLogger.Fatalf("Check %s is not available.", check)
		}
		checkTypes = append(checkTypes, checkType)
	}
	return checkTypes
}

func checkArgs(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		availableCheckCommands := make([]string, 0, len(availableChecks))
		for cmdName := range availableChecks {
			availableCheckCommands = append(availableCheckCommands, cmdName)
		}
		return errors.New("at least one of the following checks should be specified: " +
			strings.Join(availableCheckCommands, ", "))
	}
	for _, arg := range args {
		if _, ok := availableChecks[arg]; !ok {
			return fmt.Errorf("invalid check specified: %s", arg)
		}
	}
	return nil
}

func init() {
	cmd.AddCommand(walVerifyCmd)
	walVerifyCmd.Flags().BoolVar(&useJsonOutput, useJsonOutputFlag, false, useJsonOutputDescription)
}
