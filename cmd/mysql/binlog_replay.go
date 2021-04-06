package mysql

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

const replaySinceFlagShortDescr = "backup name starting from which you want to fetch binlogs"
const replayUntilFlagShortDescr = "time in RFC3339 for PITR"

var replayBackupName string
var replayUntilTS string

var binlogReplayCmd = &cobra.Command{
	Use:   "binlog-replay",
	Short: "fetches binlogs from storage and replays them to mysql",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBinlogReplay(folder, replayBackupName, replayUntilTS)
	},
}

func init() {
	binlogReplayCmd.PersistentFlags().StringVar(&replayBackupName, "since", "LATEST", replaySinceFlagShortDescr)
	binlogReplayCmd.PersistentFlags().StringVar(&replayUntilTS, "until",
		utility.TimeNowCrossPlatformUTC().Format(time.RFC3339), replayUntilFlagShortDescr)
	cmd.AddCommand(binlogReplayCmd)
}
