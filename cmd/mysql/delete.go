package mysql

import (
	"fmt"
	"path"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

var confirmed = false

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete", //for example "delete mysql before time"
	Short: "Clears old backups and binary logs",
}

var deleteBeforeCmd = &cobra.Command{
	Use:     internal.DeleteBeforeUsageExample, // TODO : improve description
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       internal.DeleteRetainUsageExample, // TODO : improve description
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Args:      internal.DeleteRetainArgsValidator,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample, // TODO : improve description
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

var (
	MatchPrefix = false
	MatchExact  = false
)

var deleteTargetCmd = &cobra.Command{
	Use:     internal.DeleteTargetUsageExample, // TODO : improve description
	Example: internal.DeleteTargetExamples,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := internal.DeleteTargetArgsValidator(cmd, args); err != nil {
			return err
		}
		if MatchExact && MatchPrefix {
			return fmt.Errorf("expected to get either match exact nor match prefix, but never both")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if MatchPrefix {
			runDeleteTargets(cmd, args, func(object storage.Object, target string) bool {
				return strings.HasPrefix(object.GetName(), target)
			})
		} else if MatchExact {
			runDeleteTargets(cmd, args, func(object storage.Object, target string) bool {
				return object.GetName() == target
			})
		}
	},
}

func runDeleteEverything(_ *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	internal.DeleteEverything(folder, confirmed, args)
}

func runDeleteBefore(_ *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	isFullBackup := func(object storage.Object) bool {
		return IsFullBackup()
	}
	internal.HandleDeleteBefore(folder, args, confirmed, isFullBackup, GetLessFunc(folder))
}

func runDeleteRetain(_ *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	isFullBackup := func(object storage.Object) bool {
		return IsFullBackup()
	}
	internal.HandleDeleteRetain(folder, args, confirmed, isFullBackup, GetLessFunc(folder))
}

func runDeleteTargets(_ *cobra.Command, args []string, filter func(object storage.Object, target string) bool) {
	target := args[0]

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	targets := make([]storage.Object, 0, 0)

	for _, e := range []string{
		mysql.BinlogPath,
		utility.BaseBackupPath,
	} {
		targetsCurr, err := internal.FindTargets(folder.GetSubFolder(e), func(object storage.Object) bool {
			return filter(object, target)
		})
		tracelog.ErrorLogger.FatalOnError(err)
		targets = append(targets, targetsCurr...)
	}

	nameMp := make(map[string]bool)
	for _, e := range targets {
		nameMp[e.GetName()] = true
	}
	for _, e := range []string{
		mysql.BinlogPath,
		utility.BaseBackupPath,
	} {
		tracelog.ErrorLogger.FatalOnError(internal.DeleteTargets(folder.GetSubFolder(e), func(object storage.Object) bool {
			if v, ok := nameMp[object.GetName()]; v && ok {
				return true
			}
			return false
		}, confirmed))
	}
}

func init() {
	Cmd.AddCommand(deleteCmd)
	deleteTargetCmd.Flags().BoolVarP(&MatchExact, "match-exact", "e", false, "")
	deleteTargetCmd.Flags().BoolVarP(&MatchPrefix, "match-prefix", "p", false, "")
	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd, deleteTargetCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func IsFullBackup() bool {
	return true
}

func GetLessFunc(folder storage.Folder) func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok := utility.TryFetchTimeRFC3999(object1.GetName())
		if !ok {
			return binlogLess(folder, object1, object2)
		}
		time2, ok := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok {
			return binlogLess(folder, object1, object2)
		}
		return time1 < time2
	}
}

func binlogLess(folder storage.Folder, object1, object2 storage.Object) bool {
	binlogName1, ok := tryFetchBinlogName(folder, object1)
	if !ok {
		return false
	}
	binlogName2, ok := tryFetchBinlogName(folder, object2)
	if !ok {
		return false
	}
	return binlogName1 < binlogName2
}

func tryFetchBinlogName(folder storage.Folder, object storage.Object) (string, bool) {
	name := object.GetName()
	if strings.HasPrefix(name, mysql.BinlogPath) {
		_, name = path.Split(name)
		return name, true
	}
	name = strings.Replace(name, utility.SentinelSuffix, "", 1)
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backup := internal.NewBackup(baseBackupFolder, name)
	var sentinel mysql.StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &sentinel)
	if err != nil {
		tracelog.InfoLogger.Println("Fail to fetch stream sentinel " + name)
		return "", false
	}
	return sentinel.BinLogStart, true
}
