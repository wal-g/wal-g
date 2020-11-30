package mysql

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

var confirmed = false

const DeleteBinlogUsage =  "binlog"

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

var deleteTargetBackupCmd = &cobra.Command{
	Use:     "target-backup", // TODO : improve description
	Example: internal.DeleteTargetExamples,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := internal.DeleteTargetArgsValidator(cmd, args); err != nil {
			return err
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		target := args[0]
		if err := runDeleteTargetBackups(folder, target); err != nil {
			tracelog.ErrorLogger.FatalOnError(fmt.Errorf("delete target backup: failed due %v", ))
		}
	},
}

var deleteBinlogCmd = &cobra.Command{
	Use:   DeleteBinlogUsage, // TODO : improve description
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

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		regex := args[0]
		r, err := regexp.Compile(regex)
		tracelog.ErrorLogger.FatalOnError(err)

		if err := runDeleteBinlogs(folder, func(name string) bool {
			return r.MatchString(name)
		}); err != nil {
			tracelog.ErrorLogger.FatalOnError(fmt.Errorf("delete target backup: failed due %v", err))
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

func runDeleteTargetBackups(folder storage.Folder, name string) error {
	b, err := internal.BackupByName(name, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	return b.Delete(confirmed)
}


func runDeleteBinlogs(folder storage.Folder, filter func(name string) bool) error {

	mp := make(map[string]struct{})

	targets, err := internal.FindTargets(folder.GetSubFolder(mysql.BinlogPath), func(object storage.Object) bool {
		return filter(object.GetName())
	})
	tracelog.ErrorLogger.FatalOnError(err)
	for _, e := range targets {
		mp[e.GetName()] = struct{}{}
	}

	return internal.DeleteTargets(folder.GetSubFolder(
		utility.BaseBackupPath), func(object storage.Object) bool {
		if _, ok := mp[object.GetName()]; ok {
			return true
		}
		return false
	}, confirmed)
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



func init() {
	Cmd.AddCommand(deleteCmd)

	deleteTargetBackupCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")

	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd, deleteTargetBackupCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms objects deletion")

	deleteBinlogCmd.Flags().BoolVarP(&MatchExact, "match-exact", "e", false, "")
	deleteBinlogCmd.Flags().BoolVarP(&MatchPrefix, "match-prefix", "p", false, "")

	deleteCmd.AddCommand(deleteBinlogCmd)
}
