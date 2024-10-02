package mongo

import (
	"context"
	"encoding/json"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
)

const LatestBackupString = "LATEST_BACKUP"

// oplogReplayCmd represents oplog replay procedure
var oplogReplayCmd = &cobra.Command{
	Use:   "oplog-replay <since ts.inc> <until ts.inc>",
	Short: "Fetches oplog archives from storage and applies to database",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		defer func() { tracelog.ErrorLogger.FatalOnError(err) }()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		replayArgs, mongodbURL, err := buildOplogReplayRunArgs(args)
		if err != nil {
			return
		}

		err = mongo.RunOplogReplay(ctx, mongodbURL, replayArgs)
	},
}

func buildOplogReplayRunArgs(cmdargs []string) (binary.ReplyOplogConfig, string, error) {
	var args binary.ReplyOplogConfig
	// resolve archiving settings
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	if err != nil {
		return args, "", err
	}
	args.Since, err = processArg(cmdargs[0], downloader)
	if err != nil {
		return args, "", err
	}
	args.Until, err = processArg(cmdargs[1], downloader)
	if err != nil {
		return args, "", err
	}

	if ignoreErrCodesStr, ok := conf.GetSetting(conf.OplogReplayIgnoreErrorCodes); ok {
		if err = json.Unmarshal([]byte(ignoreErrCodesStr), &args.IgnoreErrCodes); err != nil {
			return args, "", err
		}
	}

	mongodbURL, err := conf.GetRequiredSetting(conf.MongoDBUriSetting)
	if err != nil {
		return args, "", err
	}

	oplogAlwaysUpsert, hasOplogAlwaysUpsert, err := conf.GetBoolSetting(conf.OplogReplayOplogAlwaysUpsert)
	if err != nil {
		return args, "", err
	}
	if hasOplogAlwaysUpsert {
		args.OplogAlwaysUpsert = &oplogAlwaysUpsert
	}

	if oplogApplicationMode, hasOplogApplicationMode := conf.GetSetting(
		conf.OplogReplayOplogApplicationMode); hasOplogApplicationMode {
		args.OplogApplicationMode = &oplogApplicationMode
	}

	return args, mongodbURL, nil
}

func processArg(arg string, downloader *archive.StorageDownloader) (models.Timestamp, error) {
	switch arg {
	case internal.LatestString:
		return downloader.LastKnownArchiveTS()
	case LatestBackupString:
		lastBackupName, err := downloader.LastBackupName()
		if err != nil {
			return models.Timestamp{}, err
		}
		backupMeta, err := downloader.BackupMeta(lastBackupName)
		if err != nil {
			return models.Timestamp{}, err
		}
		return models.TimestampFromBson(backupMeta.MongoMeta.BackupLastTS), nil
	default:
		return models.TimestampFromStr(arg)
	}
}

func init() {
	cmd.AddCommand(oplogReplayCmd)
}
