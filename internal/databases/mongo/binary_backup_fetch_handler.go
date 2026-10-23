package mongo

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/utility"
)

func HandleBinaryFetchPush(
	ctx context.Context,
	mongodConfigPath, minimalConfigPath, backupName, restoreMongodVersion, rsName string,
	rsMembers []string,
	rsMemberIDs []int,
	shardName, mongoCfgConnectionString string,
	shardConnectionStrings []string,
	skipBackupDownload, skipReconfig, skipChecks bool,
	pitrSince, pitrUntil string,
	pitrFollowLatest bool,
	whitelist, blacklist []string,
) error {
	config, err := binary.CreateMongodConfig(mongodConfigPath)
	if err != nil {
		return err
	}

	localStorage := binary.CreateLocalStorage(config.GetDBPath())
	uploader, err := internal.ConfigureUploader(ctx)
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	if minimalConfigPath == "" {
		minimalConfigPath, err = config.SaveConfigToTempFile("storage", "systemLog")
		if err != nil {
			return err
		}
	}

	restoreService, err := binary.CreateRestoreService(localStorage, uploader, minimalConfigPath)
	if err != nil {
		return err
	}

	rsConfig := binary.NewRsConfig(rsName, rsMembers, rsMemberIDs)
	if err = rsConfig.Validate(); err != nil {
		return err
	}

	shConfig := binary.NewShConfig(shardName, mongoCfgConnectionString)
	if err = shConfig.Validate(); err != nil {
		return fmt.Errorf("ShConfig validation failed: %v", err)
	}

	mongocfgConfig, err := binary.NewMongoCfgConfig(shardConnectionStrings)
	if err != nil {
		return err
	}

	// check backup existence and resolve flag LATEST
	backup, err := internal.GetBackupByName(ctx, backupName, "", uploader.Folder())
	if err != nil {
		return err
	}

	replyOplogConfig, err := buildBinaryFetchReplayConfig(
		ctx, pitrSince, pitrUntil, pitrFollowLatest, len(whitelist)+len(blacklist) > 0)
	if err != nil {
		return err
	}

	return restoreService.DoRestore(
		ctx,
		rsConfig,
		shConfig,
		mongocfgConfig,
		replyOplogConfig,
		binary.RestoreArgs{
			BackupName:     backup.Name,
			RestoreVersion: restoreMongodVersion,

			SkipChecks:         skipChecks,
			SkipBackupDownload: skipBackupDownload,
			SkipMongoReconfig:  skipReconfig,

			Whitelist: whitelist,
			Blacklist: blacklist,
		},
	)
}

func buildBinaryFetchReplayConfig(
	ctx context.Context,
	pitrSince, pitrUntil string,
	followLatest, partial bool,
) (binary.ReplyOplogConfig, error) {
	if followLatest && (pitrSince == "" || pitrUntil == "") {
		return binary.ReplyOplogConfig{}, fmt.Errorf(
			"pitr-follow-latest requires both pitr-since and pitr-until")
	}
	if pitrSince == "" || pitrUntil == "" {
		return binary.ReplyOplogConfig{}, nil
	}
	return binary.NewReplyOplogConfig(
		ctx, pitrSince, pitrUntil, partial, false, followLatest, "")
}
