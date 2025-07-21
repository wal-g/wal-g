package mongo

import (
	"context"
	"fmt"
	"github.com/wal-g/tracelog"
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
	whitelist, blacklist []string,
) error {
	config, err := binary.CreateMongodConfig(mongodConfigPath)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Println("t1")
	localStorage := binary.CreateLocalStorage(config.GetDBPath())
	tracelog.InfoLogger.Println("t2")
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")
	tracelog.InfoLogger.Println("t3")
	if minimalConfigPath == "" {
		minimalConfigPath, err = config.SaveConfigToTempFile("storage", "systemLog")
		if err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Println("t4")
	restoreService, err := binary.CreateRestoreService(ctx, localStorage, uploader, minimalConfigPath)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Println("t5")
	rsConfig := binary.NewRsConfig(rsName, rsMembers, rsMemberIDs)
	if err = rsConfig.Validate(); err != nil {
		return err
	}
	tracelog.InfoLogger.Println("t6")
	shConfig := binary.NewShConfig(shardName, mongoCfgConnectionString)
	if err = shConfig.Validate(); err != nil {
		return fmt.Errorf("ShConfig validation failed: %v", err)
	}
	tracelog.InfoLogger.Println("t7")
	mongocfgConfig, err := binary.NewMongoCfgConfig(shardConnectionStrings)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Println("t8")
	// check backup existence and resolve flag LATEST
	backup, err := internal.GetBackupByName(backupName, "", uploader.Folder())
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Println("t9")

	var replyOplogConfig binary.ReplyOplogConfig
	if pitrSince != "" && pitrUntil != "" {
		replyOplogConfig, err = binary.NewReplyOplogConfig(pitrSince, pitrUntil)
		if err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Println("t10")

	return restoreService.DoRestore(
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
