package functests

import (
	"fmt"
	"github.com/cucumber/godog"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
)

func SetupMongodbBinaryBackupSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^we create binary mongo-backup on ([^\s]*)$`, tctx.createMongoBinaryBackup)
	ctx.Step(`^we restore binary mongo-backup #(\d+) to ([^\s]+)`, tctx.restoreMongoBinaryBackupAsNonInitialized)
	ctx.Step(`^we restore initialized binary mongo-backup #(\d+) to ([^\s]+)`,
		tctx.restoreMongoBinaryBackupAsInitialized)
	ctx.Step(`^we restore mongo-backup #(\d+) to ([^\s]+) with whitelist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithWhitelist)
	ctx.Step(`^we restore mongo-backup #(\d+) to ([^\s]+) with blacklist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithBlacklist)
	ctx.Step(`^we restore mongo-backup #(\d+) to ([^\s]+) with whitelist "([^"]*)" and blacklist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithWhitelistAndBlacklist)
	ctx.Step(`^we restore non-initialized mongo-backup #(\d+) to ([^\s]+) with whitelist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithWhitelistAsNonInitialized)
	ctx.Step(`^journal info count is #(\d+)$`,
		tctx.checkJournals)
}

func (tctx *TestContext) createMongoBinaryBackup(container string) error {
	host := tctx.ContainerFQDN(container)

	walg := WalgUtilFromTestContext(tctx, container)
	err := walg.PushBinaryBackup()
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Backup created")

	tctx.PreviousBackupTime, err = helpers.TimeInContainer(tctx.Context, host)
	if err != nil {
		return err
	}

	return nil
}

func (tctx *TestContext) restoreMongoBinaryBackupAsNonInitialized(backupNumber int, container string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, false, "", "")
}

func (tctx *TestContext) restoreMongoBinaryBackupAsInitialized(backupNumber int, container string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, "", "")
}

func (tctx *TestContext) restoreMongoBinaryBackupWithWhitelist(backupNumber int, container, whitelist string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, whitelist, "")
}

func (tctx *TestContext) restoreMongoBinaryBackupWithBlacklist(backupNumber int, container, blacklist string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, "", blacklist)
}

func (tctx *TestContext) restoreMongoBinaryBackupWithWhitelistAndBlacklist(
	backupNumber int, container, whitelist, blacklist string,
) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, whitelist, blacklist)
}

func (tctx *TestContext) restoreMongoBinaryBackupWithWhitelistAsNonInitialized(
	backupNumber int, container, whitelist string,
) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, false, whitelist, "")
}

func (tctx *TestContext) restoreMongoBinaryBackup(
	backupNumber int, container string, initialized bool, whitelist, blacklist string,
) error {
	walg := WalgUtilFromTestContext(tctx, container)

	backup, err := walg.GetBackupByNumber(backupNumber)
	if err != nil {
		return err
	}

	mc, err := MongoCtlFromTestContext(tctx, container)
	if err != nil {
		return err
	}

	mongodbVersion, err := mc.GetVersion()
	if err != nil {
		return err
	}

	configPath, err := mc.GetConfigPath()
	if err != nil {
		return err
	}

	err = mc.StopMongod()
	if err != nil {
		return err
	}

	rsName := ""
	rsMembers := ""
	if initialized {
		rsName = container
		rsMembers = fmt.Sprintf("%s:%d", container, mc.GetMongodPort())
	}
	err = walg.FetchBinaryBackup(backup, configPath, mongodbVersion, rsName, rsMembers, whitelist, blacklist)
	if err != nil {
		return err
	}

	if err := mc.ChownDBPath(); err != nil {
		return err
	}

	if err := mc.StartMongod(); err != nil {
		return err
	}

	if !initialized {
		if err := tctx.initiateReplSet(container); err != nil {
			return err
		}
	} else {
		tracelog.DebugLogger.Println("Skip initiateReplSet")
	}

	return nil
}

func (tctx *TestContext) checkJournals(count int) error {
	s3client, err := S3StorageFromTestContext(tctx, tctx.S3Host()).Client()
	if err != nil {
		return err
	}

	contents, err := s3client.List("mongodb-backup/test_uuid/test_mongodb/basebackups_005/journal_")
	if err != nil {
		return err
	}

	if len(contents) != count {
		return fmt.Errorf("expected %d journals, got %v", count, contents)
	}
	return nil
}
