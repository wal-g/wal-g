package functests

import (
	"fmt"
	"github.com/stretchr/testify/assert"

	"github.com/cucumber/godog"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
)

func SetupMongodbBinaryBackupSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^we create binary mongo-backup on ([^\s]*)$`, tctx.createMongoBinaryBackup)
	ctx.Step(`^we restore binary mongo-backup #(\d+) to ([^\s]+)`, tctx.restoreMongoBinaryBackupAsNonInitialized)
	ctx.Step(`^we restore initialized binary mongo-backup #(\d+) to ([^\s]+)`,
		tctx.restoreMongoBinaryBackupAsInitialized)
	ctx.Step(`^we restore partial mongo-backup #(\d+) to ([^\s]+) ns "([^"]*)"$`,
		tctx.partiallyRestoreMongoDBBinaryBackup)
	ctx.Step(`^we restore partial mongo-backup #(\d+) to ([^\s]+) ns "([^"]*)" with blacklist "([^"]*)"$`,
		tctx.partiallyRestoreMongoDBBinaryBackupWithBlacklist)
	ctx.Step(`^we restore partial mongo-backup #(\d+) to ([^\s]+) ns "([^"]*)" with error "([^"]*)"$`,
		tctx.partiallyRestoreMongoDBBinaryBackupWithError)
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
	return tctx.restoreMongoBinaryBackup(backupNumber, container, false)
}

func (tctx *TestContext) restoreMongoBinaryBackupAsInitialized(backupNumber int, container string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true)
}

func (tctx *TestContext) partiallyRestoreMongoDBBinaryBackup(backupNumber int, container, paths string) error {
	return tctx.partiallyRestoreMongoDBBinaryBackupImpl(backupNumber, container, paths, "", "")
}

func (tctx *TestContext) partiallyRestoreMongoDBBinaryBackupWithBlacklist(
	backupNumber int,
	container, paths, blacklist string,
) error {
	return tctx.partiallyRestoreMongoDBBinaryBackupImpl(backupNumber, container, paths, blacklist, "")
}

func (tctx *TestContext) partiallyRestoreMongoDBBinaryBackupWithError(
	backupNumber int,
	container, paths, errMsg string,
) error {
	return tctx.partiallyRestoreMongoDBBinaryBackupImpl(backupNumber, container, paths, "", errMsg)
}

func (tctx *TestContext) partiallyRestoreMongoDBBinaryBackupImpl(backupNumber int,
	container, paths, blacklist, errMsg string,
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

	err = walg.PartialRestore(backup, configPath, mongodbVersion, paths, blacklist)
	if err != nil {
		if errMsg != "" {
			if !assert.ErrorContains(TestingfWrap(tracelog.ErrorLogger.Printf), err, errMsg) {
				return fmt.Errorf("error expected to contain \"%s\" but was \"%s\"", errMsg, err)
			}
		} else {
			return err
		}
	}

	if err = mc.DeleteMongodReplSetSetting(); err != nil {
		return err
	}

	if err = mc.ChownDBPath(); err != nil {
		return err
	}

	if err = mc.StartMongod(); err != nil {
		return err
	}

	return nil
}

func (tctx *TestContext) restoreMongoBinaryBackup(backupNumber int, container string, initialized bool) error {
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
	err = walg.FetchBinaryBackup(backup, configPath, mongodbVersion, rsName, rsMembers)
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
