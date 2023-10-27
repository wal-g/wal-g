package functests

import (
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
)

func SetupMongodbBinaryBackupSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^we create binary mongo-backup on ([^\s]*)$`, tctx.createMongoBinaryBackup)
	ctx.Step(`^we restore binary mongo-backup #(\d+) to ([^\s]+)`, tctx.restoreMongoBinaryBackupAsNonInitialized)
	ctx.Step(`^we restore rs from binary mongo-backup #(\d+) to ([^\s]+)`,
		tctx.restoreMongoReplSetBinaryBackupAsNonInitialized)
	ctx.Step(`^we restore initialized binary mongo-backup #(\d+) to ([^\s]+)`,
		tctx.restoreMongoBinaryBackupAsInitialized)
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

func (tctx *TestContext) restoreMongoReplSetBinaryBackupAsNonInitialized(backupNumber int, containers string) error {
	containerNames := strings.Split(containers, ",")
	return tctx.restoreMongoReplSetBinaryBackup(backupNumber, containerNames)
}

func (tctx *TestContext) restoreMongoBinaryBackupAsInitialized(backupNumber int, container string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true)
}

func (tctx *TestContext) restoreMongoReplSetBinaryBackup(backupNumber int, containerNames []string) error {
	var walgList []*helpers.WalgUtil
	var mongoCtlList []*helpers.MongoCtl
	var backup string
	var mongodbVersion string
	var configPath string

	for _, container := range containerNames {
		walg := WalgUtilFromTestContext(tctx, container)
		walgList = append(walgList, walg)

		if len(backup) == 0 {
			backupName, err := walg.GetBackupByNumber(backupNumber)
			if err != nil {
				return err
			}
			backup = backupName
		}

		mongoCtl, err := MongoCtlFromTestContext(tctx, container)
		if err != nil {
			return err
		}
		mongoCtlList = append(mongoCtlList, mongoCtl)

		version, err := mongoCtl.GetVersion()
		if err != nil {
			return err
		}
		mongodbVersion = version

		confPath, err := mongoCtl.GetConfigPath()
		if err != nil {
			return err
		}
		configPath = confPath
	}

	for _, mongoCtl := range mongoCtlList {
		err := mongoCtl.StopMongod()
		if err != nil {
			return err
		}
	}

	var rsMemberList []string
	for i, mongoCtl := range mongoCtlList {
		rsMember := fmt.Sprintf("%s:%d", containerNames[i], mongoCtl.GetMongodPort())
		rsMemberList = append(rsMemberList, rsMember)
	}
	rsName := "mongodb01"
	rsMembers := strings.Join(rsMemberList, ",")

	for _, walg := range walgList {
		err := walg.FetchBinaryBackup(backup, configPath, mongodbVersion, rsName, rsMembers)
		if err != nil {
			return err
		}
	}

	for _, mongoCtl := range mongoCtlList {
		if err := mongoCtl.ChownDBPath(); err != nil {
			return err
		}

		if err := mongoCtl.StartMongod(); err != nil {
			return err
		}
	}

	time.Sleep(30 * time.Minute)

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
