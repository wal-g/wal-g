package functests

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
	"strings"

	"github.com/cucumber/godog"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/mongodb/mongoload"
	"github.com/wal-g/wal-g/tests_func/mongodb/mongoload/models"
)

func SetupMongodbSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^a working mongodb on ([^\s]*)$`, tctx.testMongoConnect)
	ctx.Step(`^mongodb replset initialized on ([^\s]*)$`, tctx.initiateReplSet)
	ctx.Step(`^mongodb replset is synchronized on ([^\s]*)$`, tctx.waitSecondariesSync)
	ctx.Step(`^mongodb role is primary on ([^\s]*)$`, tctx.isMongoPrimary)
	ctx.Step(`^mongodb auth initialized on ([^\s]*)$`, tctx.mongoEnableAuth)
	ctx.Step(`^mongodb initialized on ([^\s]*)$`, tctx.mongoInit)
	ctx.Step(`^([^\s]*) has no data$`, tctx.purgeMongoDataDir)
	ctx.Step(`^([^\s]+) replset has no data$`, tctx.purgeMongoRsDataDir)

	ctx.Step(`^([^\s]*) has test mongodb data test(\d+)$`, tctx.fillMongodbWithTestData)
	ctx.Step(`^([^\s]*) has been loaded with "([^"]*)"$`, tctx.loadMongodbOpsFromConfig)
	ctx.Step(`^we got same mongodb data at ([^\s]*) ([^\s]*)$`, tctx.testEqualMongodbDataAtHosts)
	ctx.Step(`^we have same data in "([^"]*)" and "([^"]*)"$`, tctx.sameDataCheck)
	ctx.Step(`^we save ([^\s]*) data "([^"]*)"$`, tctx.saveMongoSnapshot)

	ctx.Step(`^we delete mongo backups retain (\d+) via ([^\s]*)$`, tctx.purgeBackupRetain)

	ctx.Step(`^we got (\d+) backup entries of ([^\s]*)$`, tctx.checkBackupsCount)
	ctx.Step(`^we delete mongo backup #(\d+) via ([^\s]*)$`, tctx.deleteMongoBackup)
	ctx.Step(`^we ensure ([^\s]*) #(\d+) backup metadata contains$`, tctx.backupMetadataContains)
	ctx.Step(`^we put empty backup via ([^\s]*) to ([^\s]*)$`, tctx.putEmptyBackupViaMinio)
	ctx.Step(`^we check if empty backups were purged via ([^\s]*)$`, tctx.testEmptyBackupsViaMinio)

	SetupMongodbLogicalSteps(ctx, tctx)
}

func SetupMongodbLogicalSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^we create ([^\s]*) mongo-backup$`, tctx.createMongoBackup)
	ctx.Step(`^we restore #(\d+) backup to ([^\s]*)$`, tctx.restoreBackupToMongodb)
	ctx.Step(`^we restore rs from #(\d+) backup to "([^"]*)" timestamp to ([^\s]+)$`,
		tctx.replayReplSetOplog)

	// oplog
	ctx.Step(`we save last oplog timestamp on ([^\s]*) to "([^"]*)"`, tctx.saveOplogTimestamp)
	ctx.Step(`let's wait new oplog after "([^"]*)"`, tctx.waitNewOplogAfter)
	ctx.Step(`^at least one oplog archive exists in storage$`, tctx.oplogArchiveIsNotEmpty)
	ctx.Step(`^we purge oplog archives via ([^\s]*)$`, tctx.purgeOplogArchives)
	ctx.Step(`^oplog archiving is enabled on ([^\s]*)$`, tctx.enableOplogPush)
	ctx.Step(`^we restore from #(\d+) backup to "([^"]*)" timestamp to ([^\s]*)$`, tctx.replayOplog)

	ctx.Step(`^mongodb doesn't have initial sync on ([^\s]+)$`, tctx.checkInitialSync)
}

func (tctx *TestContext) createMongoBackup(container string) error {
	host := tctx.ContainerFQDN(container)
	beforeBackupTime, err := helpers.TimeInContainer(tctx.Context, host)
	if err != nil {
		return err
	}

	passed := beforeBackupTime.Sub(tctx.PreviousBackupTime)
	if passed < time.Second {
		time.Sleep(time.Second)
	}

	walg := WalgUtilFromTestContext(tctx, container)
	backupID, err := walg.PushBackup()
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Backup created: ", backupID)

	afterBackupTime, err := helpers.TimeInContainer(tctx.Context, host)
	if err != nil {
		return err
	}

	tctx.PreviousBackupTime = afterBackupTime
	tctx.AuxData.CreatedBackupNames = append(tctx.AuxData.CreatedBackupNames, backupID)
	return nil
}

func (tctx *TestContext) oplogArchiveIsNotEmpty() error {
	s3 := S3StorageFromTestContext(tctx, tctx.S3Host())

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		archives, err := s3.Archives()
		if err != nil {
			return err
		}
		if len(archives) < 1 {
			return fmt.Errorf("oplog archives are not exist")
		}
		return nil
	})
}


func (tctx *TestContext) testEqualMongodbDataAtHosts(host1, host2 string) error {
	mc1, err := MongoCtlFromTestContext(tctx, host1)
	if err != nil {
		return err
	}
	mc2, err := MongoCtlFromTestContext(tctx, host2)
	if err != nil {
		return err
	}

	snap1, err := mc1.Snapshot()
	if err != nil {
		return err
	}
	if !assert.NotEmpty(TestingfWrap(tracelog.ErrorLogger.Printf), snap1) {
		return fmt.Errorf("host %s snapshot is empty: %+v", host1, snap1)
	}

	snap2, err := mc2.Snapshot()
	if err != nil {
		return err
	}
	if !assert.NotEmpty(TestingfWrap(tracelog.ErrorLogger.Printf), snap2) {
		return fmt.Errorf("host %s snapshot is empty: %+v", host2, snap2)
	}

	if !assert.Equal(TestingfWrap(tracelog.ErrorLogger.Printf), snap1, snap2) {
		return fmt.Errorf("expected the same data at hosts %s and %s", host1, host2)
	}

	return nil
}

func (tctx *TestContext) loadMongodbOpsFromConfig(host string, loadId string) error {
	ammoFile, err := os.Open(tctx.getMongoLoadFile(loadId, "config.json"))
	if err != nil {
		return err
	}
	defer func() { _ = ammoFile.Close() }()

	expectedFile, err := os.Open(tctx.getMongoLoadFile(loadId, "expected.json"))
	if err != nil {
		return err
	}
	defer func() { _ = expectedFile.Close() }()

	expectedData, err := io.ReadAll(expectedFile)
	if err != nil {
		return err
	}
	var expectedStat models.LoadStat
	if err := json.Unmarshal(expectedData, &expectedStat); err != nil {
		return err
	}

	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	client, err := mc.AdminConnect()
	if err != nil {
		return err
	}

	currentStat, err := mongoload.HandleLoad(tctx.Context, ammoFile, client, 1)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Printf("load stat: %+v", currentStat)

	if err := currentStat.GetError(); err != nil {
		return err
	}

	if !assert.Equal(TestingfWrap(tracelog.ErrorLogger.Printf), expectedStat, currentStat) {
		return fmt.Errorf("expected and current stat are different")
	}

	tsLast, err := mc.LastTS()
	if err != nil {
		return err
	}

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		tsMaj, err := mc.LastMajTS()
		if err != nil {
			return err
		}
		if helpers.LessTS(tsMaj, tsLast) {
			return fmt.Errorf("last maj (%v) < last ts (%v)", tsLast, tsMaj)
		}
		tracelog.DebugLogger.Printf("last ts (%v) == last maj (%v)\n", tsLast, tsMaj)

		return nil
	})
}

func (tctx *TestContext) fillMongodbWithTestData(host string, testId int) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	dbCount, tablesCount, rowsCount := 2, 2, 3
	return mc.WriteTestData(fmt.Sprintf("test%02d", testId), dbCount, tablesCount, rowsCount)
}

func (tctx *TestContext) testMongoConnect(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		conn, err := mc.Connect(nil)
		if err != nil {
			return err
		}
		if err := conn.Ping(tctx.Context, nil); err != nil {
			return err
		}
		return nil
	})
}

func (tctx *TestContext) initiateReplSet(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	if err := helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, mc.InitReplSet); err != nil {
		return err
	}

	return nil
}

func (tctx *TestContext) isMongoPrimary(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		isMaster, err := mc.IsMaster()
		if err != nil {
			return err
		}
		if !isMaster {
			return fmt.Errorf("is not master")
		}
		return nil
	})
}

func (tctx *TestContext) mongoInit(host string) error {
	if err := tctx.testMongoConnect(host); err != nil {
		return err
	}
	if err := tctx.initiateReplSet(host); err != nil {
		return err
	}
	if err := tctx.isMongoPrimary(host); err != nil {
		return err
	}
	if err := tctx.mongoEnableAuth(host); err != nil {
		return err
	}

	return nil
}

func (tctx *TestContext) mongoEnableAuth(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	return mc.EnableAuth()
}

func (tctx *TestContext) purgeMongoDataDir(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	return mc.ResetMongod()
}

func (tctx *TestContext) purgeMongoRsDataDir(containers string) error {
	containerNames := strings.Split(containers, ",")
	var mongoCtlList []*helpers.MongoCtl

	for _, container := range containerNames {
		mongoCtl, err := MongoCtlFromTestContext(tctx, container)
		if err != nil {
			return err
		}
		mongoCtlList = append(mongoCtlList, mongoCtl)
	}

	for _, mongoCtl := range mongoCtlList {
		err := mongoCtl.StopMongod()
		if err != nil {
			return err
		}
	}

	for _, mongoCtl := range mongoCtlList {
		err := mongoCtl.PurgeDatadir()
		if err != nil {
			return err
		}
	}

	for _, mongoCtl := range mongoCtlList {
		err := mongoCtl.StartMongod()
		if err != nil {
			return err
		}
	}

	return nil
}

func (tctx *TestContext) saveMongoSnapshot(host, dataId string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	snapshot, err := mc.Snapshot()
	if err != nil {
		return err
	}

	tctx.AuxData.Snapshots[dataId] = snapshot
	return nil
}

func (tctx *TestContext) saveOplogTimestamp(host, timestampId string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	ts, err := mc.LastMajTS()
	if err != nil {
		return err
	}
	tctx.AuxData.Timestamps[timestampId] = ts
	return nil
}

func (tctx *TestContext) waitNewOplogAfter(timestampId string) error {
	until := tctx.AuxData.Timestamps[timestampId]
	until.Inc++

	s3 := S3StorageFromTestContext(tctx, tctx.S3Host())
	tracelog.DebugLogger.Printf("Waiting until ts %v appears in storage", until)

	return helpers.Retry(tctx.Context, 30, func() error {
		archives, err := s3.Archives()
		if err != nil {
			return err
		}
		tracelog.DebugLogger.Printf("Current timestmaps %v", archives)

		exists, err := s3.ArchTsExists(until)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("ts %v does not exists", until)
		}
		return nil
	})
}

func (tctx *TestContext) enableOplogPush(container string) error {
	if tctx.AuxData.OplogPushEnabled {
		return nil
	}
	tctx.AuxData.OplogPushEnabled = true
	walg := WalgUtilFromTestContext(tctx, container)
	go func() {
		err := walg.OplogPush() // TODO: run in background with supervisord?
		tracelog.DebugLogger.Println(err)
	}()
	return nil
}

func (tctx *TestContext) purgeOplogArchives(container string) error {
	walg := WalgUtilFromTestContext(tctx, container)
	return walg.OplogPurge()
}

func (tctx *TestContext) restoreBackupToMongodb(backupNum int, container string) error {
	walg := WalgUtilFromTestContext(tctx, container)
	return walg.FetchBackupByNum(backupNum)
}

func (tctx *TestContext) replayOplog(backupId int, timestampId string, container string) error {
	walg := WalgUtilFromTestContext(tctx, container)

	backupMeta, err := walg.BackupMeta(backupId)
	if err != nil {
		return fmt.Errorf("can not retrieve backup #%d metadata: %v", backupId, err)
	}
	from := backupMeta.MongoMeta.GetBackupLastTS()
	until := tctx.AuxData.Timestamps[timestampId]
	tracelog.DebugLogger.Printf("Saved timestamps:\nbackup #%d majTs: %v\n%s: %v\n", backupId, from, timestampId, until)
	until.Inc++

	err = tctx.waitNewOplogAfter(timestampId)
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Printf("Starting oplog replay from %v until %v", from, until)
	return walg.OplogReplay(from, until)
}


func (tctx *TestContext) replayReplSetOplog(backupNumber int, timestampId string, containers string) error {
	containerNames := strings.Split(containers, ",")

	for _, container := range containerNames {
		mc, err := MongoCtlFromTestContext(tctx, container)

		isMaster, err := mc.IsMaster()
		if err != nil {
			return err
		}
		if isMaster {
			return tctx.replayOplog(backupNumber, timestampId, container)
		}
	}

	return nil
}


func (tctx *TestContext) checkInitialSync(container string) error {
	mongoCtl, err := MongoCtlFromTestContext(tctx, container)
	if err != nil {
		return err
	}

	markersOfInitialSync := []string {"Initial sync required", "Initial sync started"}

	for _, text := range markersOfInitialSync {
		logs, err := mongoCtl.GrepLogs(text)
		if err != nil {
			return err
		}
		if logs != "" {
			return fmt.Errorf("Mongodb has done initial sync. Found logs: %s", logs)
		}
	}

	return nil
}


func (tctx *TestContext) waitSecondariesSync(containers string) error {
	containerNames := strings.Split(containers, ",")
	var mongoCtlList []*helpers.MongoCtl
	var masterId int

	if len(containerNames) < 2 {
		return fmt.Errorf("invalid count containers")
	}

	for _, container := range containerNames {
		mongoCtl, err := MongoCtlFromTestContext(tctx, container)
		if err != nil {
			return err
		}
		mongoCtlList = append(mongoCtlList, mongoCtl)
	}

	for i, mongoCtl := range mongoCtlList {
		isMaster, err := mongoCtl.IsMaster()
		if err != nil {
			return err
		}
		if isMaster {
			masterId = i
		}
	}

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		rsStatus, err := mongoCtlList[masterId].GetRsStatus()

		if err != nil {
			return err
		}

		for _, member := range rsStatus.Members {
			if rsStatus.Optimes.LastCommittedOpTime != member.Optime {
				return fmt.Errorf("mongos is not synchronized")
			}
		}
		return nil
	})
}
