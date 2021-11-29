package functests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/mongodb/mongoload"
	"github.com/wal-g/wal-g/tests_func/mongodb/mongoload/models"
)

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
	mc2, err := MongoCtlFromTestContext(tctx, host1)
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

	expectedData, err := ioutil.ReadAll(expectedFile)
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

	return mc.PurgeDatadir()
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
	from := backupMeta.MongoMeta.Before.LastMajTS
	until := tctx.AuxData.Timestamps[timestampId]
	tracelog.DebugLogger.Printf("Saved timestamps:\nbackup #%d majTs: %v\n%s: %v\n", backupId, from, timestampId, until)
	until.Inc++

	s3 := S3StorageFromTestContext(tctx, tctx.S3Host())
	tracelog.DebugLogger.Printf("Waiting until ts %v appears in storage", until)

	err = helpers.Retry(tctx.Context, 30, func() error {
		exists, err := s3.ArchTsExists(until)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("ts %v does not exists", until)
		}
		return nil
	})
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Printf("Starting oplog replay from %v until %v", from, until)
	return walg.OplogReplay(from, until)
}
