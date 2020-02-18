package functests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"time"

	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/mongoload"
	"github.com/wal-g/wal-g/tests_func/mongoload/models"

	"github.com/DATA-DOG/godog/gherkin"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
)

type TestingfWrap func(format string, args ...interface{})

func (tf TestingfWrap) Errorf(format string, args ...interface{}) {
	tf(format, args)
}

func (tctx *TestContext) sameDataCheck(dataId1, dataId2 string) error {
	if snap1, ok := tctx.AuxData.Snapshots[dataId1]; ok {
		if snap2, ok := tctx.AuxData.Snapshots[dataId2]; ok {
			if assert.Equal(TestingfWrap(tracelog.ErrorLogger.Printf), snap1, snap2) {
				return nil
			}
			return fmt.Errorf("same snapshots expected (%s) == (%s)", dataId1, dataId2)
		}
		return fmt.Errorf("no snapshot is saved for with id %s", dataId2)
	}
	return fmt.Errorf("no snapshot is saved for with id %s", dataId1)
}

func (tctx *TestContext) createBackup(container string) error {
	// backups may have the same name if less than one second has passed since the last backup
	before := time.Now()
	passed := before.Sub(tctx.AuxData.PreviousBackupTime)
	if passed < time.Second {
		time.Sleep(time.Second - passed)
	}

	walg := WalgUtilFromTestContext(tctx, container)
	backupId, err := walg.PushBackup()
	if err != nil {
		return err
	}
	tctx.AuxData.PreviousBackupTime = before
	tctx.AuxData.CreatedBackupNames = append(tctx.AuxData.CreatedBackupNames, backupId)
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

func (tctx *TestContext) oplogArchiveIsNotEmpty() error {
	s3 := S3StorageFromTestContext(tctx, tctx.S3Host())

	return helpers.Retry(tctx.Context, 10, func() error {
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

func (tctx *TestContext) checkBackupsCount(backupCount int, container string) error {
	walg := WalgUtilFromTestContext(tctx, container)
	backups, err := walg.Backups()
	if err != nil {
		return err
	}
	if len(backups) != backupCount {
		return fmt.Errorf("expected %d number of backups, but found %d", backupCount, len(backups))
	}
	return nil
}

func (tctx *TestContext) purgeBackupRetain(retainCount int, container string) error {
	walg := WalgUtilFromTestContext(tctx, container)
	return walg.PurgeRetain(retainCount)
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

func (tctx *TestContext) purgeBackupsAfterTime(retainCount int, timestampId string, container string) error {
	walg := WalgUtilFromTestContext(tctx, container)
	afterTime := time.Unix(int64(tctx.AuxData.Timestamps[timestampId].TS), 0)
	return walg.PurgeAfterTime(retainCount, afterTime)
}

func (tctx *TestContext) purgeBackupsAfterID(retainCount int, afterBackupId int, container string) error {
	walg := WalgUtilFromTestContext(tctx, container)
	return walg.PurgeAfterNum(retainCount, afterBackupId)
}

func (tctx *TestContext) purgeDataDir(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	return mc.PurgeDatadir()
}

func (tctx *TestContext) saveSnapshot(host, dataId string) error {
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

func (tctx *TestContext) backupMetadataContains(container string, backupId int, expectedMeta *gherkin.DocString) error {

	s3client, err := S3StorageFromTestContext(tctx, tctx.S3Host()).Client()
	if err != nil {
		return err
	}

	walg := WalgUtilFromTestContext(tctx, container)
	backups, err := walg.Backups()
	if err != nil {
		return err
	}
	if backupId >= len(backups) {
		return fmt.Errorf("cannot get backup #%d, there are only %d", backupId, len(backups))
	}

	backup := backups[backupId]
	path := fmt.Sprintf("%s/basebackups_005/%s_backup_stop_sentinel.json", tctx.Env["WALG_S3_PREFIX"], backup)
	contents, err := s3client.FileContents(path)
	if err != nil {
		return err
	}

	expected := helpers.Sentinel{}
	err = json.Unmarshal([]byte(expectedMeta.Content), &expected)
	if err != nil {
		return err
	}

	given := helpers.Sentinel{}
	err = json.Unmarshal(contents, &given)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(expected, given) {
		return fmt.Errorf("error: expected metadata %v, given %v", expected, given)
	}
	return nil
}

func (tctx *TestContext) testMongoConnect(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	return helpers.Retry(tctx.Context, 10, func() error {
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

func (tctx *TestContext) configureS3(host string) error {
	return S3StorageFromTestContext(tctx, host).InitMinio()
}

func (tctx *TestContext) initiateReplSet(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	if err := helpers.Retry(tctx.Context, 10, mc.InitReplSet); err != nil {
		return err
	}

	return nil
}

func (tctx *TestContext) isPrimary(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	return helpers.Retry(tctx.Context, 10, func() error {
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

func (tctx *TestContext) enableAuth(host string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	return mc.EnableAuth()
}

func (tctx *TestContext) loadMongodbOpsFromConfig(host string, loadId string) error {

	ammoFile, err := os.Open(path.Join("config", loadId, "config.json"))
	if err != nil {
		return err
	}
	defer func() { _ = ammoFile.Close() }()

	expectedFile, err := os.Open(path.Join("config", loadId, "expected.json"))
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

	return helpers.Retry(tctx.Context, 10, func() error {
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

	return mc.WriteTestData(fmt.Sprintf("test%02d", testId))
}

func (tctx *TestContext) putEmptyBackupViaMinio(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, tctx.Env["TEST_ID"])
	backupName := "20010203T040506"
	bucketName := tctx.Env["S3_BUCKET"]
	backupRootDir := tctx.Env["WALG_S3_PREFIX"]
	backupDir := "/export/" + bucketName + "/" + backupRootDir + "/" + backupName
	backupDumpPath := filepath.Join(backupDir, "mongodump.archive")
	tctx.AuxData.NometaBackupNames = append(tctx.AuxData.NometaBackupNames, backupName)
	_, err := helpers.RunCommand(tctx.Context, containerName, []string{"mkdir", "-p", backupDir})
	if err != nil {
		return err
	}
	_, err = helpers.RunCommand(tctx.Context, containerName, []string{"touch", backupDumpPath})
	if err != nil {
		return err
	}
	return nil
}

func (tctx *TestContext) testEmptyBackupsViaMinio(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, tctx.Env["TEST_ID"])
	bucketName := tctx.Env["S3_BUCKET"]
	backupRootDir := tctx.Env["WALG_S3_PREFIX"]
	backupNames := tctx.AuxData.NometaBackupNames
	for _, backupName := range backupNames {
		backupDir := filepath.Join("/export", bucketName, backupRootDir, backupName)
		_, err := helpers.RunCommand(tctx.Context, containerName, []string{"ls", backupDir})
		if err != nil {
			return err
		}
	}
	return nil
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
	snap2, err := mc2.Snapshot()
	if err != nil {
		return err
	}

	if !assert.Equal(TestingfWrap(tracelog.ErrorLogger.Printf), snap1, snap2) {
		return fmt.Errorf("expected the same data at hosts %s and %s", host1, host2)
	}

	return nil
}
