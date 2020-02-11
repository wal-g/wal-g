package functests

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/wal-g/wal-g/tests_func/helpers"

	"github.com/DATA-DOG/godog/gherkin"
	"github.com/wal-g/tracelog"
)

func (tctx *TestContext) sameDataCheck(dataId1, dataId2 string) error {
	if data1, ok := tctx.AuxData.DatabaseSnap[dataId1]; ok {
		if data2, ok := tctx.AuxData.DatabaseSnap[dataId2]; ok {
			if !reflect.DeepEqual(data1, data2) {
				return nil
			}
			tracelog.ErrorLogger.Printf(
				"Data check failed:\nData %s:\n %+v\n\nData %s:\n %+v\n",
				dataId1, data1, dataId2, data2)
			return fmt.Errorf("expected the same data in %s and %s", dataId1, dataId2)
		}
		return fmt.Errorf("no data is saved for with id %s", dataId2)
	}
	return fmt.Errorf("no data is saved for with id %s", dataId1)
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
	s3 := S3StorageFromTestContext(tctx, tctx.S3Host())

	err = helpers.Retry(tctx.Context, 15, func() error {
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

func (tctx *TestContext) saveUserData(host, dataId string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	userData, err := mc.UserData()
	if err != nil {
		return err
	}

	tctx.AuxData.DatabaseSnap[dataId] = userData
	return nil
}

func (tctx *TestContext) saveOplogTimestamp(host, timestampId string) error {
	mc, err := MongoCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	ts, err := mc.LastTS()
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

	data1, err := mc1.UserData()
	if err != nil {
		return err
	}
	data2, err := mc2.UserData()
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(data1, data2) {
		tracelog.ErrorLogger.Printf("Captured data:\n%s:\n%+v\n\n%s:\n%+v\n", host1, data1, host2, data2)
		return fmt.Errorf("expected the same data at hosts %s and %s", host1, host2)
	}
	return nil
}
