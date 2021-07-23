package functests

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/utils"
)

func (tctx *TestContext) isWorkingRedis(hostName string) error {
	redisCtl, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		tracelog.DebugLogger.Printf("Redis client connect to host '%s'", redisCtl.Addr())

		status := redisCtl.Ping()
		err = status.Err()
		if err != nil {
			return fmt.Errorf("Client on ping returned err: %v\n", err)
		}
		if status.Val() != "PONG" {
			return fmt.Errorf("Client on ping does not returned PONG: %v\n", err)
		}
		tracelog.DebugLogger.Printf("Redis: Got PONG on PING from %s", hostName)
		return nil
	})
}

func (tctx *TestContext) redisHasTestRedisDataTest(host string, testId int) error {
	rc, err := GetRedisCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	docsCount := 10
	err = rc.WriteTestData(fmt.Sprintf("test%02d", testId), docsCount)
	if err != nil {
		return err
	}
	return nil
}

func (tctx *TestContext) createRedisBackup(host string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, host)
	if err != nil {
		return nil
	}

	beforeBackupTime, err := helpers.TimeInContainer(tctx.Context, rc.Host())
	if err != nil {
		return err
	}

	passed := beforeBackupTime.Sub(tctx.PreviousBackupTime)
	if passed < time.Second {
		cmd := []string{"sleep", "1"}
		if _, err := helpers.RunCommandStrict(tctx.Context, host, cmd); err != nil {
			return err
		}
	}

	tracelog.DebugLogger.Println("Push redis backup")
	backupId, err := rc.PushBackup()
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Second)
	tracelog.DebugLogger.Println("Backup created: ", backupId)
	return nil
}

func (tctx *TestContext) weDeleteRedisBackupsRetainViaRedis(backupsRetain int, host string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	return rc.PurgeRetain(backupsRetain)
}

func (tctx *TestContext) weRestartRedisServerAt(host string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	cmd := rc.ShutdownNoSave()
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

func (tctx *TestContext) testEqualRedisDataAtHosts(host1, host2 string) error {
	rc1, err := GetRedisCtlFromTestContext(tctx, host1)
	if err != nil {
		return err
	}

	rc2, err := GetRedisCtlFromTestContext(tctx, host2)
	if err != nil {
		return err
	}

	dbsize1 := rc1.DBSize()
	if dbsize1.Err() != nil {
		return errors.Wrapf(dbsize1.Err(), "Host %s doesn't return 'dbsize'", host1)
	}
	dbsize2 := rc2.DBSize()
	if dbsize2.Err() != nil {
		return errors.Wrapf(dbsize1.Err(), "Host %s doesn't return 'dbsize'", host2)
	}
	if dbsize1.Val() != dbsize2.Val() {
		return fmt.Errorf("hosts %s and %s have not equal keys count %d != %d", host1, host2, dbsize1.Val(), dbsize2.Val())
	}

	keys1 := rc1.Keys("*")
	if keys1.Err() != nil {
		return keys1.Err()
	}

	keys2 := rc2.Keys("*")
	if keys2.Err() != nil {
		return keys2.Err()
	}

	if len(keys1.Val()) == 0 || len(keys2.Val()) == 0 {
		return fmt.Errorf("keys1 or keys2 is empty - broken backup")
	}

	if !utils.IsArraysEqual(keys1.Val(), keys2.Val()) {
		return fmt.Errorf("keys from redis1/redis2 aren't equal")
	}
	values1 := rc1.MGet(keys1.Val()...)
	values2 := rc1.MGet(keys2.Val()...)
	vals1 := make([]string, len(values1.Val()))
	vals2 := make([]string, len(values1.Val()))

	for i, val := range values1.Val() {
		vals1[i] = val.(string)
	}

	for i, val := range values2.Val() {
		vals2[i] = val.(string)
	}
	if !utils.IsArraysEqual(vals1, vals2) {
		return fmt.Errorf("values from redis1/redis2 aren't equal")
	}
	return nil
}
