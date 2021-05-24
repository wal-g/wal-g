package functests

import (
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
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
