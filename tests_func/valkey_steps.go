package functests

import (
	"fmt"
	"slices"
	"time"

	"github.com/cucumber/godog"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/utils"
)

func SetupValkeySteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^valkey stopped on ([^\s]*)$`, tctx.valkeyStoppedOn)
	ctx.Step(`^a working valkey on ([^\s]*)$`, tctx.isWorkingValkey)
	ctx.Step(`^([^\s]*) has test valkey data test(\d+)$`, tctx.valkeyHasTestValkeyDataTest)
	ctx.Step(`^we create ([^\s]*) ([^\s]*)-valkey-backup with ([^\s]*)$`, tctx.createValkeyBackup)
	ctx.Step(`^we delete valkey backups retain (\d+) via ([^\s]*)$`, tctx.weDeleteValkeyBackupsRetainViaValkey)
	ctx.Step(`^we restart valkey-server at ([^\s]*)$`, tctx.weRestartValkeyServerAt)
	ctx.Step(`^we stop valkey-server at ([^\s]*)$`, tctx.weStopValkeyServerAt)
	ctx.Step(`^we start valkey-server at ([^\s]*)$`, tctx.weStartValkeyServerAt)
	ctx.Step(`^we got same valkey data at ([^\s]*) ([^\s]*)$`, tctx.testEqualValkeyDataAtHosts)
	ctx.Step(`^([^\s]*) manifest is not empty$`, tctx.manifestIsNotEmpty)
	ctx.Step(`^([^\s]*) has heavy write$`, tctx.hasHeavyWrite)
	ctx.Step(`^we stop heavy write on ([^\s]*)$`, tctx.weStopHeavyWriteOn)
	ctx.Step(`^we restore #(\d+) aof ([^\s]*) version backup to ([^\s]*)$`, tctx.weRestoreAofBackupToValkey)
}

func (tctx *TestContext) weRestoreAofBackupToValkey(backupNum int, matchVersion string, container string) error {
	var version string
	if matchVersion == "same" {
		version = tctx.Version.Full
	} else if matchVersion == "wrong" {
		version = "5.50.50"
	} else {
		return fmt.Errorf("known options for matchVersion are same and wrong")
	}
	walg := WalgUtilFromTestContext(tctx, container)
	err := walg.FetchAofBackupByNum(backupNum, version)
	if matchVersion == "same" && err != nil {
		return err
	}
	if matchVersion == "wrong" && err == nil {
		return fmt.Errorf("expected error on wrong version")
	}
	return nil
}

func (tctx *TestContext) hasHeavyWrite(hostName string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	host := rc.Host()

	cmd := "valkey-benchmark -l -a password -t set -n 100000000 -d 1000 -r 100000"
	err = helpers.RunAsyncCommand(tctx.Context, host, cmd)
	if err != nil {
		return fmt.Errorf("heavy write cmd err: %+v", err)
	}

	return nil
}

func (tctx *TestContext) weStopHeavyWriteOn(hostName string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	host := rc.Host()

	cmd := []string{"pkill", "-fil", "-9", "valkey-benchmark"}
	_, err = helpers.RunCommandStrict(tctx.Context, host, cmd)
	if err != nil {
		cmd = []string{"ps", "wawux"}
		exc, _ := helpers.RunCommandStrict(tctx.Context, host, cmd)
		tracelog.ErrorLogger.Printf("Active process list is:\n%s\n", exc.Stdout())
		return fmt.Errorf("heavy write stop err: %+v", err)
	}

	return nil
}

func (tctx *TestContext) manifestIsNotEmpty(hostName string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	host := rc.Host()

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		cmd := []string{"stat", "--printf=\"%s\"", "/data/appendonlydir/appendonly.aof.manifest"}
		res, err := helpers.RunCommandStrict(tctx.Context, host, cmd)
		if err != nil {
			return fmt.Errorf("manifest is missing")
		}
		if res.Stdout() == "0" {
			return fmt.Errorf("manifest is empty")
		}

		return nil
	})
}

func (tctx *TestContext) valkeyStoppedOn(hostName string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	host := rc.Host()

	cmd := []string{"bash", "-c", "ps aux | grep [r]edis-server"}
	res, err := helpers.RunCommandStrict(tctx.Context, host, cmd)
	if err != nil && res.ExitCode == 1 && res.Stdout() == "" && res.Stderr() == "" {
		return nil
	}

	return fmt.Errorf("unexpected result of checking running valkey: %+v, %d: %s: %s", err, res.ExitCode, res.Stdout(), res.Stderr())
}

func (tctx *TestContext) isWorkingValkey(hostName string) error {
	valkeyCtl, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}

	return helpers.Retry(tctx.Context, MAX_RETRIES_COUNT, func() error {
		tracelog.DebugLogger.Printf("Valkey client connect to host '%s'", valkeyCtl.Addr())
		status := valkeyCtl.Ping(tctx.Context)
		err = status.Err()
		if err != nil {
			return fmt.Errorf("Client on ping returned err: %v\n", err)
		}
		if status.Val() != "PONG" {
			return fmt.Errorf("Client on ping does not returned PONG: %v\n", err)
		}
		tracelog.DebugLogger.Printf("Valkey: Got PONG on PING from %s", hostName)
		return nil
	})
}

func (tctx *TestContext) valkeyHasTestValkeyDataTest(host string, testId int) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, host)
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

func (tctx *TestContext) createValkeyBackup(host, backupType, resultType string) error {
	allowedValues := []string{"success", "error", "threshold"}
	if !slices.Contains(allowedValues, resultType) {
		return fmt.Errorf("undefined resultType: use one of %+v", allowedValues)
	}

	configType := ""
	if resultType == "threshold" {
		configType = "-low-disk-usage"
	}
	rc, err := GetValkeyCtlFromTestContextTyped(tctx, host, configType)
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

	tracelog.DebugLogger.Printf("Push valkey %s backup\n", backupType)
	backupId, err := rc.PushBackup(backupType)
	if err != nil && resultType == "success" {
		return err
	}
	if err == nil && slices.Contains([]string{"error", "threshold"}, resultType) {
		return fmt.Errorf("no expected error occurred")
	}

	time.Sleep(1 * time.Second)
	tracelog.DebugLogger.Println("Backup created: ", backupId)
	return nil
}

func (tctx *TestContext) weDeleteValkeyBackupsRetainViaValkey(backupsRetain int, host string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}

	return rc.PurgeRetain(backupsRetain)
}

func (tctx *TestContext) weRestartValkeyServerAt(host string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, host)
	if err != nil {
		return err
	}
	rc.ShutdownNoSave(tctx.Context)
	return nil
}

func (tctx *TestContext) weStopValkeyServerAt(hostName string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	host := rc.Host()

	cmd := []string{"supervisorctl", "stop", "valkey"}
	_, err = helpers.RunCommandStrict(tctx.Context, host, cmd)
	if err != nil {
		return fmt.Errorf("stop valkey failed: %+v", err)
	}

	return nil
}

func (tctx *TestContext) weStartValkeyServerAt(hostName string) error {
	rc, err := GetValkeyCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	host := rc.Host()

	cmd := []string{"supervisorctl", "start", "valkey"}
	_, err = helpers.RunCommandStrict(tctx.Context, host, cmd)
	if err != nil {
		return fmt.Errorf("start valkey failed: %+v", err)
	}

	return nil
}

func (tctx *TestContext) testEqualValkeyDataAtHosts(host1, host2 string) error {
	rc1, err := GetValkeyCtlFromTestContext(tctx, host1)
	if err != nil {
		return err
	}

	rc2, err := GetValkeyCtlFromTestContext(tctx, host2)
	if err != nil {
		return err
	}

	dbsize1 := rc1.DBSize(tctx.Context)
	if dbsize1.Err() != nil {
		return errors.Wrapf(dbsize1.Err(), "Host %s doesn't return 'dbsize'", host1)
	}
	dbsize2 := rc2.DBSize(tctx.Context)
	if dbsize2.Err() != nil {
		return errors.Wrapf(dbsize1.Err(), "Host %s doesn't return 'dbsize'", host2)
	}
	if dbsize1.Val() != dbsize2.Val() {
		return fmt.Errorf("hosts %s and %s have not equal keys count %d != %d", host1, host2, dbsize1.Val(), dbsize2.Val())
	}

	keys1 := rc1.Keys(tctx.Context, "*")
	if keys1.Err() != nil {
		return keys1.Err()
	}

	keys2 := rc2.Keys(tctx.Context, "*")
	if keys2.Err() != nil {
		return keys2.Err()
	}

	if len(keys1.Val()) == 0 || len(keys2.Val()) == 0 {
		return fmt.Errorf("keys1 or keys2 is empty - broken backup")
	}

	if !utils.IsArraysEqual(keys1.Val(), keys2.Val()) {
		return fmt.Errorf("keys from valkey1/valkey2 aren't equal")
	}
	values1 := rc1.MGet(tctx.Context, keys1.Val()...)
	values2 := rc1.MGet(tctx.Context, keys2.Val()...)
	vals1 := make([]string, len(values1.Val()))
	vals2 := make([]string, len(values1.Val()))

	for i, val := range values1.Val() {
		vals1[i] = val.(string)
	}

	for i, val := range values2.Val() {
		vals2[i] = val.(string)
	}
	if !utils.IsArraysEqual(vals1, vals2) {
		return fmt.Errorf("values from valkey1/valkey2 aren't equal")
	}
	return nil
}
