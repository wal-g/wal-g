package functests

import (
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/wal-g/wal-g/tests_func/helpers"
)

const tsTestBackupID = "functional-test-ts-backup"

func setupRedisTSSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^([^\s]*) has a frozen ts tree at ([^\s]*)$`, tctx.createFrozenTSTree)
	ctx.Step(`^we copy the ts tree from ([^\s]*) to ([^\s]*) on ([^\s]*)$`, tctx.copyTSTree)
	ctx.Step(`^the ts tree at ([^\s]*) on ([^\s]*) has incompressible data$`, tctx.addIncompressibleTSData)
	ctx.Step(`^we create ([^\s]*) ([^\s]*) tiered backup from ([^\s]*)$`, tctx.createTieredStorageBackup)
	ctx.Step(`^we fetch latest ([^\s]*) backup from ([^\s]*) into ([^\s]*)$`, tctx.fetchLatestTieredStorageBackup)
	ctx.Step(`^we fetch latest ([^\s]*) backup from ([^\s]*) on ([^\s]*) into ([^\s]*)$`, tctx.fetchLatestTieredStorageBackupOnHost)
	ctx.Step(`^the ts tree at ([^\s]*) matches ([^\s]*) on ([^\s]*)$`, tctx.assertTSTreesMatch)
	ctx.Step(`^we delete latest redis backup via ([^\s]*)$`, tctx.deleteLatestRedisBackup)
	ctx.Step(`^we create ([^\s]*) ([^\s]*) tiered backup from ([^\s]*) and it fails$`, tctx.createTieredStorageBackupAndExpectFailure)
	ctx.Step(`^we remove ([^\s]*) during the next tiered backup on ([^\s]*)$`, tctx.removeTSTreeDuringNextBackup)
}

func (tctx *TestContext) createFrozenTSTree(hostName, treePath string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}

	command := fmt.Sprintf(
		"rm -rf -- %s && mkdir -p %s/nested/deeper && "+
			"printf 'tiered-storage-root-data\\n' > %s/root.data && "+
			"printf 'tiered-storage-nested-data\\n' > %s/nested/deeper/part.data && "+
			"dd if=/dev/zero of=%s/nested/blob.data bs=1M count=1 status=none",
		shellQuote(treePath), shellQuote(treePath), shellQuote(treePath), shellQuote(treePath), shellQuote(treePath),
	)
	_, err = helpers.RunCommandStrict(tctx.Context, rc.Host(), []string{"bash", "-c", command})
	return err
}

func (tctx *TestContext) copyTSTree(sourcePath, targetPath, hostName string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	command := fmt.Sprintf("rm -rf -- %s && cp -a %s %s", shellQuote(targetPath), shellQuote(sourcePath), shellQuote(targetPath))
	_, err = helpers.RunCommandStrict(tctx.Context, rc.Host(), []string{"bash", "-c", command})
	return err
}

func (tctx *TestContext) addIncompressibleTSData(treePath, hostName string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	command := fmt.Sprintf("dd if=/dev/urandom of=%s/nested/incompressible.data bs=1M count=32 status=none", shellQuote(treePath))
	_, err = helpers.RunCommandStrict(tctx.Context, rc.Host(), []string{"bash", "-c", command})
	return err
}

func (tctx *TestContext) createTieredStorageBackup(hostName, backupType, sourcePath string) error {
	if !isTieredStorageBackupType(backupType) {
		return fmt.Errorf("unsupported tiered backup type %q", backupType)
	}
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}

	if err = rc.PushTSBackup(backupType, sourcePath, tsTestBackupID); err != nil {
		return err
	}
	// Backup names have second precision; avoid collisions if another scenario step pushes immediately.
	time.Sleep(time.Second)
	return nil
}

func (tctx *TestContext) fetchLatestTieredStorageBackup(backupType, hostName, targetPath string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	return tctx.fetchLatestTieredStorageBackupToHost(backupType, rc, rc, targetPath)
}

func (tctx *TestContext) fetchLatestTieredStorageBackupOnHost(backupType, sourceHost, restoreHost, targetPath string) error {
	source, err := GetRedisCtlFromTestContext(tctx, sourceHost)
	if err != nil {
		return err
	}
	restore, err := GetRedisCtlFromTestContext(tctx, restoreHost)
	if err != nil {
		return err
	}
	return tctx.fetchLatestTieredStorageBackupToHost(backupType, source, restore, targetPath)
}

func (tctx *TestContext) fetchLatestTieredStorageBackupToHost(
	backupType string, source, restore *helpers.RedisCtl, targetPath string,
) error {
	if !isTieredStorageBackupType(backupType) {
		return fmt.Errorf("unsupported tiered backup type %q", backupType)
	}
	backups, err := source.Backups()
	if err != nil {
		return err
	}
	if len(backups) == 0 {
		return fmt.Errorf("no backups available for %s fetch", backupType)
	}

	command := fmt.Sprintf("rm -rf -- %s && mkdir -p %s", shellQuote(targetPath), shellQuote(targetPath))
	if _, err = helpers.RunCommandStrict(tctx.Context, restore.Host(), []string{"bash", "-c", command}); err != nil {
		return err
	}

	redisVersion := ""
	if backupType == "aof_ts" {
		redisVersion = tctx.Version.Full
	}
	if err = restore.FetchTSBackup(backups[len(backups)-1], backupType, targetPath, redisVersion); err != nil {
		return err
	}
	if backupType == "aof_ts" {
		_, err = helpers.RunCommandStrict(tctx.Context, restore.Host(), []string{"chown", "-R", "redis:redis", "/data"})
	}
	return err
}

func (tctx *TestContext) assertTSTreesMatch(expectedPath, actualPath, hostName string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	command := fmt.Sprintf("diff -r --no-dereference %s %s", shellQuote(expectedPath), shellQuote(actualPath))
	_, err = helpers.RunCommandStrict(tctx.Context, rc.Host(), []string{"bash", "-c", command})
	return err
}

func (tctx *TestContext) deleteLatestRedisBackup(hostName string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	backups, err := rc.Backups()
	if err != nil {
		return err
	}
	if len(backups) == 0 {
		return fmt.Errorf("no backups available to delete")
	}
	return rc.DeleteBackup(backups[len(backups)-1])
}

func (tctx *TestContext) createTieredStorageBackupAndExpectFailure(hostName, backupType, sourcePath string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	if err = rc.PushTSBackup(backupType, sourcePath, tsTestBackupID); err == nil {
		return fmt.Errorf("expected %s backup to fail", backupType)
	}
	return nil
}

func (tctx *TestContext) removeTSTreeDuringNextBackup(sourcePath, hostName string) error {
	rc, err := GetRedisCtlFromTestContext(tctx, hostName)
	if err != nil {
		return err
	}
	command := fmt.Sprintf("sleep 1 && rm -rf -- %s", shellQuote(sourcePath))
	return helpers.RunAsyncCommand(tctx.Context, rc.Host(), command)
}

func isTieredStorageBackupType(backupType string) bool {
	return backupType == "ts" || backupType == "rdb_ts" || backupType == "aof_ts"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
