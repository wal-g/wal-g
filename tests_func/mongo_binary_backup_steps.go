package functests

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/cucumber/godog"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
)

func SetupMongodbBinaryBackupSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^we create binary mongo-backup on ([^\s]*)$`, tctx.createMongoBinaryBackup)
	ctx.Step(`^we create binary mongo-backup on ([^\s]*) with metadata$`, tctx.createMongoBinaryBackupWithMetadata)
	ctx.Step(`^we restore binary mongo-backup #(\d+) to ([^\s]+)$`, tctx.restoreMongoBinaryBackupAsNonInitialized)
	ctx.Step(`^we restore initialized binary mongo-backup #(\d+) to ([^\s]+)`,
		tctx.restoreMongoBinaryBackupAsInitialized)
	ctx.Step(`^we restore mongo-backup #(\d+) to ([^\s]+) with whitelist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithWhitelist)
	ctx.Step(`^we restore mongo-backup #(\d+) to ([^\s]+) with blacklist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithBlacklist)
	ctx.Step(`^we restore mongo-backup #(\d+) to ([^\s]+) with whitelist "([^"]*)" and blacklist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithWhitelistAndBlacklist)
	ctx.Step(`^we restore non-initialized mongo-backup #(\d+) to ([^\s]+) with whitelist "([^"]*)"$`,
		tctx.restoreMongoBinaryBackupWithWhitelistAsNonInitialized)
	ctx.Step(`^journal info count is #(\d+)$`,
		tctx.checkJournals)
	ctx.Step(`^([^\s]+) has (\d+) transactions with (\d+) documents each$`, tctx.addMongoTransactions)
	ctx.Step(`^([^\s]+) has (\d+) replay transaction documents$`, tctx.prepareMongoTransactionDocuments)
	ctx.Step(`^we restore binary mongo-backup #(\d+) to "([^"]*)" timestamp on ([^\s]+) and crash replay mongod$`,
		tctx.restoreMongoBinaryBackupWithInterruptedPITR)
}

func (tctx *TestContext) addMongoTransactions(container string, transactionCount, documentsPerTransaction int) error {
	mc, err := MongoCtlFromTestContext(tctx, container)
	if err != nil {
		return err
	}
	return mc.WriteTransactions(transactionCount, documentsPerTransaction)
}

func (tctx *TestContext) prepareMongoTransactionDocuments(container string, documentCount int) error {
	mc, err := MongoCtlFromTestContext(tctx, container)
	if err != nil {
		return err
	}
	return mc.PrepareTransactionDocuments(documentCount)
}

func (tctx *TestContext) restoreMongoBinaryBackupWithInterruptedPITR(
	backupNumber int,
	timestampID,
	container string,
) error {
	walg := WalgUtilFromTestContext(tctx, container)
	backup, err := walg.GetBackupByNumber(backupNumber)
	if err != nil {
		return err
	}
	backupMeta, err := walg.BackupMeta(backupNumber)
	if err != nil {
		return err
	}
	since := backupMeta.MongoMeta.GetBackupLastTS()
	until := tctx.AuxData.Timestamps[timestampID]
	until.Inc++

	s3 := S3StorageFromTestContext(tctx, tctx.S3Host())
	if _, err = backoff.Retry(tctx.Context, func() (struct{}, error) {
		exists, err := s3.ArchTsExists(until)
		if err != nil {
			return struct{}{}, err
		}
		if !exists {
			return struct{}{}, fmt.Errorf("timestamp %s is not archived yet", until.String())
		}
		return struct{}{}, nil
	}, backoff.WithMaxTries(30)); err != nil {
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
	if err = mc.StopMongod(); err != nil {
		return err
	}

	killC := make(chan error, 1)
	go func() {
		killC <- tctx.crashReplayMongod(walg, container)
	}()
	result, restoreErr := walg.FetchBinaryBackupWithPITR(
		backup, configPath, mongodbVersion, since.String(), until.String())
	killErr := <-killC
	if killErr != nil {
		return killErr
	}
	if restoreErr != nil {
		return restoreErr
	}

	output := result.Combined()
	checkpointPosition := strings.Index(output, "Oplog replay progress is durable through")
	restartPosition := strings.Index(output, "inline mongod crashed, restarting oplog replay")
	if checkpointPosition < 0 || restartPosition < 0 || checkpointPosition > restartPosition {
		return fmt.Errorf("expected a durable checkpoint before mongod restart, output:\n%s", output)
	}

	if err = mc.ChownDBPath(); err != nil {
		return err
	}
	if err = mc.StartMongod(); err != nil {
		return err
	}
	return tctx.initiateReplSet(container)
}

func (tctx *TestContext) crashReplayMongod(walg *helpers.WalgUtil, container string) error {
	host := tctx.ContainerFQDN(container)
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		checkpointLogged, err := walg.InterruptedReplayCheckpointLogged()
		if err != nil {
			return err
		}
		if !checkpointLogged {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		result, err := helpers.RunCommand(tctx.Context, host, []string{"bash", "-c", `
for proc in /proc/[0-9]*; do
    while IFS= read -r -d '' arg; do
        case "$arg" in
            *takeUnstableCheckpointOnShutdown=true*)
                printf '%s\n' "${proc##*/}"
                exit 0
                ;;
        esac
    done < "$proc/cmdline" 2>/dev/null
done
exit 1
`})
		if err == nil && result.ExitCode == 0 {
			pidFields := strings.Fields(result.Stdout())
			if len(pidFields) == 0 {
				continue
			}
			pid, err := strconv.Atoi(pidFields[0])
			if err != nil {
				return err
			}
			_, err = helpers.RunCommandStrict(tctx.Context, host, []string{
				"bash", "-c", `kill -ABRT "$1"`, "--", strconv.Itoa(pid),
			})
			return err
		}
		return fmt.Errorf("replay mongod exited before it could be interrupted")
	}
	return fmt.Errorf("durable replay checkpoint was not reached within one minute")
}

func (tctx *TestContext) createMongoBinaryBackup(container string) error {
	host := tctx.ContainerFQDN(container)

	walg := WalgUtilFromTestContext(tctx, container)
	err := walg.PushBinaryBackup(true)
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

func (tctx *TestContext) createMongoBinaryBackupWithMetadata(container string) error {
	host := tctx.ContainerFQDN(container)

	walg := WalgUtilFromTestContext(tctx, container)
	err := walg.PushBinaryBackup(false)
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
	return tctx.restoreMongoBinaryBackup(backupNumber, container, false, "", "")
}

func (tctx *TestContext) restoreMongoBinaryBackupAsInitialized(backupNumber int, container string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, "", "")
}

func (tctx *TestContext) restoreMongoBinaryBackupWithWhitelist(backupNumber int, container, whitelist string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, whitelist, "")
}

func (tctx *TestContext) restoreMongoBinaryBackupWithBlacklist(backupNumber int, container, blacklist string) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, "", blacklist)
}

func (tctx *TestContext) restoreMongoBinaryBackupWithWhitelistAndBlacklist(
	backupNumber int, container, whitelist, blacklist string,
) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, true, whitelist, blacklist)
}

func (tctx *TestContext) restoreMongoBinaryBackupWithWhitelistAsNonInitialized(
	backupNumber int, container, whitelist string,
) error {
	return tctx.restoreMongoBinaryBackup(backupNumber, container, false, whitelist, "")
}

func (tctx *TestContext) restoreMongoBinaryBackup(
	backupNumber int, container string, initialized bool, whitelist, blacklist string,
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

	rsName := ""
	rsMembers := ""
	if initialized {
		rsName = container
		rsMembers = fmt.Sprintf("%s:%d", container, mc.GetMongodPort())
	}
	err = walg.FetchBinaryBackup(backup, configPath, mongodbVersion, rsName, rsMembers, whitelist, blacklist)
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

func (tctx *TestContext) checkJournals(count int) error {
	s3client, err := S3StorageFromTestContext(tctx, tctx.S3Host()).Client()
	if err != nil {
		return err
	}

	contents, err := s3client.List(tctx.Context, "mongodb-backup/test_uuid/test_mongodb/basebackups_005/journal_")
	if err != nil {
		return err
	}

	if len(contents) != count {
		return fmt.Errorf("expected %d journals, got %v", count, contents)
	}
	return nil
}
