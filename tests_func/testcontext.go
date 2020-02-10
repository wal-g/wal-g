package functests

import (
	"context"
	"fmt"
	"os"

	testConf "github.com/wal-g/wal-g/tests_func/config"
	testHelper "github.com/wal-g/wal-g/tests_func/helpers"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"

	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/gherkin"
	"github.com/docker/docker/client"
	"github.com/wal-g/tracelog"
	"time"
)

func (tctx *TestContext) ContainerFQDN(name string) string {
	return fmt.Sprintf("%s.test_net_%s", name, tctx.Env["TEST_ID"])
}

func (tctx *TestContext) S3Host() string {
	return tctx.Env["S3_HOST"]
}

func WalgUtilFromTestContext(tctx *TestContext, host string) *testHelper.WalgUtil {
	return testHelper.NewWalgUtil(
		tctx.Context,
		tctx.ContainerFQDN(host),
		tctx.Env["WALG_CLIENT_PATH"],
		tctx.Env["WALG_CONF_PATH"])
}

func S3StorageFromTestContext(tctx *TestContext, host string) *testHelper.S3Storage {
	return testHelper.NewS3Storage(
		tctx.Context,
		tctx.ContainerFQDN(host),
		tctx.Env["S3_BUCKET"],
		tctx.Env["S3_ACCESS_KEY"],
		tctx.Env["S3_SECRET_KEY"])
}

func MongoCtlFromTestContext(tctx *TestContext, host string) (*testHelper.MongoCtl, error) {
	return testHelper.NewMongoCtl(
		tctx.Context,
		tctx.ContainerFQDN(host),
		testHelper.AdminCreds(testHelper.AdminCredsFromEnv(tctx.Env)))
}

type AuxData struct {
	Timestamps         map[string]testHelper.OpTimestamp
	DatabaseSnap       map[string][]testHelper.UserData
	CreatedBackupNames []string
	NometaBackupNames  []string
	OplogPushEnabled   bool
	PreviousBackupTime   time.Time
}

type TestContext struct {
	Docker  *client.Client
	Env     map[string]string
	Context context.Context
	AuxData AuxData
}

func NewTestContext() (*TestContext, error) {
	tctx := &TestContext{}

	var err error
	tctx.Context = context.Background()
	tctx.Docker, err = client.NewEnvClient()
	if err != nil {
		return nil, fmt.Errorf("can not setup docker client: %v", err)
	}
	return tctx, nil
}

func (tctx *TestContext) Stop() error {
	return testHelper.CallCompose(tctx.Env["COMPOSE_FILE"], tctx.Env, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func (tctx *TestContext) StartRecreate() error {
	return testHelper.CallCompose(tctx.Env["COMPOSE_FILE"], tctx.Env, []string{"--verbose", "--log-level", "WARNING", "up", "--detach", "--build", "--force-recreate"})
}

func (tctx *TestContext) ShutdownEnv() error {
	if err := testHelper.ShutdownContainers(tctx.Env["COMPOSE_FILE"], tctx.Env); err != nil {
		return err
	}
	if err := testHelper.ShutdownNetwork(tctx.Context, tctx.Env["NETWORK_NAME"]); err != nil {
		return err
	}

	// TODO: Enable net cleanup
	//if err := testHelper.RemoveNet(TestContext); err != nil {
	//	log.Fatalln(err)
	//}

	if err := os.RemoveAll(testConf.Env["STAGING_DIR"]); err != nil {
		return err
	}
	return nil
}

func (tctx *TestContext) setupSuites(s *godog.Suite) {
	s.BeforeFeature(func(feature *gherkin.Feature) {
		tctx.AuxData.CreatedBackupNames = []string{}
		tctx.AuxData.NometaBackupNames = []string{}
		tctx.AuxData.OplogPushEnabled = false
		tctx.AuxData.Timestamps = make(map[string]testHelper.OpTimestamp)
		tctx.AuxData.DatabaseSnap = make(map[string][]testHelper.UserData)
		tctx.AuxData.PreviousBackupTime = time.Unix(0, 0)
		if err := tctx.StartRecreate(); err != nil {
			tracelog.ErrorLogger.Fatalln(err)
		}
	})

	s.BeforeSuite(func() {
		stagingPath := testConf.Env["STAGING_DIR"]
		envFilePath := testConf.Env["ENV_FILE"]
		newEnv := !EnvExists(envFilePath)
		if newEnv {
			err := SetupEnv(envFilePath, stagingPath)
			tracelog.ErrorLogger.FatalOnError(err)
		}

		env, err := ReadEnv(envFilePath)
		tracelog.ErrorLogger.FatalOnError(err)
		tctx.Env = testUtils.MergeEnvs(testUtils.ParseEnvLines(os.Environ()), env)

		if newEnv {
			err := tctx.SetupStaging()
			tracelog.ErrorLogger.FatalOnError(err)

			err = BuildBase(tctx)
			tracelog.ErrorLogger.FatalOnError(err)
		}

	})

	s.Step(`^a configured s3 on ([^\s]*)$`, tctx.configureS3)
	s.Step(`^at least one oplog archive exists in storage$`, tctx.oplogArchiveIsNotEmpty)

	s.Step(`^a working mongodb on ([^\s]*)$`, tctx.testMongoConnect)
	s.Step(`^mongodb replset initialized on ([^\s]*)$`, tctx.initiateReplSet)
	s.Step(`^mongodb role is primary on ([^\s]*)$`, tctx.isPrimary)
	s.Step(`^mongodb auth initialized on ([^\s]*)$`, tctx.enableAuth)
	s.Step(`^([^\s]*) has no data$`, tctx.purgeDataDir)

	s.Step(`we save last oplog timestamp on ([^\s]*) to "([^"]*)"`, tctx.saveOplogTimestamp)
	s.Step(`^([^\s]*) has test mongodb data test(\d+)$`, tctx.fillMongodbWithTestData)
	s.Step(`^we got same mongodb data at ([^\s]*) ([^\s]*)$`, tctx.testEqualMongodbDataAtHosts)
	s.Step(`^we have same data in "([^"]*)" and "([^"]*)"$`, tctx.sameDataCheck)
	s.Step(`^we save ([^\s]*) data "([^"]*)"$`, tctx.saveUserData)

	s.Step(`^we create ([^\s]*) backup$`, tctx.createBackup)
	s.Step(`^we got (\d+) backup entries of ([^\s]*)$`, tctx.checkBackupsCount)
	s.Step(`^we put empty backup via ([^\s]*)$`, tctx.putEmptyBackupViaMinio)
	s.Step(`^we delete backups retain (\d+) via ([^\s]*)$`, tctx.purgeBackupRetain)
	s.Step(`^we check if empty backups were purged via ([^\s]*)$`, tctx.testEmptyBackupsViaMinio)
	s.Step(`^we restore #(\d+) backup to ([^\s]*)$`, tctx.restoreBackupToMongodb)
	s.Step(`^we ensure ([^\s]*) #(\d+) backup metadata contains$`, tctx.backupMetadataContains)
	s.Step(`^we delete backups retain (\d+) after #(\d+) backup via ([^\s]*)$`, tctx.purgeBackupsAfterID)
	s.Step(`^we delete backups retain (\d+) after "([^"]*)" timestamp via ([^\s]*)$`, tctx.purgeBackupsAfterTime)
	s.Step(`^oplog archiving is enabled on ([^\s]*)$`, tctx.enableOplogPush)
	s.Step(`^we restore from #(\d+) backup to "([^"]*)" timestamp to ([^\s]*)$`, tctx.replayOplog)
}
