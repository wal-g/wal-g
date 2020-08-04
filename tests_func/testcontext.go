package functests

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/wal-g/wal-g/tests_func/config"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/utils"

	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/gherkin"
	"github.com/wal-g/tracelog"
)

var (
	featureFiles = map[string]string{
		"backup": "features/check_mongodb_backup.feature",
		"pitr":   "features/check_mongodb_pitr.feature",
	}
)

func (tctx *TestContext) ContainerFQDN(name string) string {
	return fmt.Sprintf("%s.test_net_%s", name, tctx.Env["TEST_ID"])
}

func (tctx *TestContext) S3Host() string {
	return tctx.Env["S3_HOST"]
}

func WalgUtilFromTestContext(tctx *TestContext, host string) *helpers.WalgUtil {
	return helpers.NewWalgUtil(
		tctx.Context,
		tctx.ContainerFQDN(host),
		tctx.Env["WALG_CLIENT_PATH"],
		tctx.Env["WALG_CONF_PATH"],
		tctx.Version.Major)
}

func S3StorageFromTestContext(tctx *TestContext, host string) *helpers.S3Storage {
	return helpers.NewS3Storage(
		tctx.Context,
		tctx.ContainerFQDN(host),
		tctx.Env["S3_BUCKET"],
		tctx.Env["S3_ACCESS_KEY"],
		tctx.Env["S3_SECRET_KEY"])
}

func MongoCtlFromTestContext(tctx *TestContext, host string) (*helpers.MongoCtl, error) {
	return helpers.NewMongoCtl(
		tctx.Context,
		tctx.ContainerFQDN(host),
		helpers.AdminCreds(helpers.AdminCredsFromEnv(tctx.Env)))
}

func InfraFromTestContext(tctx *TestContext) *helpers.Infra {
	return helpers.NewInfra(
		tctx.Context,
		tctx.Env["COMPOSE_FILE"],
		tctx.Env,
		tctx.Env["NETWORK_NAME"],
		helpers.BaseImage{Path: tctx.Env["BACKUP_BASE_PATH"], Tag: tctx.Env["BACKUP_BASE_TAG"]})
}

type AuxData struct {
	Timestamps         map[string]helpers.OpTimestamp
	Snapshots          map[string][]helpers.NsSnapshot
	CreatedBackupNames []string
	NometaBackupNames  []string
	OplogPushEnabled   bool
	PreviousBackupTime time.Time
}

type MongoVersion struct {
	Major string
	Full  string
}

type TestContext struct {
	Infra    *helpers.Infra
	Env      map[string]string
	Context  context.Context
	AuxData  AuxData
	Version  MongoVersion
	Features []string
}

func NewTestContext() (*TestContext, error) {
	environ := utils.ParseEnvLines(os.Environ())

	features := utils.GetMapValues(featureFiles)
	requestedFeature := environ["MONGO_FEATURE"]
	if requestedFeature != "" {
		feature, ok := featureFiles[requestedFeature]
		if !ok {
			return nil, fmt.Errorf("requested feature is not found: %s", requestedFeature)
		}
		features = []string{feature}
	}

	return &TestContext{
		Context: context.Background(),
		Version: MongoVersion{
			Major: environ["MONGO_MAJOR"],
			Full:  environ["MONGO_VERSION"]},
		Features: features}, nil
}

func (tctx *TestContext) StopEnv() error {
	return tctx.Infra.Shutdown()
}

func (tctx *TestContext) CleanEnv() error {
	// TODO: Enable net cleanup
	//if err := helpers.RemoveNet(TestContext); err != nil {
	//	log.Fatalln(err)
	//}

	return os.RemoveAll(config.Env["STAGING_DIR"])
}

func (tctx *TestContext) setupSuites(s *godog.Suite) {
	s.BeforeFeature(func(feature *gherkin.Feature) {
		tctx.AuxData.CreatedBackupNames = []string{}
		tctx.AuxData.NometaBackupNames = []string{}
		tctx.AuxData.OplogPushEnabled = false
		tctx.AuxData.Timestamps = make(map[string]helpers.OpTimestamp)
		tctx.AuxData.Snapshots = make(map[string][]helpers.NsSnapshot)
		tctx.AuxData.PreviousBackupTime = time.Unix(0, 0)
		if err := tctx.Infra.RecreateContainers(); err != nil {
			tracelog.ErrorLogger.Fatalln(err)
		}
	})

	s.BeforeSuite(func() {
		stagingPath := config.Env["STAGING_DIR"]
		envFilePath := config.Env["ENV_FILE"]
		newEnv := !EnvExists(envFilePath)
		if newEnv {
			err := SetupEnv(envFilePath, stagingPath)
			tracelog.ErrorLogger.FatalOnError(err)
		}

		env, err := ReadEnv(envFilePath)
		tracelog.ErrorLogger.FatalOnError(err)
		tctx.Env = utils.MergeEnvs(utils.ParseEnvLines(os.Environ()), env)
		tctx.Infra = InfraFromTestContext(tctx)

		if newEnv {
			err := SetupStaging(tctx.Env["IMAGES_DIR"], tctx.Env["STAGING_DIR"])
			tracelog.ErrorLogger.FatalOnError(err)
		}

		err = tctx.Infra.Setup()
		tracelog.ErrorLogger.FatalOnError(err)

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
	s.Step(`^([^\s]*) has been loaded with "([^"]*)"$`, tctx.loadMongodbOpsFromConfig)
	s.Step(`^we got same mongodb data at ([^\s]*) ([^\s]*)$`, tctx.testEqualMongodbDataAtHosts)
	s.Step(`^we have same data in "([^"]*)" and "([^"]*)"$`, tctx.sameDataCheck)
	s.Step(`^we save ([^\s]*) data "([^"]*)"$`, tctx.saveSnapshot)

	s.Step(`^we create ([^\s]*) backup$`, tctx.createBackup)
	s.Step(`^we got (\d+) backup entries of ([^\s]*)$`, tctx.checkBackupsCount)
	s.Step(`^we put empty backup via ([^\s]*)$`, tctx.putEmptyBackupViaMinio)
	s.Step(`^we delete backups retain (\d+) via ([^\s]*)$`, tctx.purgeBackupRetain)
	s.Step(`^we delete backup #(\d+) via ([^\s]*)$`, tctx.deleteBackup)
	s.Step(`^we check if empty backups were purged via ([^\s]*)$`, tctx.testEmptyBackupsViaMinio)
	s.Step(`^we restore #(\d+) backup to ([^\s]*)$`, tctx.restoreBackupToMongodb)
	s.Step(`^we ensure ([^\s]*) #(\d+) backup metadata contains$`, tctx.backupMetadataContains)
	s.Step(`^oplog archiving is enabled on ([^\s]*)$`, tctx.enableOplogPush)
	s.Step(`^we restore from #(\d+) backup to "([^"]*)" timestamp to ([^\s]*)$`, tctx.replayOplog)

	s.Step(`we sleep ([^\s]*)$`, tctx.sleep)
}
