package functests

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/tests_func/utils"
)

const (
	stagingDir = "staging"
	envFile    = "env.file"

	MAX_RETRIES_COUNT = 10
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
		helpers.BaseImage{
			Path: tctx.Env["BACKUP_BASE_PATH"],
			Tag:  tctx.Env["BACKUP_BASE_TAG"],
		},
	)
}

type AuxData struct {
	Timestamps         map[string]helpers.OpTimestamp
	Snapshots          map[string][]helpers.NsSnapshot
	CreatedBackupNames []string
	NometaBackupNames  []string
	OplogPushEnabled   bool
}

type DBVersion struct {
	Major string
	Full  string
}

type TestContext struct {
	EnvFilePath        string
	Infra              *helpers.Infra
	Env                map[string]string
	Context            context.Context
	AuxData            AuxData
	Version            DBVersion
	PreviousBackupTime time.Time
}

func CreateTestContext(database string) (tctx *TestContext, err error) {
	environ := utils.ParseEnvLines(os.Environ())

	envFilePath := path.Join(stagingDir, envFile)

	Env["ENV_FILE"] = envFilePath // set ENV_FILE for docker-compose
	Env["DOCKER_FILE"] = "Dockerfile." + database
	Env["COMPOSE_FILE"] = database + Env["COMPOSE_FILE_SUFFIX"]
	Env["WALG_S3_PREFIX"] = strings.ReplaceAll(Env["WALG_S3_PREFIX"], "DBNAME", database)
	tracelog.DebugLogger.Printf("Database name %s\nEnv: %s\n", database, Env)

	var env map[string]string

	if !EnvExists(envFilePath) {
		env, err = SetupNewEnv(Env, environ, envFilePath, stagingDir)
		if err != nil {
			return nil, err
		}

		err = SetupStaging(env["IMAGES_DIR"], stagingDir)
		if err != nil {
			return nil, err
		}
	}

	var version DBVersion
	if database == "mongodb" {
		version = DBVersion{
			Major: environ["MONGO_MAJOR"],
			Full:  environ["MONGO_VERSION"],
		}
	} else if database == "redis" {
		full := environ["REDIS_VERSION"]
		parts := strings.Split(full, ".")
		major := strings.Join(parts[:2], ".")
		version = DBVersion{
			Major: major,
			Full:  full,
		}
	} else {
		return nil, fmt.Errorf("database %s is not expected here", database)
	}

	tctx = &TestContext{
		EnvFilePath: envFilePath,
		Context:     context.Background(),
		Version:     version,
		Env:         env,
	}
	return tctx, tctx.LoadEnv()
}

func (tctx *TestContext) StopEnv() error {
	return tctx.Infra.Shutdown()
}

func (tctx *TestContext) CleanEnv() error {
	return os.RemoveAll(path.Dir(tctx.EnvFilePath))
}

func (tctx *TestContext) LoadEnv() (err error) {
	if tctx.Env == nil {
		tctx.Env, err = ReadEnv(tctx.EnvFilePath)
		if err != nil {
			return err
		}
	}

	// mix os.environ to our database params
	tctx.Env = utils.MergeEnvs(tctx.Env, utils.ParseEnvLines(os.Environ()))
	tctx.Infra = InfraFromTestContext(tctx)
	return tctx.Infra.Setup()
}

func GetRedisCtlFromTestContext(tctx *TestContext, hostName string) (*helpers.RedisCtl, error) {
	return GetRedisCtlFromTestContextTyped(tctx, hostName, "")
}

func GetRedisCtlFromTestContextTyped(tctx *TestContext, hostName, configType string) (*helpers.RedisCtl, error) {
	host := tctx.ContainerFQDN(hostName)
	port, err := strconv.Atoi(tctx.Env["REDIS_EXPOSE_PORT"])
	if err != nil {
		return nil, err
	}
	confPath := tctx.Env["WALG_CONF_PATH"]
	if configType != "" {
		ext := filepath.Ext(confPath)
		confPath = strings.Join([]string{strings.TrimSuffix(confPath, ext), configType, ext}, "")
	}
	return helpers.NewRedisCtl(
		tctx.Context,
		helpers.RedisCtlArgs{
			BinPath:  tctx.Env["WALG_CLIENT_PATH"],
			ConfPath: confPath,
			Host:     host,
			Port:     port,
			Username: tctx.Env["REDIS_USERNAME"],
			Password: tctx.Env["REDIS_PASSWORD"],
		},
	)
}
