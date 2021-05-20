package functests

import (
	"flag"
	"os"
	"path"
	"testing"

	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/colors"
	"github.com/wal-g/tracelog"
)

type TestOpts struct {
	test          bool
	clean         bool
	stop          bool
	debug         bool
	featurePrefix string
	database      string
}

const (
	testOptsPrefix = "tf."

	stagingDir = "staging"
	envFile    = "env.file"
)

var (
	godogOpts = godog.Options{
		Output:        colors.Colored(os.Stdout),
		Format:        "pretty",
		StopOnFailure: true,
		Strict:        true,
	}

	testOpts = TestOpts{}

	databases = map[string]bool{
		"mongodb": true,
		"redis":   true,
	}
)

func init() {
	flag.BoolVar(&testOpts.test, testOptsPrefix+"test", true, "run tests")
	flag.BoolVar(&testOpts.stop, testOptsPrefix+"stop", true, "shutdown test environment")
	flag.BoolVar(&testOpts.clean, testOptsPrefix+"clean", true, "delete test environment")
	flag.BoolVar(&testOpts.debug, testOptsPrefix+"debug", false, "enable debug logging")
	flag.StringVar(&testOpts.featurePrefix, testOptsPrefix+"featurePrefix", "", "features prefix")
	flag.StringVar(&testOpts.database, testOptsPrefix+"database", "", "database name [mongodb|redis]")
	godog.BindFlags("godog.", flag.CommandLine, &godogOpts)
}

func TestMain(m *testing.M) {
	flag.Parse()

	status := 0
	if testOpts.debug {
		err := tracelog.UpdateLogLevel(tracelog.DevelLogLevel)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	database := testOpts.database

	if _, ok := databases[database]; !ok {
		tracelog.ErrorLogger.Fatalf("Database '%s' is not valid, please provide test database -tf.database=dbname\n", database)
	}

	stagingPath := stagingDir
	envFilePath := path.Join(stagingDir, envFile)

	Env["ENV_FILE"] = envFilePath // set ENV_FILE for docker-compose
	Env["DOCKER_FILE"] = "Dockerfile." + database
	Env["COMPOSE_FILE"] = database + Env["COMPOSE_FILE_SUFFIX"]

	newEnv := !EnvExists(envFilePath)

	var env map[string]string
	var err error

	if newEnv {
		env, err = SetupEnv(Env, envFilePath, stagingPath)
		tracelog.ErrorLogger.FatalOnError(err)

		err := SetupStaging(env["IMAGES_DIR"], stagingPath)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	foundFeatures, err := scanFeatureDirs(database, testOpts.featurePrefix)
	tracelog.ErrorLogger.FatalOnError(err)

	if len(foundFeatures) == 0 {
		tracelog.ErrorLogger.Fatalln("No features found")
	}

	tctx, err := NewTestContext(envFilePath, database, env, foundFeatures)
	tracelog.ErrorLogger.FatalOnError(err)

	// if newEnv == false, database empty, so we should load from database file
	tctx.LoadEnv()

	if testOpts.test {
		godogOpts.Paths = tctx.Features

		tracelog.InfoLogger.Printf("Starting testing environment: mongodb %s with features: %v",
			tctx.Version.Full, godogOpts.Paths)

		status = godog.RunWithOptions("godogs", func(s *godog.Suite) {
			tctx.setupSuites(s)
		}, godogOpts)

		if st := m.Run(); st > status {
			status = st
		}
	}

	if testOpts.stop {
		err = tctx.StopEnv()
		tracelog.ErrorLogger.FatalOnError(err)
	}

	if testOpts.clean {
		err = tctx.CleanEnv()
		tracelog.ErrorLogger.FatalOnError(err)
	}

	os.Exit(status)
}
