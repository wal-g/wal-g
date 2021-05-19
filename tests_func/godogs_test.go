package functests

import (
	"flag"
	"github.com/wal-g/wal-g/tests_func/utils"
	"os"
	"testing"

	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/colors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/mongodb/config"
)

type TestOpts struct {
	test  bool
	clean bool
	stop  bool
	debug bool
	featurePrefix string
}

const (
	testOptsPrefix = "tf."
)

var (
	godogOpts = godog.Options{
		Output:        colors.Colored(os.Stdout),
		Format:        "pretty",
		StopOnFailure: true,
		Strict:        true,
	}

	testOpts = TestOpts{}
)

func init() {
	flag.BoolVar(&testOpts.test, testOptsPrefix+"test", true, "run tests")
	flag.BoolVar(&testOpts.stop, testOptsPrefix+"stop", true, "shutdown test environment")
	flag.BoolVar(&testOpts.clean, testOptsPrefix+"clean", true, "delete test environment")
	flag.BoolVar(&testOpts.debug, testOptsPrefix+"debug", false, "enable debug logging")
	flag.StringVar(&testOpts.featurePrefix, testOptsPrefix+"featurePrefix", "", "features prefix")
	godog.BindFlags("godog.", flag.CommandLine, &godogOpts)
}

func TestMain(m *testing.M) {
	flag.Parse()

	status := 0
	if testOpts.debug {
		err := tracelog.UpdateLogLevel(tracelog.DevelLogLevel)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	stagingPath := config.Env["STAGING_DIR"]
	envFilePath := config.Env["ENV_FILE"]
	newEnv := !EnvExists(envFilePath)
	env := map[string]string{}
	var err error
	if newEnv {
		env, err = SetupEnv(envFilePath, stagingPath)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	env = utils.MergeEnvs(utils.ParseEnvLines(os.Environ()), env)

	if newEnv {
		err := SetupStaging(env["IMAGES_DIR"], env["STAGING_DIR"])
		tracelog.ErrorLogger.FatalOnError(err)
	}

	foundFeatures, err := scanFeatureDirs(testOpts.featurePrefix)
	tracelog.ErrorLogger.FatalOnError(err)

	if len(foundFeatures) == 0 {
		tracelog.ErrorLogger.Fatalln("No features found")
	}

	tctx, err := NewTestContext(foundFeatures)
	tracelog.ErrorLogger.FatalOnError(err)
	tctx.Env = env

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

	tctx.Infra = InfraFromTestContext(tctx)

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
