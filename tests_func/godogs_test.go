package functests

import (
	"os"
	"testing"

	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"
	"github.com/spf13/pflag"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/utils"
)

type TestOpts struct {
	test          bool
	clean         bool
	stop          bool
	debug         bool
	featurePrefix string
	database      string
}

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
	pflag.BoolVar(&testOpts.test, "tf.test", true, "run tests")
	pflag.BoolVar(&testOpts.stop, "tf.stop", true, "shutdown test environment")
	pflag.BoolVar(&testOpts.clean, "tf.clean", true, "delete test environment")
	pflag.BoolVar(&testOpts.debug, "tf.debug", false, "enable debug logging")
	pflag.StringVar(&testOpts.featurePrefix, "tf.featurePrefix", "", "features prefix")
	pflag.StringVar(&testOpts.database, "tf.database", "", "database name [mongodb|redis]")

	godog.BindCommandLineFlags("godog.", &godogOpts)
}

func TestMain(m *testing.M) {
	pflag.Parse()

	if _, ok := databases[testOpts.database]; !ok {
		tracelog.ErrorLogger.Fatalf("Database '%s' is not valid, please provide test database -tf.database=dbname\n",
			testOpts.database)
	}

	status, err := RunTestFeatures()

	tracelog.ErrorLogger.FatalOnError(err)
	os.Exit(status)
}

func RunTestFeatures() (status int, err error) {
	if testOpts.debug {
		err := tracelog.UpdateLogLevel(tracelog.DevelLogLevel)
		if err != nil {
			return -1, err
		}
	}

	tctx, err := CreateTestContex(testOpts.database)
	if err != nil {
		return -1, err
	}

	if testOpts.test {
		godogOpts.Paths, err = utils.FindFeaturePaths(testOpts.database, testOpts.featurePrefix)
		if err != nil {
			return -1, err
		}

		tracelog.InfoLogger.Printf("Starting testing environment: mongodb %s with features: %v",
			tctx.Version.Full, godogOpts.Paths)

		suite := godog.TestSuite{
			Name: "godogs",
			TestSuiteInitializer: func(ctx *godog.TestSuiteContext) {
				ctx.BeforeSuite(func() {
					err := tctx.LoadEnv()
					tracelog.ErrorLogger.FatalOnError(err)
				})
			},
			ScenarioInitializer: func(ctx *godog.ScenarioContext) {
				SetupCommonSteps(ctx, tctx)
				SetupMongodbSteps(ctx, tctx)
				SetupMongodbBinaryBackupSteps(ctx, tctx)
				SetupRedisSteps(ctx, tctx)
			},
			Options: &godogOpts,
		}
		status = suite.Run()
	}

	if testOpts.stop {
		err = tctx.StopEnv()
		if err != nil {
			return -1, err
		}
	}

	if testOpts.clean {
		err = tctx.CleanEnv()
		if err != nil {
			return -1, err
		}
	}

	return status, nil
}
