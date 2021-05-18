package functests

import (
	"flag"
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
	godog.BindFlags("godog.", flag.CommandLine, &godogOpts)
}

func TestMain(m *testing.M) {
	flag.Parse()

	status := 0
	if testOpts.debug {
		err := tracelog.UpdateLogLevel(tracelog.DevelLogLevel)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	envFilePath := config.Env["ENV_FILE"]

	tctx, err := NewTestContext()
	tracelog.ErrorLogger.FatalOnError(err)

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

	if !EnvExists(envFilePath) {
		tracelog.ErrorLogger.Fatalln("Environment file is not found: ", envFilePath)
	}

	env, err := ReadEnv(envFilePath)
	tracelog.ErrorLogger.FatalOnError(err)

	tctx.Env = env
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
