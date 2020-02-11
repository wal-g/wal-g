package functests

import (
	"flag"
	"os"
	"testing"

	"github.com/wal-g/wal-g/tests_func/config"
	"github.com/wal-g/wal-g/tests_func/utils"

	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/colors"
	"github.com/wal-g/tracelog"
)

var opt = godog.Options{
	Output:        colors.Colored(os.Stdout),
	Format:        "pretty",
	StopOnFailure: true,
	Strict:        true,
}

var cleanEnv bool

func init() {
	flag.BoolVar(&cleanEnv, "env.clean", false, "shutdown and delete test environment")
	godog.BindFlags("godog.", flag.CommandLine, &opt)
}

func TestMain(m *testing.M) {
	flag.Parse()
	opt.Paths = flag.Args()

	environ := utils.ParseEnvLines(os.Environ())
	debug := environ["DEBUG"] != ""
	if debug {
		err := tracelog.UpdateLogLevel(tracelog.DevelLogLevel)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	tctx, err := NewTestContext()
	tracelog.ErrorLogger.FatalOnError(err)

	if cleanEnv {
		env, err := ReadEnv(config.Env["ENV_FILE"])
		tracelog.ErrorLogger.FatalOnError(err)

		tctx.Env = env
		err = tctx.ShutdownEnv()
		tracelog.ErrorLogger.FatalOnError(err)

		return
	}

	tracelog.InfoLogger.Printf("Starting testing environment: mongodb %s (%s)",
		environ["MONGO_MAJOR"], environ["MONGO_VERSION"])

	status := godog.RunWithOptions("godogs", func(s *godog.Suite) {
		tctx.setupSuites(s)
	}, opt)

	if st := m.Run(); st > status {
		status = st
	}

	if debug {
		os.Exit(status)
	}

	err = tctx.ShutdownEnv()
	tracelog.ErrorLogger.FatalOnError(err)
}
