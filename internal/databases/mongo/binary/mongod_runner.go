package binary

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type MongodProcess struct {
	config     *MongodFileConfig
	parameters []string
	port       int
	cancel     context.CancelFunc
	cmd        *exec.Cmd
}

func StartMongodWithDisableLogicalSessionCacheRefresh(config *MongodFileConfig) (*MongodProcess, error) {
	return StartMongo(config, "disableLogicalSessionCacheRefresh=true")
}

func StartMongodWithRecoverFromOplogAsStandalone(config *MongodFileConfig) (*MongodProcess, error) {
	return StartMongo(config, "recoverFromOplogAsStandalone=true", "takeUnstableCheckpointOnShutdown=true")
}

func StartMongo(config *MongodFileConfig, parameters ...string) (*MongodProcess, error) {
	mongodProcess := &MongodProcess{
		config:     config,
		parameters: parameters,
	}

	err := mongodProcess.start()
	if err != nil {
		return nil, err
	}
	return mongodProcess, nil
}

func (mongodProcess *MongodProcess) GetHostWithPort() string {
	return fmt.Sprintf("localhost:%d", mongodProcess.port)
}

func (mongodProcess *MongodProcess) GetURI() string {
	return "mongodb://" + mongodProcess.GetHostWithPort()
}

func (mongodProcess *MongodProcess) Wait() error {
	tracelog.InfoLogger.Printf("Waiting for the mongod %v to be stopped", mongodProcess.GetURI())
	err := mongodProcess.cmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Mongod %v stopped with error %v", mongodProcess.GetURI(), err)
	} else {
		tracelog.InfoLogger.Printf("Mongod %v stopped successfully!", mongodProcess.GetURI())
	}
	mongodProcess.cancel()
	return err
}

func (mongodProcess *MongodProcess) start() error {
	unusedPort, err := randomUnusedPort()
	if err != nil {
		return err
	}
	mongodProcess.port = unusedPort

	configFilePath, err := mongodProcess.config.SaveConfigToTempFile("storage", "systemLog")
	if err != nil {
		return err
	}

	cliArgs := []string{"--port", strconv.Itoa(unusedPort), "--config", configFilePath}
	for _, parameter := range mongodProcess.parameters {
		cliArgs = append(cliArgs, "--setParameter", parameter)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mongodProcess.cmd = exec.CommandContext(ctx, "mongod", cliArgs...)

	tracelog.InfoLogger.Printf("Starting mongod by command: %v", mongodProcess.cmd)
	err = mongodProcess.cmd.Start()
	if err != nil {
		cancel()
		return err
	}

	mongodProcess.cancel = cancel
	return nil
}

func randomUnusedPort() (int, error) {
	listen, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, errors.Wrap(err, "unable to choose random unused port")
	}
	defer utility.LoggedClose(listen, "unable to close listen")

	port := listen.Addr().(*net.TCPAddr).Port
	return port, nil
}
