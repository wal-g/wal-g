package functest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"

	testConf "github.com/wal-g/wal-g/tests_func/config"
	testHelper "github.com/wal-g/wal-g/tests_func/helpers"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"

	"github.com/docker/docker/client"
	"strconv"
	"math/rand"
)

func BuildBase(testContext *testHelper.TestContextType) error {
	cmd := exec.Command("docker", "build", "-t", testContext.Env["MONGODB_BACKUP_BASE_TAG"], testContext.Env["MONGODB_BACKUP_BASE_PATH"])
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error in building base: %v", err)
	}

	if err := testHelper.CreateNet(testContext, testContext.Env["TEST_ID"]); err != nil {
		return err
	}
	fmt.Printf("`docker-compose build` is running\n")
	return testHelper.CallCompose(testContext, []string{"--verbose", "--log-level", "WARNING", "build"})
}

func Stop(testContext *testHelper.TestContextType) error {
	return testHelper.CallCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func StartRecreate(testContext *testHelper.TestContextType) error {
	fmt.Printf("`docker-compose up --detach --build --force-recreate` is running\n")
	return testHelper.CallCompose(testContext, []string{"--verbose", "--log-level", "WARNING", "up", "--detach", "--build", "--force-recreate"})
}

func EnvExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func SetupEnv(envFilePath, stagingDir string) error {
	if err := os.Mkdir(stagingDir, 0755); err != nil {
		return fmt.Errorf("can not create staging dir: %v", err)
	}
	env := testUtils.MergeEnvs(testConf.Env, DynConf(testConf.Env))

	file, err := os.OpenFile(envFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("can not open env file for writing: %v", err)
	}
	defer func() { _ = file.Close() }()

	for _, line := range testUtils.EnvToList(env) {
		if _, err := file.WriteString(line); err != nil {
			return err
		}
		if err := testUtils.WriteEnv(env, file); err != nil {
			return fmt.Errorf("can not write to env file: %v", err)
		}
	}
	return nil
}

func ReadEnv(path string) (map[string]string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("can not open env file: %v", err)
	}
	defer func() { _ = file.Close() }()
	envLines, err := testUtils.ReadLines(file)
	if err != nil {
		return nil, err
	}
	return testUtils.ParseEnvLines(envLines), nil
}

func ShutdownEnv(testContext *testHelper.TestContextType) error {
	if err := testHelper.ShutdownContainers(testContext); err != nil {
		return err
	}
	if err := testHelper.ShutdownNetwork(testContext); err != nil {
		return err
	}

	// TODO: Enable net cleanup
	//if err := testHelper.RemoveNet(testContext); err != nil {
	//	log.Fatalln(err)
	//}

	if err := os.RemoveAll(testConf.Env["STAGING_DIR"]); err != nil {
		return err
	}
	return nil
}

func SetupStaging(testContext *testHelper.TestContextType) error {
	if err := testUtils.CopyDirectory(testContext.Env["IMAGES_DIR"], path.Join(testContext.Env["STAGING_DIR"], testContext.Env["IMAGES_DIR"])); err != nil {
		return fmt.Errorf("can not copy images into staging: %v", err)
	}
	return nil
}

func SetupTestContext(testContext *testHelper.TestContextType) error {
	var err error
	testContext.Context = context.Background()
	testContext.DockerClient, err = client.NewClientWithOpts(client.WithVersion("1.38"))
	if err != nil {
		return fmt.Errorf("error in building base: %v", err)
	}
	return nil
}

func DynConf(env map[string]string) map[string]string {
	portFactor := env["TEST_ID"]
	netName := fmt.Sprintf("test_net_%s", portFactor)

	return map[string]string{
		"DOCKER_BRIDGE_ID": strconv.Itoa(rand.Intn(65535)),
		"PORT_FACTOR":      portFactor,
		"NETWORK_NAME":     netName,
	}
}
