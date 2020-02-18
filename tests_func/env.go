package functests

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"

	"github.com/wal-g/wal-g/tests_func/config"
	"github.com/wal-g/wal-g/tests_func/utils"
)

func EnvExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func SetupEnv(envFilePath, stagingDir string) error {
	if err := os.Mkdir(stagingDir, 0755); err != nil {
		return fmt.Errorf("can not create staging dir: %v", err)
	}
	env := utils.MergeEnvs(config.Env, DynConf(config.Env))
	file, err := os.OpenFile(envFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("can not open env file for writing: %v", err)
	}
	defer func() { _ = file.Close() }()

	if err := utils.WriteEnv(env, file); err != nil {
		return fmt.Errorf("can not write to env file: %v", err)
	}

	return nil
}

func ReadEnv(path string) (map[string]string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("can not open env file: %v", err)
	}
	defer func() { _ = file.Close() }()
	envLines, err := utils.ReadLines(file)
	if err != nil {
		return nil, err
	}
	return utils.ParseEnvLines(envLines), nil
}

func SetupStaging(imagesDir, stagingDir string) error {
	if err := utils.CopyDirectory(imagesDir, path.Join(stagingDir, imagesDir)); err != nil {
		return fmt.Errorf("can not copy images into staging: %v", err)
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
