package helpers

import (
	"bufio"
	"fmt"
	testConf "github.com/wal-g/wal-g/tests_func/config"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"
	"math/rand"
	"os"
	"strconv"
)

type UserConfiguration struct {
	Username string
	Password string
	Dbname   string
	Roles    []string
}

func GetConfiguration(testContext *TestContextType) []string {
	portFactor := testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")
	netName := fmt.Sprintf("test_net_%s", portFactor)
	dynamicConfig := getDynamicConfiguration(testContext)

	curEnv := map[string]string{
		"DOCKER_BRIDGE_ID": strconv.Itoa(rand.Intn(65535)),
		"PORT_FACTOR":      portFactor,
		"NETWORK_NAME":     netName,
	}

	return testUtils.MergeEnvs(dynamicConfig, testUtils.MapEnvToListEnv(curEnv))
}

func getDynamicConfiguration(testContext *TestContextType) []string {
	curEnv := map[string]string{
		"S3_HOST":              fmt.Sprintf("%s.%s", testUtils.GetVarFromEnvList(testContext.Env, "S3_WORKER"), testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")),
		"S3_FAKE_HOST_PORT":    fmt.Sprintf("%s:%s", testUtils.GetVarFromEnvList(testContext.Env, "S3_FAKE_HOST"), testUtils.GetVarFromEnvList(testContext.Env, "S3_FAKE_PORT")),
		"S3_ENDPOINT":          fmt.Sprintf("http://%s:%s", testUtils.GetVarFromEnvList(testContext.Env, "S3_FAKE_HOST"), testUtils.GetVarFromEnvList(testContext.Env, "S3_FAKE_PORT")),
		"S3_ACCESS_SECRET_KEY": testUtils.GetVarFromEnvList(testContext.Env, "MINIO_SECRET_KEY"),
		"S3_ACCESS_KEY_ID":     testUtils.GetVarFromEnvList(testContext.Env, "MINIO_ACCESS_KEY"),
	}

	return testUtils.MapEnvToListEnv(curEnv)
}

func GetTestEnv(testContext *TestContextType) ([]string, error) {
	if testContext.Env != nil {
		return testContext.Env, nil
	}
	env := make([]string, 0)
	envFile := testConf.Env["ENV_FILE"]
	file, err := os.OpenFile(envFile, os.O_RDONLY, 0644)
	if err != nil {
		return []string{}, fmt.Errorf("error during writig environment variables to file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		key, value := testUtils.SplitEnvLine(line)
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	if err := scanner.Err(); err != nil {
		return []string{}, fmt.Errorf("error during writig environment variables to file: %v", err)
	}
	return env, nil
}