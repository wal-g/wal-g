package functest

import (
	"fmt"
	"github.com/docker/docker/client"
	testConf "github.com/wal-g/wal-g/tests_func/config"
	testHelper "github.com/wal-g/wal-g/tests_func/helpers"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"
	"os"
	"os/exec"
)

func BuildBase(testContext *testHelper.TestContextType) error {
	var err error
	testContext.DockerClient, err = client.NewClientWithOpts(client.WithVersion("1.38"))
	if err != nil {
		return fmt.Errorf("error in building base: %v", err)
	}
	testContext.Env = testUtils.MergeEnvs(testContext.Env, testHelper.GetConfiguration(testContext))
	cmd := exec.Command("docker", "build", "-t", testUtils.GetVarFromEnvList(testContext.Env, "MONGODB_BACKUP_BASE_TAG"), testUtils.GetVarFromEnvList(testContext.Env, "MONGODB_BACKUP_BASE_PATH"))
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error in building base: %v", err)
	}
	return nil
}

func Stop(testContext *testHelper.TestContextType) error {
	return testHelper.CallCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func Start(testContext *testHelper.TestContextType) error {
	testContext.Env = testUtils.MergeEnvs(os.Environ(), testContext.Env)
	err := testHelper.CreateNet(testContext, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	if err != nil {
		return err
	}
	fmt.Printf("`docker-compose build` is running\n")
	err = testHelper.CallCompose(testContext, []string{"build"})
	if err != nil {
		return err
	}
	fmt.Printf("`docker-compose up --detach --build --force-recreate` is running\n")
	return testHelper.CallCompose(testContext, []string{"up", "--detach", "--build", "--force-recreate"})
}

func SetupStaging(testContext *testHelper.TestContextType) error {
	for key, value := range testUtils.GenerateSecrets() {
		testConf.Env[key] = value
	}

	err := testUtils.CopyDirectory("./images/", "./staging/images")
	if err != nil {
		return fmt.Errorf("error in setuping staging: %v", err)
	}

	testUtils.UpdateFileValues("./staging/images/minio/Dockerfile", map[string]string{
		"MINIO_ACCESS_KEY": testConf.Env["MINIO_ACCESS_KEY"],
		"MINIO_SECRET_KEY": testConf.Env["MINIO_SECRET_KEY"],
	})

	testUtils.UpdateFileValues("./staging/images/base/config/.walg", map[string]string{
		"MINIO_ACCESS_KEY": testConf.Env["MINIO_ACCESS_KEY"],
		"MINIO_SECRET_KEY": testConf.Env["MINIO_SECRET_KEY"],
	})

	testUtils.UpdateFileValues("./staging/images/mongodb/config/s3cmd.conf", map[string]string{
		"MINIO_ACCESS_KEY": testConf.Env["MINIO_ACCESS_KEY"],
		"MINIO_SECRET_KEY": testConf.Env["MINIO_SECRET_KEY"],
	})

	stagingDir := testConf.Env["STAGING_DIR"]
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		err = os.Mkdir(stagingDir, os.ModeDir)
		if err != nil {
			return fmt.Errorf("error in setuping staging: %v", err)
		}
	}

	envFile := testConf.Env["ENV_FILE"]
	_, err = os.Stat(envFile)
	file, err := os.OpenFile(envFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error in setuping staging: %v", err)
	}
	defer file.Close()
	for key, value := range testConf.Env {
		_, err = fmt.Fprintf(file, "%s=%s\n", key, value)
		if err != nil {
			return fmt.Errorf("error in setuping staging: %v", err)
		}
	}

	testContext.Env, err = testHelper.GetTestEnv(testContext)

	return err
}
