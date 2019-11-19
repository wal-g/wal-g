package functest

import (
	"fmt"
	"github.com/docker/docker/client"
	c "github.com/wal-g/wal-g/tests_func/config"
	h "github.com/wal-g/wal-g/tests_func/helpers"
	u "github.com/wal-g/wal-g/tests_func/utils"
	"os"
	"os/exec"
)

func BuildBase(testContext *TestContextType) {
	var err error
	testContext.DockerClient, err = client.NewClientWithOpts(client.WithVersion("1.40"))
	if err != nil {
		panic(err)
	}
	testContext.Env = u.MergeEnvs(testContext.Env, h.GetConfiguration(testContext))
	cmd := exec.Command("docker", "build", "-t", u.GetVarFromEnvList(testContext.Env, "MONGODB_BACKUP_BASE_TAG"), u.GetVarFromEnvList(testContext.Env, "MONGODB_BACKUP_BASE_PATH"))
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	if err != nil {
		panic(err)
	}
	testContext.Configuration = conf
}

func Stop(testContext *TestContextType) {
	CallCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func Start(testContext *TestContextType) {
	testContext.Env = MergeEnvs(os.Environ(), testContext.Env)
	CreateNet(testContext, GetVarFromEnvList(testContext.Env, "TEST_ID"))
	fmt.Printf("`docker-compose build` is running\n")
	CallCompose(testContext, []string{"build"})
	fmt.Printf("`docker-compose up --detach --build --force-recreate` is running\n")
	CallCompose(testContext, []string{"up", "--detach", "--build", "--force-recreate"})
}

func SetupStaging(testContext *TestContextType) {
	for key, value := range GenerateSecrets() {
		Env[key] = value
	}

	UpdateFileValues("./staging/images/minio/Dockerfile", map[string]string{
		"ENV MINIO_ACCESS_KEY ": Env["MINIO_ACCESS_KEY"],
		"ENV MINIO_SECRET_KEY ": Env["MINIO_SECRET_KEY"],
	})

	UpdateFileValues("./staging/images/base/config/.walg", map[string]string{
		"\"AWS_ACCESS_KEY_ID\": ":     "\"" + Env["MINIO_ACCESS_KEY"] + "\",",
		"\"AWS_SECRET_ACCESS_KEY\": ": "\"" + Env["MINIO_SECRET_KEY"] + "\",",
	})

	UpdateFileValues("./staging/images/mongodb/config/s3cmd.conf", map[string]string{
		"access_key = ": Env["MINIO_ACCESS_KEY"],
		"secret_key = ": Env["MINIO_SECRET_KEY"],
	})

	stagingDir := Env["STAGING_DIR"]
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		err = os.Mkdir(stagingDir, os.ModeDir)
		if err != nil {
			panic(err)
		}
	}

	envFile := Env["ENV_FILE"]
	_, err := os.Stat(envFile)
	file, err := os.OpenFile(envFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	for key, value := range Env {
		_, err = fmt.Fprintf(file, "%s=%s\n", key, value)
		if err != nil {
			panic(err)
		}
	}

	testContext.Env = GetTestEnv(testContext)
}
