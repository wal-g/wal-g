package main

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"os"
)

func BuildBase(testContext *TestContextType) {
	var err error
	testContext.DockerClient, err = client.NewClientWithOpts(client.WithVersion("1.40"))
	if err != nil {
		panic(err)
	}
	conf := getConfiguration(testContext)
	opts := types.ImageBuildOptions{
		Tags: []string{conf.BaseImages["mongodb-backup-base"].tag},
	}
	buildContext, err := archive.TarWithOptions(conf.BaseImages["mongodb-backup-base"].path, &archive.TarOptions{})
	if err != nil {
		panic(err)
	}
	_, err = testContext.DockerClient.ImageBuild(context.Background(), buildContext, opts)
	if err != nil {
		panic(err)
	}
	testContext.Configuration = conf
}

func Stop(testContext *TestContextType) {
	callCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func Start(testContext *TestContextType) {
	testContext.Env = mergeEnvs(os.Environ(), testContext.Env)
	createNet(testContext, GetVarFromEnvList(testContext.Env, "TEST_ID"))
	fmt.Printf("`docker-compose build` is running\n")
	callCompose(testContext, []string{"build"})
	fmt.Printf("`docker-compose up --detach --build --force-recreate` is running\n")
	callCompose(testContext, []string{"up", "--detach", "--build", "--force-recreate"})
}

func SetupStaging(testContext *TestContextType) {
	for key, value := range generateSecrets(testContext) {
		Env[key] = value
	}

	updateFileValues("./staging/images/minio/Dockerfile", map[string]string{
		"ENV MINIO_ACCESS_KEY ": Env["MINIO_ACCESS_KEY"],
		"ENV MINIO_SECRET_KEY ": Env["MINIO_SECRET_KEY"],
	})

	updateFileValues("./staging/images/base/config/.walg", map[string]string{
		"\"AWS_ACCESS_KEY_ID\": ":     "\"" + Env["MINIO_ACCESS_KEY"] + "\",",
		"\"AWS_SECRET_ACCESS_KEY\": ": "\"" + Env["MINIO_SECRET_KEY"] + "\",",
	})

	updateFileValues("./staging/images/mongodb/config/s3cmd.conf", map[string]string{
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

	testContext.Env = getTestEnv(testContext)
}
