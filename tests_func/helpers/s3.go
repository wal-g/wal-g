package helpers

import (
	"fmt"
	"github.com/docker/docker/api/types"
	"strings"
)

func ConfigureS3(testContext *TestContextType, containerName *types.Container) {
	var response string
	for i := 0; i < 100; i++ {
		bucketName := testContext.Configuration.DynamicConfiguration.S3.Bucket
		accessKeyId := testContext.Configuration.DynamicConfiguration.S3.accessKeyId
		accessSecretKey := testContext.Configuration.DynamicConfiguration.S3.accessSecretKey

		command := []string{"mc", "--debug", "config", "host", "add", "local", "http://localhost:9000", accessKeyId, accessSecretKey}
		response = RunCommandInContainer(testContext, containerName.Names[0], command)

		command = []string{"mc", "mb", fmt.Sprintf("local/%s", bucketName)}
		response = RunCommandInContainer(testContext, containerName.Names[0], command)

		if strings.Contains(response, "created successfully") ||
			strings.Contains(response, "already own it") {
			return
		}
	}
	if !strings.Contains(response, "created successfully") {
		panic(fmt.Errorf("s3 is not available: %s", response))
	}
}
