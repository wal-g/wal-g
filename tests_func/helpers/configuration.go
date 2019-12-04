package helpers

import (
	"bufio"
	"fmt"
	. "github.com/wal-g/wal-g/tests_func/config"
	. "github.com/wal-g/wal-g/tests_func/utils"
	"math/rand"
	"os"
)

type BaseImageType struct {
	Tag  string
	Path string
}

type ConfigurationType struct {
	DynamicConfiguration DynamicConfigurationType
	Cleanup              bool
	StagingDir           string
	ImagesDir            string
	GitCloneOverwrite    bool
	DockerBridgeName     string
	DockerBridgeId       int
	DockerIP4Subnet      string
	DockerIP6Subnet      string
	PortFactor           string
	NetworkName          string
	Projects             map[string]DockerComposeConfiguration
	BaseImages           map[string]BaseImageType
}

type UserConfiguration struct {
	Username string
	Password string
	Dbname   string
	Roles    []string
}

type DockerComposeConfiguration struct {
	Build           string
	Users           map[string]UserConfiguration
	Expose          map[string]int
	DockerInstances int
	ExternalLinks   []string
}

func GetConfiguration(testContext *TestContextType) ConfigurationType {
	portFactor := GetVarFromEnvList(testContext.Env, "TEST_ID")
	netName := fmt.Sprintf("test_net_%s", portFactor)
	dynamicConfig := getDynamicConfiguration(testContext)
	configuration := ConfigurationType{
		DynamicConfiguration: dynamicConfig,
		Cleanup:              true,
		StagingDir:           "staging",
		ImagesDir:            "images",
		GitCloneOverwrite:    false,
		DockerBridgeName:     "dbaas",
		DockerBridgeId:       rand.Intn(65535),
		DockerIP4Subnet:      "10.%s.0/24",
		DockerIP6Subnet:      "fd00:dead:beef:%s::/96",
		PortFactor:           portFactor,
		NetworkName:          netName,
		Projects: map[string]DockerComposeConfiguration{
			"base": {DockerInstances: 0},
			"mongodb": {
				Build: "..",
				Users: map[string]UserConfiguration{
					"admin": {
						Username: "admin",
						Password: "password",
						Dbname:   "admin",
						Roles:    []string{"root"},
					},
				},
				Expose: map[string]int{
					"mongod": 27018,
					"ssh":    22,
				},
				DockerInstances: 2,
				ExternalLinks:   []string{dynamicConfig.S3.host, dynamicConfig.S3.fakeHost},
			},
			"minio": {
				Build:  "staging/images/minio",
				Expose: map[string]int{"http": 9000},
			},
		},
		BaseImages: map[string]BaseImageType{
			"mongodb-backup-base": {
				Tag:  "mongodb-backup-base",
				Path: "staging/images/base",
			},
		},
	}
	return configuration
}

type S3Configuration struct {
	host               string
	fakeHost           string
	fakeHostPort       string
	Bucket             string
	endpoint           string
	accessSecretKey    string
	accessKeyId        string
	encAccessSecretKey string
	encAccessKeyId     string
}

type GPGConfiguration struct {
	privateKey string
	keyId      string
	user       string
	homedir    string
}

type WalgConfiguration struct {
	Path string
}

type DynamicConfigurationType struct {
	S3   S3Configuration
	Gpg  GPGConfiguration
	Walg WalgConfiguration
}

func getDynamicConfiguration(testContext *TestContextType) DynamicConfigurationType {
	return DynamicConfigurationType{
		S3: S3Configuration{
			host:               fmt.Sprintf("minio01.%s", GetVarFromEnvList(testContext.Env, "TEST_ID")),
			fakeHost:           "minio",
			fakeHostPort:       "minio:9000",
			Bucket:             "dbaas",
			endpoint:           "http://minio:9000",
			accessSecretKey:    GetVarFromEnvList(testContext.Env, "MINIO_SECRET_KEY"),
			accessKeyId:        GetVarFromEnvList(testContext.Env, "MINIO_ACCESS_KEY"),
			encAccessSecretKey: "TODO",
			encAccessKeyId:     "TODO",
		},
		Gpg: GPGConfiguration{
			privateKey: "TODO",
			keyId:      "TODO",
			user:       "mongodb",
			homedir:    "/home/mongodb/.gnupg",
		},
		Walg: WalgConfiguration{Path: GetVarFromEnvList(testContext.Env, "WALG_S3_PREFIX")},
	}
}

func GetTestEnv(testContext *TestContextType) []string {
	if testContext.Env != nil {
		return testContext.Env
	}
	env := make([]string, 0)
	envFile := Env["ENV_FILE"]
	file, err := os.OpenFile(envFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		key, value := SplitEnvLine(line)
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return env
}
