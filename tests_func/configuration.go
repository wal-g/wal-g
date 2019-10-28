package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type TempNameType1 struct {
	tag  string
	path string
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
	BaseImages           map[string]TempNameType1
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

func getConfiguration(testContext *TestContextType) ConfigurationType {
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
					"ssh": 22,
				},
				DockerInstances: 2,
				ExternalLinks:   []string{dynamicConfig.s3.host, dynamicConfig.s3.fakeHost},
			},
			"minio": {
				Build:  "images/minio",
				Expose: map[string]int{"http": 9000},
			},
		},
		BaseImages: map[string]TempNameType1{
			"mongodb-backup-base": {
				tag:  "mongodb-backup-base",
				path: "images/base",
			},
		},
	}
	return configuration
}

type S3Configuration struct {
	host               string
	fakeHost           string
	fakeHostPort       string
	bucket             string
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
	path string
}

type DynamicConfigurationType struct {
	s3   S3Configuration
	gpg  GPGConfiguration
	walg WalgConfiguration
}

func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789")
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

func getDynamicConfiguration(testContext *TestContextType) DynamicConfigurationType {
	accessKeyId := "1NdvGnOio1ad3HFmWNae"
	accessSecretKey := "PZK1ZuHiVM7I8vzLfBeEf6yfElqrXrZdNfaPORIM"

	return DynamicConfigurationType{
		s3: S3Configuration{
			host:               fmt.Sprintf("minio01.%s", GetVarFromEnvList(testContext.Env, "TEST_ID")),
			fakeHost:           "minio",
			fakeHostPort:       "minio:9000",
			bucket:             "dbaas",
			endpoint:           "http://minio:9000",
			accessSecretKey:    accessSecretKey,
			accessKeyId:        accessKeyId,
			encAccessSecretKey: "TODO",
			encAccessKeyId:     "TODO",
		},
		gpg: GPGConfiguration{
			privateKey: "TODO",
			keyId:      "TODO",
			user:       "mongodb",
			homedir:    "/home/mongodb/.gnupg",
		},
		walg: WalgConfiguration{path: "mongodb-backup/test_uuid/test_mongodb"},
	}
}
