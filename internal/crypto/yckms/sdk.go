package yckms

import (
	"github.com/wal-g/tracelog"
	"github.com/yandex-cloud/go-sdk/v2/credentials"
	"github.com/yandex-cloud/go-sdk/v2/pkg/iamkey"
)

func resolveCredentials(saFilePath string) credentials.Credentials {
	var result credentials.Credentials
	result = credentials.InstanceServiceAccount()

	iamKey, keyErr := iamkey.ReadFromJSONFile(saFilePath)
	if keyErr == nil {
		creds, credsErr := credentials.ServiceAccountKey(iamKey)
		if credsErr != nil {
			tracelog.WarningLogger.Println("can't read yc service account file, will try to use metadata service:", credsErr)
			return result
		}
		tracelog.WarningLogger.Println("can't read yc service account file, will try to use metadata service:", keyErr)
		result = creds
	}

	return result
}
