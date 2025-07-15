package yckms

import (
	"github.com/wal-g/tracelog"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/go-sdk/iamkey"
)

func resolveCredentials(saFilePath string) ycsdk.Credentials {
	var credentials ycsdk.Credentials
	credentials = ycsdk.InstanceServiceAccount()

	iamKey, keyErr := iamkey.ReadFromJSONFile(saFilePath)
	if keyErr == nil {
		creds, credsErr := ycsdk.ServiceAccountKey(iamKey)
		if credsErr != nil {
			tracelog.WarningLogger.Println("can't read yc service account file, will try to use metadata service:", credsErr)
			return credentials
		}
		tracelog.WarningLogger.Println("can't read yc service account file, will try to use metadata service:", keyErr)
		credentials = creds
	}

	return credentials
}
