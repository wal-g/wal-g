package postgres

import (
	"context"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"testing"
)

const (
	deltaFromUserDataFlag    = "delta-from-user-data"
	deltaFromNameFlag        = "delta-from-name"
	withoutFilesMetadataFlag = "without-files-metadata"
)

var (
	permanent             = false
	fullBackup            = false
	verifyPageChecksums   = false
	storeAllCorruptBlocks = false
	useRatingComposer     = false
	useCopyComposer       = false
	deltaFromName         = ""
	deltaFromUserData     = ""
	userDataRaw           = ""
	withoutFilesMetadata  = false
)

func TestBackupHandler_HandleBackupPush(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bh := initCommand()
			bh.HandleBackupPush(context.TODO())
		})
	}
}

func chooseTarBallComposer() TarBallComposerType {
	tarBallComposerType := RegularComposer

	useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
	if useRatingComposer {
		tarBallComposerType = RatingComposer
	}
	useCopyComposer = useCopyComposer || viper.GetBool(internal.UseCopyComposerSetting)
	if useCopyComposer {
		fullBackup = true
		tarBallComposerType = CopyComposer
	}

	return tarBallComposerType
}

func initCommand() *BackupHandler {
	internal.ConfigureLimiters()

	var dataDirectory string

	verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
	storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)

	tarBallComposerType := chooseTarBallComposer()

	if deltaFromName == "" {
		deltaFromName = viper.GetString(internal.DeltaFromNameSetting)
	}
	if deltaFromUserData == "" {
		deltaFromUserData = viper.GetString(internal.DeltaFromUserDataSetting)
	}
	if userDataRaw == "" {
		userDataRaw = viper.GetString(internal.SentinelUserDataSetting)
	}
	withoutFilesMetadata = withoutFilesMetadata || viper.GetBool(internal.WithoutFilesMetadataSetting)
	if withoutFilesMetadata {
		// files metadata tracking is required for delta backups and copy/rating composers
		if tarBallComposerType != RegularComposer {
			tracelog.ErrorLogger.Fatalf(
				"%s option cannot be used with non-regular tar ball composer",
				withoutFilesMetadataFlag)
		}
		if deltaFromName != "" || deltaFromUserData != "" {
			tracelog.ErrorLogger.Fatalf(
				"%s option cannot be used with %s, %s options",
				withoutFilesMetadataFlag, deltaFromNameFlag, deltaFromUserDataFlag)
		}
		tracelog.InfoLogger.Print("Files metadata tracking is disabled")
		fullBackup = true
	}

	deltaBaseSelector, err := internal.NewDeltaBaseSelector(
		deltaFromName, deltaFromUserData, NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)

	userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
	tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

	folder, err := internal.ConfigureFolder()
	uploader, err := internal.ConfigureUploaderToFolder(folder)

	arguments := NewBackupArguments(uploader, dataDirectory, utility.BaseBackupPath,
		permanent, verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting),
		fullBackup, storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting),
		tarBallComposerType, NewRegularDeltaBackupConfigurator(deltaBaseSelector),
		userData, withoutFilesMetadata)

	backupHandler, err := NewBackupHandler(arguments)
	return backupHandler
}
