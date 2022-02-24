package greenplum

import (
	"fmt"
	"strconv"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
)

func SetSegmentStoragePrefix(contentID int) {
	viper.Set(internal.StoragePrefixSetting, FormatSegmentStoragePrefix(contentID))
}

func ConfigureSegContentID(contentIDFlag string) (int, error) {
	var rawContentID string
	if contentIDFlag != "" {
		rawContentID = contentIDFlag
	} else if contentIDSetting, ok := internal.GetSetting(internal.GPSegContentID); ok {
		rawContentID = contentIDSetting
	} else {
		return 0, fmt.Errorf("segment content ID is not specified, add the --content-id flag or use the %s setting", internal.GPSegContentID)
	}

	contentID, err := strconv.Atoi(rawContentID)
	if err != nil {
		return 0, fmt.Errorf("failed to parse the segment content ID: %v", err)
	}

	return contentID, nil
}

//initGpLog is required for gp-common-go library to function properly
func initGpLog(logsDir string) {
	gplog.InitializeLogging("wal-g", logsDir)
	gplog.SetLogFileNameFunc(func(program, logdir string) string {
		return fmt.Sprintf("%s/%s-gplog.log", logdir, program)
	})
}
