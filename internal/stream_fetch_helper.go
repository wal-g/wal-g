package internal

import (
	"time"
)

// GetOperationLogsSettings ... TODO
func GetOperationLogsSettings(OperationLogEndTsSetting string, operationLogsDstSetting string) (endTS *time.Time, dstFolder string, err error) {
	endTSStr, ok := GetSetting(OperationLogEndTsSetting)
	if ok {
		t, err := time.Parse(time.RFC3339, endTSStr)
		if err != nil {
			return nil, "", err
		}
		endTS = &t
	}
	dstFolder, ok = GetSetting(operationLogsDstSetting)
	if !ok {
		return endTS, dstFolder, NewUnsetRequiredSettingError(operationLogsDstSetting)
	}
	return endTS, dstFolder, nil
}
