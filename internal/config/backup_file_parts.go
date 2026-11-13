package config

import (
	"fmt"
	"strconv"
)

func GetBackupFilePartSize() (int64, error) {
	value, ok := GetSetting(BackupFilePartSize)
	if !ok {
		return 0, nil
	}
	partSize, err := strconv.ParseInt(value, 10, 64)
	if err != nil || partSize < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer number of bytes", BackupFilePartSize)
	}
	return partSize, nil
}
