package internal

import (
	"time"
)

func FormatTimeInner(backupTime time.Time, timeFormat string) string {
	if backupTime.IsZero() {
		return "-"
	}
	return backupTime.Format(timeFormat)
}

func FormatTime(backupTime time.Time) string {
	return FormatTimeInner(backupTime, time.RFC3339)
}

func PrettyFormatTime(backupTime time.Time) string {
	return FormatTimeInner(backupTime, time.RFC850)
}
