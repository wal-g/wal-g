package greenplum

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	RestorePoint *string   `json:"RestorePoint,omitempty"`
	BackupNames  *[]string `json:"BackupNames,omitempty"`
}

// NewBackupSentinelDto returns new BackupSentinelDto instance
func NewBackupSentinelDto(curBackupInfo CurBackupInfo) BackupSentinelDto {
	sentinel := BackupSentinelDto{
		RestorePoint: &curBackupInfo.backupName,
		BackupNames:  &curBackupInfo.pgBackupNames,
	}
	return sentinel
}
