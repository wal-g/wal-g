package internal

// BackupDetails is used to append ExtendedMetadataDto details to BackupTime struct
type BackupDetail struct {
	BackupTime
	ExtendedMetadataDto
}

// writeBackupTime sets matching elements in BackupDetail with values from BackupTime
func (backupDetail *BackupDetail) writeBackupTime(backupTime BackupTime) {
	backupDetail.BackupName = backupTime.BackupName
	backupDetail.Time = backupTime.Time
	backupDetail.WalFileName = backupTime.WalFileName
}

// writeExtendedMetadataDto sets matching elements in BackupDetail with values from ExtendedMetadataDto
func (backupDetail *BackupDetail) writeExtendedMetadataDto(extendedMetadataDto ExtendedMetadataDto) {
	backupDetail.DataDir = extendedMetadataDto.DataDir
	backupDetail.FinishLsn = extendedMetadataDto.FinishLsn
	backupDetail.FinishTime = extendedMetadataDto.FinishTime
	backupDetail.Hostname = extendedMetadataDto.Hostname
	backupDetail.PgVersion = extendedMetadataDto.PgVersion
	backupDetail.StartLsn = extendedMetadataDto.StartLsn
	backupDetail.StartTime = extendedMetadataDto.StartTime
}
