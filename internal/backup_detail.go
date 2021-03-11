package internal

// BackupDetails is used to append ExtendedMetadataDto details to BackupTime struct
type BackupDetail struct {
	BackupTime
	ExtendedMetadataDto
}

type BackupDetailSlice struct {
	Data []BackupDetail                 `json:"data"`
	TimeDenotation BackupTimeDenotation `json:"time_denotation"`
}
