package walg

// S3TarBallMaker creates tarballs that are uploaded to S3.
type S3TarBallMaker struct {
	partCount        int
	ArchiveDirectory string
	BackupName       string
	Uploader         *Uploader
	Lsn              *uint64
	IncrementFromLsn *uint64
	IncrementFrom    string
}

// Make returns a tarball with required S3 fields.
func (tarBallMaker *S3TarBallMaker) Make(dedicatedUploader bool) TarBall {
	tarBallMaker.partCount++
	uploader := tarBallMaker.Uploader
	if dedicatedUploader {
		uploader = uploader.Clone()
	}
	return &S3TarBall{
		partCount:        tarBallMaker.partCount,
		archiveDirectory: tarBallMaker.ArchiveDirectory,
		backupName:       tarBallMaker.BackupName,
		uploader:         uploader,
		Lsn:              tarBallMaker.Lsn,
		IncrementFromLsn: tarBallMaker.IncrementFromLsn,
		IncrementFrom:    tarBallMaker.IncrementFrom,
	}
}
