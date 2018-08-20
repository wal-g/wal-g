package walg

// S3TarBallMaker creates tarballs that are uploaded to S3.
type S3TarBallMaker struct {
	partCount  int
	backupName string
	uploader   *Uploader
}

func NewS3TarBallMaker(backupName string, uploader *Uploader) *S3TarBallMaker {
	return &S3TarBallMaker{0, backupName, uploader}
}

// Make returns a tarball with required S3 fields.
func (tarBallMaker *S3TarBallMaker) Make(dedicatedUploader bool) TarBall {
	tarBallMaker.partCount++
	uploader := tarBallMaker.uploader
	if dedicatedUploader {
		uploader = uploader.Clone()
	}
	return &S3TarBall{
		partNumber: tarBallMaker.partCount,
		backupName: tarBallMaker.backupName,
		uploader:   uploader,
	}
}
