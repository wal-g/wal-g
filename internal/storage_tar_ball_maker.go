package internal

// StorageTarBallMaker creates tarballs that are uploaded to storage.
type StorageTarBallMaker struct {
	partCount  int
	backupName string
	uploader   *Uploader
}

func NewStorageTarBallMaker(backupName string, uploader *Uploader) *StorageTarBallMaker {
	return &StorageTarBallMaker{0, backupName, uploader}
}

// Make returns a tarball with required storage fields.
func (tarBallMaker *StorageTarBallMaker) Make(dedicatedUploader bool) TarBall {
	tarBallMaker.partCount++
	uploader := tarBallMaker.uploader
	if dedicatedUploader {
		uploader = uploader.Clone()
	}
	size := int64(0)
	return &StorageTarBall{
		partNumber: tarBallMaker.partCount,
		backupName: tarBallMaker.backupName,
		uploader:   uploader,
		size:       &size,
	}
}
