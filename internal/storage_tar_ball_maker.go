package internal

// StorageTarBallMaker creates tarballs that are uploaded to storage.
type StorageTarBallMaker struct {
	partCount       int
	backupName      string
	uploader        *Uploader
	resolveNames    bool
	tarNameResolver *TarCopiesNameResolver
}

func NewStorageTarBallMaker(backupName string, resolveNames bool, uploader *Uploader) *StorageTarBallMaker {
	if resolveNames {
		return &StorageTarBallMaker{0, backupName, uploader, true, NewTarCopiesNameResolver()}
	}
	return &StorageTarBallMaker{0, backupName, uploader, false, nil}
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
		partNumber:   tarBallMaker.partCount,
		backupName:   tarBallMaker.backupName,
		uploader:     uploader,
		partSize:     &size,
		resolver:     tarBallMaker.tarNameResolver,
	}
}

func (tarBallMaker *StorageTarBallMaker) AddCopiedTarName(tarName string) {
	if tarBallMaker.resolveNames {
		tarBallMaker.tarNameResolver.copiedTarNames[tarName] = true
	}
}