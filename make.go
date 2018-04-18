package walg

// TarBallMaker is used to allow for
// flexible creation of different TarBalls.
type TarBallMaker interface {
	Make(inheritState bool) TarBall
}

// S3TarBallMaker creates tarballs that are uploaded to S3.
type S3TarBallMaker struct {
	number           int
	size             int64
	BaseDir          string
	Trim             string
	BkupName         string
	Tu               *TarUploader
	Lsn              *uint64
	IncrementFromLsn *uint64
	IncrementFrom    string
	Files            BackupFileList
}

// Make returns a tarball with required S3 fields.
func (s *S3TarBallMaker) Make(inheritState bool) TarBall {
	s.number++
	uploader := s.Tu
	if !inheritState {
		uploader = uploader.Clone()
	}
	return &S3TarBall{
		number:           s.number,
		size:             s.size,
		baseDir:          s.BaseDir,
		trim:             s.Trim,
		bkupName:         s.BkupName,
		tu:               uploader,
		Lsn:              s.Lsn,
		IncrementFromLsn: s.IncrementFromLsn,
		IncrementFrom:    s.IncrementFrom,
		Files:            s.Files,
	}
}
