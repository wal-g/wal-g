package walg

import ()

type TarBallMaker interface {
	Make() TarBall
}

type S3TarBallMaker struct {
	number   int
	size     int64
	BaseDir  string
	Trim     string
	BkupName string
	Tu       *TarUploader
}

func (s *S3TarBallMaker) Make() TarBall {
	s.number++
	return &S3TarBall{
		number:   s.number,
		size:     s.size,
		baseDir:  s.BaseDir,
		trim:     s.Trim,
		bkupName: s.BkupName,
		tu:       s.Tu,
	}
}
