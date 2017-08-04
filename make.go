package walg

import ()

/**
 *  Generic TarBall handler.
 */
type TarBallMaker interface {
	Make() TarBall
}

/**
 *  Handles all tarballs that are uploaded to S3.
 */
type S3TarBallMaker struct {
	number   int
	size     int64
	BaseDir  string
	Trim     string
	BkupName string
	Tu       *TarUploader
}

/**
 *  Returns a tarball with fields needed in order to
 *  upload to S3.
 */
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
