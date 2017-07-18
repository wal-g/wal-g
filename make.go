package walg

import ()

type TarBallMaker interface {
	Make() TarBall
}

type FileTarBallMaker struct {
	number  int
	size    int64
	BaseDir string
	Trim    string
	Out     string
}

func (f *FileTarBallMaker) Make() TarBall {
	f.number += 1
	return &FileTarBall{
		number:  f.number,
		size:    f.size,
		baseDir: f.BaseDir,
		trim:    f.Trim,
		out:     f.Out,
	}
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
	s.number += 1
	return &S3TarBall{
		number:   s.number,
		size:     s.size,
		baseDir:  s.BaseDir,
		trim:     s.Trim,
		bkupName: s.BkupName,
		tu:       s.Tu,
	}
}

type NOPTarBallMaker struct {
	number  int
	size    int64
	BaseDir string
	Trim    string
	Nop     bool
}

func (n *NOPTarBallMaker) Make() TarBall {
	n.number += 1
	return &NOPTarBall{
		number:  n.number,
		size:    n.size,
		baseDir: n.BaseDir,
		nop:     n.Nop,
		trim:    n.Trim,
	}
}
