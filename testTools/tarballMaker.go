package tools

import (
	"github.com/katie31/wal-g"
)

type FileTarBallMaker struct {
	number  int
	size    int64
	BaseDir string
	Trim    string
	Out     string
}

func (f *FileTarBallMaker) Make() walg.TarBall {
	f.number += 1
	return &FileTarBall{
		number:  f.number,
		size:    f.size,
		baseDir: f.BaseDir,
		trim:    f.Trim,
		out:     f.Out,
	}
}

type NOPTarBallMaker struct {
	number  int
	size    int64
	BaseDir string
	Trim    string
	Nop     bool
}

func (n *NOPTarBallMaker) Make() walg.TarBall {
	n.number += 1
	return &NOPTarBall{
		number:  n.number,
		size:    n.size,
		baseDir: n.BaseDir,
		nop:     n.Nop,
		trim:    n.Trim,
	}
}
