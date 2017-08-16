package tools

import (
	"github.com/wal-g/wal-g"
)

// FileTarBallMaker creates a new FileTarBall
// with the directory that files should be
// extracted to.
type FileTarBallMaker struct {
	number  int
	size    int64
	BaseDir string
	Trim    string
	Out     string
}

// Make creates a new FileTarBall.
func (f *FileTarBallMaker) Make() walg.TarBall {
	f.number++
	return &FileTarBall{
		number:  f.number,
		size:    f.size,
		baseDir: f.BaseDir,
		trim:    f.Trim,
		out:     f.Out,
	}
}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number  int
	size    int64
	BaseDir string
	Trim    string
	Nop     bool
}

// Make creates a new NOPTarBall.
func (n *NOPTarBallMaker) Make() walg.TarBall {
	n.number++
	return &NOPTarBall{
		number:  n.number,
		size:    n.size,
		baseDir: n.BaseDir,
		nop:     n.Nop,
		trim:    n.Trim,
	}
}
