package tools

import (
	"github.com/wal-g/wal-g"
)

// FileTarBallMaker creates a new FileTarBall
// with the directory that files should be
// extracted to.
type FileTarBallMaker struct {
	number           int
	size             int64
	ArchiveDirectory string
	Out              string
}

// Make creates a new FileTarBall.
func (f *FileTarBallMaker) Make(inheritState bool) walg.TarBall {
	f.number++
	return &FileTarBall{
		number:           f.number,
		size:             f.size,
		archiveDirectory: f.ArchiveDirectory,
		out:              f.Out,
	}
}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number  int
	size    int64
	Trim    string
}

// Make creates a new NOPTarBall.
func (n *NOPTarBallMaker) Make(inheritState bool) walg.TarBall {
	n.number++
	return &NOPTarBall{
		number:           n.number,
		size:             n.size,
		archiveDirectory: n.Trim,
	}
}
