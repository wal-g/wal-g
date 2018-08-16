package testtools

import (
	"bytes"
	"github.com/wal-g/wal-g"
)

// FileTarBallMaker creates a new FileTarBall
// with the directory that files should be
// extracted to.
type FileTarBallMaker struct {
	number int
	size   int64
	Out    string
}

// Make creates a new FileTarBall.
func (tarBallMaker *FileTarBallMaker) Make(inheritState bool) walg.TarBall {
	tarBallMaker.number++
	return &FileTarBall{
		number: tarBallMaker.number,
		size:   tarBallMaker.size,
		out:    tarBallMaker.Out,
	}
}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number int
	size   int64
}

// Make creates a new NOPTarBall.
func (tarBallMaker *NOPTarBallMaker) Make(inheritState bool) walg.TarBall {
	tarBallMaker.number++
	return &NOPTarBall{
		number: tarBallMaker.number,
		size:   tarBallMaker.size,
	}
}

type BufferTarBallMaker struct {
	number        int
	size          int64
	BufferToWrite *bytes.Buffer
}

func (tarBallMaker *BufferTarBallMaker) Make(dedicatedUploader bool) walg.TarBall {
	tarBallMaker.number++
	return &BufferTarBall{
		number:     tarBallMaker.number,
		size:       tarBallMaker.size,
		underlying: tarBallMaker.BufferToWrite,
	}
}
