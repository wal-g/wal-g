package testtools

import (
	"bytes"
	"github.com/wal-g/wal-g/internal"
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
func (tarBallMaker *FileTarBallMaker) Make(inheritState bool) internal.TarBall {
	tarBallMaker.number++
	return &FileTarBall{
		name:   fmt.Sprintf("file_%d", tarBallMaker.number),
		number: tarBallMaker.number,
		size:   tarBallMaker.size,
		out:    tarBallMaker.Out,
	}
}

type BufferTarBallMaker struct {
	number        int
	size          int64
	BufferToWrite *bytes.Buffer
}

func (tarBallMaker *BufferTarBallMaker) Make(dedicatedUploader bool) internal.TarBall {
	tarBallMaker.number++
	return &BufferTarBall{
		name:       fmt.Sprintf("buffer_%d", tarBallMaker.number),
		number:     tarBallMaker.number,
		size:       tarBallMaker.size,
		underlying: tarBallMaker.BufferToWrite,
	}
}
