package internal

import (
	"archive/tar"
	"io"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/crypto"
)

// NOPTarBall mocks a tarball. Used for prefault logic.
type NOPTarBall struct {
	number    int
	partSize  *atomic.Int64
	tarWriter *tar.Writer
}

func (tarBall *NOPTarBall) Name() string {
	return "NOPTarBall"
}

func (tarBall *NOPTarBall) SetUp(crypter crypto.Crypter, params ...string) {}
func (tarBall *NOPTarBall) CloseTar() error                                { return nil }

func (tarBall *NOPTarBall) Size() int64            { return tarBall.partSize.Load() }
func (tarBall *NOPTarBall) AddSize(i int64)        { tarBall.partSize.Add(i) }
func (tarBall *NOPTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *NOPTarBall) AwaitUploads()          {}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number   int
	partSize *atomic.Int64
}

// Make creates a new NOPTarBall.
func (tarBallMaker *NOPTarBallMaker) Make(inheritState bool) TarBall {
	tarBallMaker.number++
	return &NOPTarBall{
		number:    tarBallMaker.number,
		partSize:  tarBallMaker.partSize,
		tarWriter: tar.NewWriter(io.Discard),
	}
}

func NewNopTarBallMaker() TarBallMaker {
	return &NOPTarBallMaker{0, new(atomic.Int64)}
}
