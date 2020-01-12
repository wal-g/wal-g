package internal

import (
	"archive/tar"
	"io/ioutil"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/crypto"
)

// NOPTarBall mocks a tarball. Used for prefault logic.
type NOPTarBall struct {
	number          int
	allTarballsSize *int64
	tarWriter       *tar.Writer
}

func (tarBall *NOPTarBall) SetUp(crypter crypto.Crypter, params ...string) {}
func (tarBall *NOPTarBall) CloseTar() error                                { return nil }

func (tarBall *NOPTarBall) Size() int64            { return atomic.LoadInt64(tarBall.allTarballsSize) }
func (tarBall *NOPTarBall) AddSize(i int64)        { atomic.AddInt64(tarBall.allTarballsSize, i) }
func (tarBall *NOPTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *NOPTarBall) AwaitUploads()          {}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number          int
	allTarballsSize *int64
}

// Make creates a new NOPTarBall.
func (tarBallMaker *NOPTarBallMaker) Make(inheritState bool) TarBall {
	tarBallMaker.number++
	return &NOPTarBall{
		number:          tarBallMaker.number,
		allTarballsSize: tarBallMaker.allTarballsSize,
		tarWriter:       tar.NewWriter(ioutil.Discard),
	}
}

func newNopTarBallMaker() TarBallMaker {
	size := int64(0)
	return &NOPTarBallMaker{0, &size}
}
