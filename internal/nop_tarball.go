package internal

import (
	"archive/tar"
	"fmt"
	"io/ioutil"

	"github.com/wal-g/wal-g/internal/crypto"
)

// NOPTarBall mocks a tarball. Used for prefault logic.
type NOPTarBall struct {
	name      string
	number    int
	size      int64
	tarWriter *tar.Writer
}

func (tarBall *NOPTarBall) Name() string                                   { return tarBall.name }
func (tarBall *NOPTarBall) SetUp(crypter crypto.Crypter, params ...string) {}
func (tarBall *NOPTarBall) CloseTar() error                                { return nil }

func (tarBall *NOPTarBall) Size() int64            { return tarBall.size }
func (tarBall *NOPTarBall) AddSize(i int64)        { tarBall.size += i }
func (tarBall *NOPTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *NOPTarBall) AwaitUploads()          {}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number int
	size   int64
}

// Make creates a new NOPTarBall.
func (tarBallMaker *NOPTarBallMaker) Make(inheritState bool) TarBall {
	tarBallMaker.number++
	return &NOPTarBall{
		name:      fmt.Sprintf("nop_%d", tarBallMaker.number),
		number:    tarBallMaker.number,
		size:      tarBallMaker.size,
		tarWriter: tar.NewWriter(ioutil.Discard),
	}
}

func NewNopTarBallMaker() TarBallMaker {
	return &NOPTarBallMaker{0, 0}
}
