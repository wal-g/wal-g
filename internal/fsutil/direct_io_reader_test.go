package fsutil_test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wal-g/wal-g/internal/fsutil"
)

func Test_NewDirectIOReadSeekCloser(t *testing.T) {
	for _, testCase := range []int64{
		0,
		8*1024*1024 - 1,
		directio.BlockSize,
		32 * directio.BlockSize,
		32*directio.BlockSize + 1,
	} {
		t.Run(fmt.Sprintf("run with file size: %d", testCase), func(t *testing.T) {
			directNewDirectIOReadSeekCloser(t, testCase)
		})
	}
}

func directNewDirectIOReadSeekCloser(t *testing.T, fileSize int64) {
	fd, errFD := os.CreateTemp(os.TempDir(), "directio_read_seek_closer")
	require.NoError(t, errFD)
	defer fd.Close()
	{ // write random
		buf := make([]byte, fileSize)
		size, errRand := rand.Read(buf)
		require.NoError(t, errRand)
		assert.Equal(t, len(buf), size)
		for {
			n, err := fd.Write(buf)
			assert.NoError(t, err)
			if n == len(buf) {
				break
			}
			buf = buf[:size]
		}
	}
	ioFD, errIOFD := os.Open(fd.Name())
	require.NoError(t, errIOFD)
	defer ioFD.Close()
	directIOReadSeekCloser, errDirectIOFD := fsutil.NewDirectIOReadSeekCloserReadOnly(fd.Name())
	require.NoError(t, errDirectIOFD)
	defer directIOReadSeekCloser.Close()
	assert.Equal(t, getSHA256(t, ioFD), getSHA256(t, directIOReadSeekCloser))
}

func getSHA256(t *testing.T, r io.ReadCloser) string {
	h := sha256.New()
	_, err := io.Copy(h, r)
	require.NoError(t, err)
	return fmt.Sprintf("%x", h.Sum(nil))
}
