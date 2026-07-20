package fsutil_test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/ncw/directio"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/fsutil"
)

func TestMain(m *testing.M) {
	// In production config.Configure() registers its default (32).
	viper.Set(conf.DirectIOBlockCountSetting, 32)
	os.Exit(m.Run())
}

func Test_NewDirectIOReadSeekCloser(t *testing.T) {
	for _, testCaseSize := range []int64{
		0,
		8*1024*1024 - 1,
		directio.BlockSize,
		32 * directio.BlockSize,
		32*directio.BlockSize + 1,
	} {
		t.Run(fmt.Sprintf("run with file size: %d (seek 0)", testCaseSize), func(t *testing.T) {
			directNewDirectIOReadSeekCloser(t, testCaseSize, 0)
		})
		for _, testCaseSeek := range []int64{0, 1, 8*1024 - 1, 8 * 1024 * 1024} {
			if testCaseSeek > testCaseSize {
				continue
			}
			t.Run(fmt.Sprintf("run with file size: %d (seek %d)", testCaseSize, testCaseSeek), func(t *testing.T) {
				directNewDirectIOReadSeekCloser(t, testCaseSize, testCaseSeek)
			})
		}
	}
}

func directNewDirectIOReadSeekCloser(t *testing.T, fileSize int64, seek int64) {
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

	// os.Open.
	ioFD, errIOFD := os.Open(fd.Name())
	require.NoError(t, errIOFD)
	defer ioFD.Close()
	seekFDN, errIOSeek := ioFD.Seek(seek, io.SeekStart)
	assert.Equal(t, seek, seekFDN)
	if fileSize == 0 && seek == 1 {
		t.Log(seekFDN)
		t.Log(errIOSeek)
	}

	// directIO.
	directIOReadSeekCloser, errDirectIOFD := fsutil.NewDirectIOReadSeekCloser(fd.Name(), syscall.O_RDONLY, 0)
	require.NoError(t, errDirectIOFD)
	defer directIOReadSeekCloser.Close()
	seekDirectION, errIOSeekDirectIO := directIOReadSeekCloser.Seek(seek, io.SeekStart)
	{
		assert.Equal(t, errIOSeek, errIOSeekDirectIO)
		assert.Equal(t, seekFDN, seekDirectION)
	}

	// check sha.
	assert.Equal(t, getSHA256(t, ioFD), getSHA256(t, directIOReadSeekCloser))
}

func getSHA256(t *testing.T, r io.ReadCloser) string {
	h := sha256.New()
	_, err := io.Copy(h, r)
	require.NoError(t, err)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func Test_NewDirectIOReadSeekCloser_BlockCountOptions(t *testing.T) {
	t.Cleanup(func() { viper.Set(conf.DirectIOBlockCountSetting, 32) })

	// Iterate over common DirectIOBlockCountSetting options
	for _, blockCount := range []int{1, 8, 32, 1024} {
		viper.Set(conf.DirectIOBlockCountSetting, blockCount)

		// Run 3 file size cases for each IOBlock size
		t.Run(fmt.Sprintf("blockCount=%d/empty", blockCount), func(t *testing.T) {
			directNewDirectIOReadSeekCloser(t, 0, 0)
		})
		t.Run(fmt.Sprintf("blockCount=%d/one_block", blockCount), func(t *testing.T) {
			directNewDirectIOReadSeekCloser(t, directio.BlockSize, 0)
		})
		t.Run(fmt.Sprintf("blockCount=%d/blocks_plus_partial", blockCount), func(t *testing.T) {
			directNewDirectIOReadSeekCloser(t, 32*directio.BlockSize+1, 0)
		})
	}
}

func Test_NewDirectIOReadSeekCloser_InvalidBlockCountError(t *testing.T) {
	t.Cleanup(func() { viper.Set(conf.DirectIOBlockCountSetting, 32) })
	fd, err := os.CreateTemp(os.TempDir(), "directio_invalid")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	defer os.Remove(fd.Name())
	for _, blockCount := range []int{0, -1} {
		viper.Set(conf.DirectIOBlockCountSetting, blockCount)
		_, err := fsutil.NewDirectIOReadSeekCloser(fd.Name(), syscall.O_RDONLY, 0)
		assert.Error(t, err, "block count %d should be rejected", blockCount)
	}
}
