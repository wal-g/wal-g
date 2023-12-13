package internal_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

const (
	PrivateKeyFilePath = "../test/testdata/waleGpgKey"
	randomBytesAmount  = 1024
	seed               = 4
	minBufferSize      = 1024
)

func TestExtractAll_noFilesProvided(t *testing.T) {
	buf := &testtools.NOPTarInterpreter{}
	err := internal.ExtractAllWithSleeper(buf, []internal.ReaderMaker{}, NOPSleeper{})
	assert.IsType(t, err, internal.NoFilesToExtractError{})
}

func TestExtractAll_fileDoesntExist(t *testing.T) {
	readerMaker := &testtools.FileReaderMaker{Key: "testdata/booba.tar"}
	err := internal.ExtractAllWithSleeper(&testtools.NOPTarInterpreter{}, []internal.ReaderMaker{readerMaker}, NOPSleeper{})
	assert.Error(t, err)
}

func generateRandomBytes() []byte {
	sb := testtools.NewStrideByteReader(seed)
	lr := &io.LimitedReader{
		R: sb,
		N: int64(randomBytesAmount),
	}
	b, _ := io.ReadAll(lr)
	return b
}

func makeTar(name string) (BufferReaderMaker, []byte) {
	b := generateRandomBytes()
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	r, w := io.Pipe()
	go func() {
		bw := bufio.NewWriterSize(w, minBufferSize)

		defer utility.LoggedClose(w, "")
		defer func() {
			if err := bw.Flush(); err != nil {
				panic(err)
			}
		}()

		testtools.CreateNamedTar(bw, &io.LimitedReader{
			R: bytes.NewBuffer(b),
			N: int64(len(b)),
		}, name)

	}()
	tarContents := &bytes.Buffer{}
	io.Copy(tarContents, r)

	return BufferReaderMaker{tarContents, "/usr/local.tar"}, bCopy
}

// Returns byte array and encrypted, compressed and corrupted tar
func makeCorruptedTar(name string) (BufferReaderMaker, []byte) {
	b := generateRandomBytes()
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	r, w := io.Pipe()
	go func() {
		bw := bufio.NewWriterSize(w, minBufferSize)

		defer utility.LoggedClose(w, "")
		defer func() {
			if err := bw.Flush(); err != nil {
				panic(err)
			}
		}()

		crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, noPassphrase)

		compressor := GetLz4Compressor()
		compressed := internal.CompressAndEncrypt(bytes.NewReader(b), compressor, crypter)

		temp, err := io.ReadAll(compressed)
		if err != nil {
			panic(err)
		}

		testtools.CreateNamedTar(bw, &io.LimitedReader{
			R: bytes.NewBuffer(temp[0 : len(temp)-1]),
			N: int64(len(temp) - 1),
		}, name)

	}()
	tarContents := &bytes.Buffer{}
	io.Copy(tarContents, r)

	return BufferReaderMaker{tarContents, "/usr/local2.tar.lz4"}, bCopy
}

func TestExtractAll_simpleTar(t *testing.T) {
	os.Setenv(internal.DownloadConcurrencySetting, "1")
	defer os.Unsetenv(internal.DownloadConcurrencySetting)

	brm, b := makeTar("booba")

	buf := &testtools.BufferTarInterpreter{}
	files := []internal.ReaderMaker{&brm}

	err := internal.ExtractAllWithSleeper(buf, files, NOPSleeper{})
	if err != nil {
		t.Log(err)
	}

	assert.Equalf(t, b, buf.Out, "ExtractAll: Output does not match input.")
}

func TestDecryptAndDecompressTar_encrypted2(t *testing.T) {
	os.Setenv(internal.DownloadConcurrencySetting, "1")
	os.Setenv(internal.DownloadFileRetriesSetting, "7")
	defer os.Unsetenv(internal.DownloadConcurrencySetting)

	brm, _ := makeCorruptedTar("corrupted")

	buf := &testtools.BufferTarInterpreter{}
	files := []internal.ReaderMaker{&brm}

	start := time.Now()

	err := internal.ExtractAllWithSleeper(buf, files, Sleeper05{})

	duration := time.Since(start)

	// file corrupted so we expect error
	assert.Error(t, err)
	// function must retry 7 times so overall time should be +- 3.5 seconds
	assert.Greater(t, duration, time.Second*3)
	assert.Less(t, duration, time.Second*4)
}

func TestExtractAll_multipleTars(t *testing.T) {
	internal.GetMaxDownloadConcurrency()
	os.Setenv(internal.DownloadConcurrencySetting, "1")
	defer os.Unsetenv(internal.DownloadConcurrencySetting)

	fileAmount := 3
	bufs := [][]byte{}
	brms := []internal.ReaderMaker{}

	for i := 0; i < fileAmount; i++ {
		brm, b := makeTar(strconv.Itoa(i))
		bufs = append(bufs, b)
		brms = append(brms, &brm)
	}

	buf := testtools.NewConcurrentConcatBufferTarInterpreter()

	err := internal.ExtractAllWithSleeper(buf, brms, NOPSleeper{})
	if err != nil {
		t.Log(err)
	}

	for i := 0; i < fileAmount; i++ {
		assert.Equal(t, bufs[i], buf.Out[strconv.Itoa(i)], "Some of outputs do not match input")
	}
}

func TestExtractAll_multipleConcurrentTars(t *testing.T) {
	os.Setenv(internal.DownloadConcurrencySetting, "4")
	defer os.Unsetenv(internal.DownloadConcurrencySetting)

	fileAmount := 24
	bufs := [][]byte{}
	brms := []internal.ReaderMaker{}

	for i := 0; i < fileAmount; i++ {
		brm, b := makeTar(strconv.Itoa(i))
		bufs = append(bufs, b)
		brms = append(brms, &brm)
	}

	buf := testtools.NewConcurrentConcatBufferTarInterpreter()

	err := internal.ExtractAllWithSleeper(buf, brms, NOPSleeper{})
	if err != nil {
		t.Log(err)
	}

	for i := 0; i < fileAmount; i++ {
		assert.Equal(t, bufs[i], buf.Out[strconv.Itoa(i)], "Some of outputs do not match input")
	}
}

func noPassphrase() (string, bool) {
	return "", false
}

func TestDecryptAndDecompressTar_unencrypted(t *testing.T) {
	b := generateRandomBytes()
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(bytes.NewReader(b), compressor, nil)

	compressedBuffer := &bytes.Buffer{}
	_, _ = compressedBuffer.ReadFrom(compressed)

	reader, err := internal.DecryptAndDecompressTar(compressedBuffer, "/usr/local/test.tar.lz4", nil)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	decompressed, readErr := io.ReadAll(reader)
	if readErr != nil {
		t.Logf("%+v\n", readErr)
	}

	assert.Equalf(t, bCopy, decompressed, "decompressed tar does not match the input")
}

func TestDecryptAndDecompressTar_encrypted(t *testing.T) {
	b := generateRandomBytes()

	// Copy generated bytes to another slice to make the test more robust against modifications of "b".
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, noPassphrase)

	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(bytes.NewReader(b), compressor, crypter)

	reader, err := internal.DecryptAndDecompressTar(compressed, "/usr/local/test.tar.lz4", crypter)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	decompressed, readErr := io.ReadAll(reader)
	if readErr != nil {
		t.Logf("%+v\n", readErr)
	}

	assert.Equalf(t, bCopy, decompressed, "decompressed tar does not match the input")
}

func TestDecryptAndDecompressTar_noCrypter(t *testing.T) {
	b := generateRandomBytes()

	// Copy generated bytes to another slice to make the test more robust against modifications of "b".
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, noPassphrase)

	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(bytes.NewReader(b), compressor, crypter)

	reader, err := internal.DecryptAndDecompressTar(compressed, "/usr/local/test.tar.lz4", nil)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	_, readErr := io.ReadAll(reader)
	if readErr != nil {
		t.Logf("%+v\n", readErr)
	}

	assert.Error(t, readErr)
}

func TestDecryptAndDecompressTar_wrongCrypter(t *testing.T) {
	b := generateRandomBytes()

	// Copy generated bytes to another slice to make the test more robust against modifications of "b".
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, noPassphrase)

	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(bytes.NewReader(b), compressor, crypter)

	_, err := internal.DecryptAndDecompressTar(compressed, "/usr/local/test.tar.lzma", crypter)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	assert.Error(t, err)
}

func TestDecryptAndDecompressTar_unknownFormat(t *testing.T) {
	b := generateRandomBytes()

	// Copy generated bytes to another slice to make the test more robust against modifications of "b".
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	_, err := internal.DecryptAndDecompressTar(bytes.NewBuffer(b), "/usr/local/test.some_unsupported_file_format", nil)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	assert.Error(t, err)
	assert.IsType(t, internal.UnsupportedFileTypeError{}, err)
}

func TestDecryptAndDecompressTar_uncompressed(t *testing.T) {
	b := generateRandomBytes()
	bCopy := make([]byte, len(b))
	copy(bCopy, b)

	compressed := internal.CompressAndEncrypt(bytes.NewReader(b), nil, nil)

	compressedBuffer := &bytes.Buffer{}
	_, _ = compressedBuffer.ReadFrom(compressed)

	reader, err := internal.DecryptAndDecompressTar(compressedBuffer, "/usr/local/test.tar", nil)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	decompressed, readErr := io.ReadAll(reader)
	if readErr != nil {
		t.Logf("%+v\n", err)
	}

	assert.Equalf(t, bCopy, decompressed, "decompressed tar does not match the input")
}

// Used to mock files in memory.
type BufferReaderMaker struct {
	Buf *bytes.Buffer
	Key string
}

func (b *BufferReaderMaker) Reader() (io.ReadCloser, error) { return io.NopCloser(b.Buf), nil }
func (b *BufferReaderMaker) StoragePath() string            { return b.Key }
func (b *BufferReaderMaker) LocalPath() string              { return b.Key }
func (b *BufferReaderMaker) FileType() internal.FileType    { return internal.TarFileType }
func (b *BufferReaderMaker) Mode() int64                    { return 0 }

type NOPSleeper struct{}

func (s NOPSleeper) Sleep() {}

// sleeps for 0.5 econds
type Sleeper05 struct{}

func (s Sleeper05) Sleep() {
	time.Sleep(time.Millisecond * 500)
}
