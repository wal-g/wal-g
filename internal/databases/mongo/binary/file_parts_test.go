package binary

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestFilePartsTarInterpreterRestoresBytes(t *testing.T) {
	for _, size := range []int{97 * 32, 97*32 + 13} {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("size=%d/parallel=%t", size, parallel), func(t *testing.T) {
				const name = "/db/collection-1.wt"
				const partSize = 97
				data := make([]byte, size)
				for i := range data {
					data[i] = byte((i*31 + i/partSize) % 251)
				}
				directory := t.TempDir()
				interpreter := NewFilePartsTarInterpreter(directory, &models.FilePartsInfo{
					PartSize: partSize, Files: map[string]int64{name: int64(size)},
				})

				var archives [][]byte
				for offset := 0; offset < size; offset += partSize {
					end := min(offset+partSize, size)
					header := filePartsTestHeader(name, int64(size), int64(offset), int64(end-offset))
					archives = append(archives, filePartsTestArchive(t, header, data[offset:end]))
				}

				restore := func(archive []byte) error {
					reader := tar.NewReader(bytes.NewReader(archive))
					header, err := reader.Next()
					if err != nil {
						return err
					}
					return interpreter.Interpret(reader, header)
				}
				if parallel {
					start := make(chan struct{})
					errors := make(chan error, len(archives))
					var workers sync.WaitGroup
					for _, archive := range archives {
						workers.Add(1)
						go func(archive []byte) {
							defer workers.Done()
							<-start
							errors <- restore(archive)
						}(archive)
					}
					close(start)
					workers.Wait()
					close(errors)
					for err := range errors {
						require.NoError(t, err)
					}
				} else {
					for i := len(archives) - 1; i >= 0; i-- {
						require.NoError(t, restore(archives[i]))
					}
				}

				require.NoError(t, interpreter.Validate())
				restored, err := os.ReadFile(filepath.Join(directory, "db", "collection-1.wt"))
				require.NoError(t, err)
				require.Equal(t, data, restored)
			})
		}
	}
}

func TestFilePartsTarInterpreterExtractAllRetriesOnlyFailedObject(t *testing.T) {
	previousConcurrency := viper.Get(conf.DownloadConcurrencySetting)
	previousRetries := viper.Get(conf.DownloadFileRetriesSetting)
	viper.Set(conf.DownloadConcurrencySetting, 3)
	viper.Set(conf.DownloadFileRetriesSetting, 1)
	t.Cleanup(func() {
		viper.Set(conf.DownloadConcurrencySetting, previousConcurrency)
		viper.Set(conf.DownloadFileRetriesSetting, previousRetries)
	})
	const name = "/db/collection-1.wt"
	const data = "ABCDEFGHIJK"
	directory := t.TempDir()
	interpreter := NewFilePartsTarInterpreter(directory, &models.FilePartsInfo{
		PartSize: 4, Files: map[string]int64{name: int64(len(data))},
	})
	makers := make([]internal.ReaderMaker, 0, 3)
	var objects []*filePartsReaderMaker
	for offset := 0; offset < len(data); offset += 4 {
		end := min(offset+4, len(data))
		archive := filePartsTestArchive(t, filePartsTestHeader(name, int64(len(data)), int64(offset), int64(end-offset)),
			[]byte(data[offset:end]))
		object := &filePartsReaderMaker{key: fmt.Sprintf("part_%d.tar", offset), archive: archive}
		if offset == 4 {
			rawReader := bytes.NewReader(archive)
			_, err := tar.NewReader(rawReader).Next()
			require.NoError(t, err)
			object.failAfter = len(archive) - rawReader.Len() + 2
		}
		objects = append(objects, object)
		makers = append(makers, object)
	}

	require.NoError(t, internal.ExtractAllWithSleeper(t.Context(), interpreter, makers, filePartsNoSleep{}))
	require.NoError(t, interpreter.Validate())
	require.Equal(t, int32(1), objects[0].reads.Load())
	require.Equal(t, int32(2), objects[1].reads.Load())
	require.Equal(t, int32(1), objects[2].reads.Load())
	restored, err := os.ReadFile(filepath.Join(directory, "db", "collection-1.wt"))
	require.NoError(t, err)
	require.Equal(t, data, string(restored))
}

func TestFilePartsTarInterpreterRetryPreservesOtherParts(t *testing.T) {
	for _, failure := range []error{io.EOF, errors.New("interrupted download")} {
		t.Run(failure.Error(), func(t *testing.T) {
			const name = "/db/index-1.wt"
			const data = "ABCDEFGHIJK"
			directory := t.TempDir()
			interpreter := NewFilePartsTarInterpreter(directory, &models.FilePartsInfo{
				PartSize: 4, Files: map[string]int64{name: int64(len(data))},
			})
			first := filePartsTestHeader(name, int64(len(data)), 0, 4)
			middle := filePartsTestHeader(name, int64(len(data)), 4, 4)
			last := filePartsTestHeader(name, int64(len(data)), 8, 3)
			require.NoError(t, interpreter.Interpret(bytes.NewBufferString(data[:4]), first))
			require.NoError(t, interpreter.Interpret(bytes.NewBufferString(data[8:]), last))

			reader := io.MultiReader(bytes.NewBufferString(data[4:6]), filePartsErrorReader{err: failure})
			require.Error(t, interpreter.Interpret(reader, middle))
			require.Error(t, interpreter.Validate())
			partiallyRestored, err := os.ReadFile(filepath.Join(directory, "db", "index-1.wt"))
			require.NoError(t, err)
			require.Len(t, partiallyRestored, len(data))
			require.Equal(t, data[:4], string(partiallyRestored[:4]))
			require.Equal(t, data[8:], string(partiallyRestored[8:]))

			require.NoError(t, interpreter.Interpret(bytes.NewBufferString(data[4:8]), middle))
			require.NoError(t, interpreter.Validate())
			restored, err := os.ReadFile(filepath.Join(directory, "db", "index-1.wt"))
			require.NoError(t, err)
			require.Equal(t, data, string(restored))
		})
	}
}

func TestFilePartsTarInterpreterDetectsMissingData(t *testing.T) {
	for _, missing := range []string{"first part", "middle part", "last part", "entire file"} {
		t.Run(missing, func(t *testing.T) {
			const name = "/collection-1.wt"
			interpreter := NewFilePartsTarInterpreter(t.TempDir(), &models.FilePartsInfo{
				PartSize: 4, Files: map[string]int64{name: 11},
			})
			for i, part := range []string{"ABCD", "EFGH", "IJK"} {
				if missing == "entire file" || missing == []string{"first part", "middle part", "last part"}[i] {
					continue
				}
				for attempt := 0; attempt < 2; attempt++ {
					require.NoError(t, interpreter.Interpret(bytes.NewBufferString(part),
						filePartsTestHeader(name, 11, int64(i*4), int64(len(part)))))
				}
			}
			require.Error(t, interpreter.Validate())
		})
	}
}

func TestFilePartsTarInterpreterRejectsInvalidParts(t *testing.T) {
	const name = "/collection-1.wt"
	for _, test := range []struct {
		name   string
		modify func(*tar.Header)
	}{
		{name: "missing offset", modify: func(header *tar.Header) { delete(header.PAXRecords, filePartOffsetPAX) }},
		{name: "missing file size", modify: func(header *tar.Header) { delete(header.PAXRecords, fileSizePAX) }},
		{name: "empty offset", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "" }},
		{name: "empty file size", modify: func(header *tar.Header) { header.PAXRecords[fileSizePAX] = "" }},
		{name: "invalid offset", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "no offset" }},
		{name: "negative offset", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "-4" }},
		{name: "overflowing offset", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "9223372036854775808" }},
		{name: "invalid file size", modify: func(header *tar.Header) { header.PAXRecords[fileSizePAX] = "no size" }},
		{name: "negative file size", modify: func(header *tar.Header) { header.PAXRecords[fileSizePAX] = "-11" }},
		{name: "zero file size", modify: func(header *tar.Header) { header.PAXRecords[fileSizePAX] = "0" }},
		{name: "overflowing file size", modify: func(header *tar.Header) { header.PAXRecords[fileSizePAX] = "9223372036854775808" }},
		{name: "conflicting file size", modify: func(header *tar.Header) { header.PAXRecords[fileSizePAX] = "12" }},
		{name: "misaligned offset", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "1" }},
		{name: "offset after end", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "12" }},
		{name: "negative part size", modify: func(header *tar.Header) { header.Size = -1 }},
		{name: "empty part", modify: func(header *tar.Header) { header.Size = 0 }},
		{name: "short nonfinal part", modify: func(header *tar.Header) { header.Size = 3 }},
		{name: "oversized part", modify: func(header *tar.Header) { header.Size = 5 }},
		{name: "part beyond end", modify: func(header *tar.Header) { header.PAXRecords[filePartOffsetPAX] = "8" }},
		{name: "directory with part metadata", modify: func(header *tar.Header) { header.Typeflag = tar.TypeDir }},
	} {
		t.Run(test.name, func(t *testing.T) {
			interpreter := NewFilePartsTarInterpreter(t.TempDir(), &models.FilePartsInfo{
				PartSize: 4, Files: map[string]int64{name: 11},
			})
			header := filePartsTestHeader(name, 11, 0, 4)
			test.modify(header)
			require.Error(t, interpreter.Interpret(bytes.NewBufferString("ABCD"), header))
			require.Error(t, interpreter.Validate())
		})
	}
}

func TestFilePartsTarInterpreterRequiresManifest(t *testing.T) {
	for _, expected := range []*models.FilePartsInfo{
		nil,
		{PartSize: 4, Files: map[string]int64{"/index-1.wt": 11}},
	} {
		interpreter := NewFilePartsTarInterpreter(t.TempDir(), expected)
		require.Error(t, interpreter.Interpret(bytes.NewBufferString("ABCD"),
			filePartsTestHeader("/collection-1.wt", 11, 0, 4)))
	}
}

func TestFilePartsTarInterpreterRejectsUnsplitFileInManifest(t *testing.T) {
	const name = "/collection-1.wt"
	interpreter := NewFilePartsTarInterpreter(t.TempDir(), &models.FilePartsInfo{
		PartSize: 4, Files: map[string]int64{name: 11},
	})
	header := &tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0600, Size: 11}
	require.Error(t, interpreter.Interpret(bytes.NewBufferString("ABCDEFGHIJK"), header))
	require.Error(t, interpreter.Validate())
}

func TestFilePartsTarInterpreterRejectsInvalidManifest(t *testing.T) {
	const name = "/collection-1.wt"
	for _, test := range []struct {
		name     string
		partSize int64
		fileSize int64
	}{
		{name: "zero part size", fileSize: 11},
		{name: "negative part size", partSize: -4, fileSize: 11},
		{name: "zero file size", partSize: 4},
		{name: "negative file size", partSize: 4, fileSize: -11},
	} {
		t.Run(test.name, func(t *testing.T) {
			interpreter := NewFilePartsTarInterpreter(t.TempDir(), &models.FilePartsInfo{
				PartSize: test.partSize, Files: map[string]int64{name: test.fileSize},
			})
			require.Error(t, interpreter.Interpret(bytes.NewBufferString("ABCD"), filePartsTestHeader(name, 11, 0, 4)))
			require.Error(t, interpreter.Validate())
		})
	}
}

func TestFilePartsTarInterpreterLegacyFiles(t *testing.T) {
	for _, typeflag := range []byte{tar.TypeReg, tar.TypeRegA} {
		t.Run(fmt.Sprintf("type=%d", typeflag), func(t *testing.T) {
			directory := t.TempDir()
			interpreter := NewFilePartsTarInterpreter(directory, nil)
			const data = "legacy WiredTiger file"
			header := &tar.Header{
				Name: "/db/collection-1.wt", Typeflag: typeflag, Mode: 0600, Size: int64(len(data)),
			}
			require.NoError(t, interpreter.Interpret(bytes.NewBufferString(data), header))
			require.NoError(t, interpreter.Validate())
			restored, err := os.ReadFile(filepath.Join(directory, "db", "collection-1.wt"))
			require.NoError(t, err)
			require.Equal(t, data, string(restored))
		})
	}
}

func filePartsTestHeader(name string, size, offset, partSize int64) *tar.Header {
	return &tar.Header{
		Name: name, Typeflag: tar.TypeReg, Format: tar.FormatPAX, Mode: 0600, Size: partSize,
		PAXRecords: map[string]string{
			filePartOffsetPAX: strconv.FormatInt(offset, 10),
			fileSizePAX:       strconv.FormatInt(size, 10),
		},
	}
}

func filePartsTestArchive(t *testing.T, header *tar.Header, data []byte) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	require.NoError(t, writer.WriteHeader(header))
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return archive.Bytes()
}

type filePartsReaderMaker struct {
	key       string
	archive   []byte
	failAfter int
	reads     atomic.Int32
}

func (object *filePartsReaderMaker) Reader(context.Context) (io.ReadCloser, error) {
	if object.reads.Add(1) == 1 && object.failAfter > 0 {
		return io.NopCloser(io.MultiReader(bytes.NewReader(object.archive[:object.failAfter]),
			filePartsErrorReader{err: errors.New("interrupted object download")})), nil
	}
	return io.NopCloser(bytes.NewReader(object.archive)), nil
}

func (object *filePartsReaderMaker) StoragePath() string  { return object.key }
func (object *filePartsReaderMaker) LocalPath() string    { return object.key }
func (*filePartsReaderMaker) FileType() internal.FileType { return internal.TarFileType }
func (*filePartsReaderMaker) Mode() int64                 { return 0600 }

type filePartsNoSleep struct{}

func (filePartsNoSleep) Sleep() {}

type filePartsErrorReader struct {
	err error
}

func (reader filePartsErrorReader) Read([]byte) (int, error) {
	return 0, reader.err
}
