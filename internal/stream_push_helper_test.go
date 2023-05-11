package internal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	functests "github.com/wal-g/wal-g/internal/testutils"

	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	RandomMult = 4463
	RandomTerm = 6361
	RandomBase = 1223
	RandomMod  = 9973
)

func getByteSampleArray(size int) []byte {
	out := make([]byte, size)
	val := RandomBase
	for i := 0; i < size; i++ {
		out[i] = byte(val)
		val = (val*RandomMult + RandomTerm) % RandomMod
	}
	return out
}

type TestWriter struct {
	Result      []byte
	CloseNotify chan struct{}
	mtx         sync.Mutex
}

func newTestWriter() *TestWriter {
	return &TestWriter{
		Result:      make([]byte, 0),
		CloseNotify: make(chan struct{}, 5),
	}
}

func (t *TestWriter) Write(p []byte) (n int, err error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.Result = append(t.Result, p...)
	tracelog.DebugLogger.Printf("Add %d length and result lenth is %d\n", len(p), len(t.Result))
	return len(p), nil
}

func (t *TestWriter) Close() error {
	tracelog.DebugLogger.Println("Close Test writer")
	t.CloseNotify <- struct{}{}
	return nil
}

func GetFolder(networkErrorAfterByteSize int) (storage.Folder, func() error, error) {
	cwd, err := filepath.Abs("./")
	if err != nil {
		return nil, nil, err
	}
	// Create temp directory.
	tmpDir, err := os.MkdirTemp(cwd, "data")
	if err != nil {
		return nil, nil, err
	}
	err = os.Chmod(tmpDir, 0755)
	if err != nil {
		return nil, nil, err
	}

	folder, err := fs.ConfigureFolder(tmpDir, nil)
	if err != nil {
		return nil, nil, err
	}
	if networkErrorAfterByteSize != 0 {
		return functests.NewNetworkErrorFolder(folder, networkErrorAfterByteSize),
			func() error {
				return os.RemoveAll(tmpDir)
			}, nil
	} else {
		return folder, func() error {
			return os.RemoveAll(tmpDir)
		}, nil
	}
}

func checkPushAndFetchBackup(t *testing.T, partitions, blockSize, maxFileSize, networkErrorAfterByteSize, retryAttempts, sampleSize int) {
	storageFolder, clear, err := GetFolder(networkErrorAfterByteSize)
	defer clear()
	compressor := compression.Compressors[compression.CompressingAlgorithms[0]]

	uploader := &SplitStreamUploader{
		Uploader:    NewRegularUploader(compressor, storageFolder),
		partitions:  partitions,
		blockSize:   blockSize,
		maxFileSize: maxFileSize,
	}

	sample := getByteSampleArray(sampleSize)
	backupName, err := uploader.PushStream(bytes.NewReader(sample))
	if err != nil {
		return
	}

	writer := newTestWriter()
	backup := Backup{
		Name:   backupName,
		Folder: storageFolder,
	}

	err = DownloadAndDecompressSplittedStream(backup, blockSize, compression.Decompressors[0].FileExtension(), writer, retryAttempts)
	assert.NoError(t, err)
	<-writer.CloseNotify

	result := writer.Result
	assert.Equal(t, sampleSize, len(result))

	for i, val := range result {
		assert.Equal(t, sample[i], val)
	}
}

func TestSplitBackup_WithCommonValues(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 3, 5, 0, 0, 51)
}

func TestSplitBackup_Synchronous(t *testing.T) {
	checkPushAndFetchBackup(t, 1, 3, 5, 0, 0, 51)
}

func TestSplitBackup_MaxSize_Equal_BlockSize(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 7919, 7919, 0, 0, 1000*1000)
}

func TestSplitBackup_MaxFileSize_GreaterThan_SampleSize(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 53, 10*1000, 0, 0, 1000)
}

func TestSplitBackup_BlockSize_Equal_MaxFileSize_Equal_SampleSize(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 1009, 0, 0, 1009)
}

func TestSplitBackup_BlockSize_Equal_MaxFileSize_Equal_SampleSize_Synchronous(t *testing.T) {
	checkPushAndFetchBackup(t, 1, 1009, 1009, 0, 0, 1009)
}

func TestBackup_WithCommonValues(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 0, 0, 0, 1000*1000)
}

func TestBackup_BlockSize_Equal_SampleSize(t *testing.T) {
	t.Skip("Broken")
	checkPushAndFetchBackup(t, 3, 1009, 0, 0, 0, 1009)
}

func TestBackup_Retry_NetworkError(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 0, 100*1000, 5, 1000*1000)
}

func TestSplitBackup_Retry_NetworkError(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 100*1000, 30*1000, 5, 1000*1000)
}
