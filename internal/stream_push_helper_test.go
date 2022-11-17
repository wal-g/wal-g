package internal

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func getByteSampleReader(size int) io.Reader {
	out := make([]byte, size)
	for i := 0; i < size; i++ {
		out[i] = byte(i)
	}
	return bytes.NewReader(out)
}

func setupTmpDir(t *testing.T) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	// Create temp directory.
	tmpDir, err := os.MkdirTemp(cwd, "data")
	if err != nil {
		t.Log(err)
	}
	err = os.Chmod(tmpDir, 0755)
	if err != nil {
		t.Log(err)
	}
	return tmpDir
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
	//tracelog.DebugLogger.Printf("Add %d length and result lenth is %d\n", len(p), len(t.Result))
	return len(p), nil
}

func (t *TestWriter) Close() error {
	tracelog.DebugLogger.Println("Close Test writer")
	t.CloseNotify <- struct{}{}
	return nil
}

func checkPushAndFetchBackup(t *testing.T, partitions, blockSize, maxFileSize, sampleSize int) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	storageFolder, err := fs.ConfigureFolder(tmpDir, nil)
	if err != nil {
		t.Log(err)
	}
	compressor := compression.Compressors[compression.CompressingAlgorithms[0]]

	uploader := &SplitStreamUploader{
		Uploader:    NewUploader(compressor, storageFolder),
		partitions:  partitions,
		blockSize:   blockSize,
		maxFileSize: maxFileSize,
	}

	sample := getByteSampleReader(sampleSize)
	backupName, err := uploader.PushStream(sample)
	if err != nil {
		return
	}

	writer := newTestWriter()
	backup := Backup{
		Name:   backupName,
		Folder: storageFolder,
	}

	err = DownloadAndDecompressSplittedStream(backup, blockSize, compression.Decompressors[0].FileExtension(), writer, maxFileSize != 0)
	<-writer.CloseNotify

	result := writer.Result
	assert.Equal(t, sampleSize, len(result))

	for i, val := range result {
		assert.Equal(t, byte(i), val)
	}
}

func TestSplitBackup_WithCommonValues(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 7919, 1000*1000)
}

func TestSplitBackup_Synchronous(t *testing.T) {
	checkPushAndFetchBackup(t, 1, 1009, 7919, 1000*1000)
}

func TestSplitBackup_MaxSize_Equal_BlockSize(t *testing.T) {
	//t.Skip("Broken")
	checkPushAndFetchBackup(t, 3, 7919, 7919, 1000*1000)
}

func TestSplitBackup_MaxFileSize_GreaterThan_SampleSize(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 1000*1000*1000, 1000*1000)
}

func TestSplitBackup_BlockSize_Equal_MaxFileSize_Equal_SampleSize(t *testing.T) {
	t.Skip("Broken")
	checkPushAndFetchBackup(t, 3, 1009, 1009, 1009)
}

func TestSplitBackup_BlockSize_Equal_MaxFileSize_Equal_SampleSize_Synchronous(t *testing.T) {
	t.Skip("Broken")
	checkPushAndFetchBackup(t, 1, 1009, 1009, 1009)
}

func TestBackup_WithCommonValues(t *testing.T) {
	checkPushAndFetchBackup(t, 3, 1009, 0, 1000*1000)
}

func TestBackup_BlockSize_Equal_SampleSize(t *testing.T) {
	t.Skip("Broken")
	checkPushAndFetchBackup(t, 3, 1009, 0, 1009)
}

func TestBackup_WithCommonValues1(t *testing.T) {
	t.Skip("Broken")
	checkPushAndFetchBackup(t, 5, 3, 0, 10)
}
