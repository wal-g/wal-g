package internal

import (
	"bytes"
	"io"
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

func GetS3Folder(networkErrorAfterByteSize int) (storage.Folder, func() error, error) {
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
		return functests.NewS3ErrorFolder(folder, networkErrorAfterByteSize),
			func() error {
				return os.RemoveAll(tmpDir)
			}, nil
	} else {
		return folder, func() error {
			return os.RemoveAll(tmpDir)
		}, nil
	}
}

// We have to repeat testtools code to evade import cycle
type NopCloserWriter struct {
	io.Writer
}

func (NopCloserWriter) Close() error {
	return nil
}

type MockCompressor struct{}

func (compressor *MockCompressor) NewWriter(writer io.Writer) io.WriteCloser {
	return &NopCloserWriter{
		writer,
	}
}

func (compressor *MockCompressor) FileExtension() string {
	return "mock"
}

func checkSplitPush(t *testing.T, partitions, blockSize, maxFileSize, s3errorAfterByteSize, sampleSize int) {
	compressor := &MockCompressor{}
	folder, clearer, err := GetS3Folder(s3errorAfterByteSize)
	defer clearer()
	if err != nil {
		t.Fatal(err)
	}
	splitUploader := &SplitStreamUploader{
		Uploader:    NewRegularUploader(compressor, folder),
		partitions:  partitions,
		blockSize:   blockSize,
		maxFileSize: maxFileSize,
	}
	splitUploader.PushStream(bytes.NewBuffer(getByteSampleArray(sampleSize)))
}

func TestSplitPush_Synchronous_WithoutFiles(t *testing.T) {
	checkSplitPush(t, 1, 3, 0, 1000, 51)
}

func TestSplitPush_Synchronous(t *testing.T) {
	checkSplitPush(t, 1, 3, 5, 1000, 51)
}

func TestSplitPush_Synchronous_2(t *testing.T) {
	checkSplitPush(t, 1, 50, 100, 1000, 24)
}

func TestSplitPush_WithoutErrors(t *testing.T) {
	checkSplitPush(t, 2, 3, 5, 15000, 500)
}

func TestSplitPush_WithoutErrors_2(t *testing.T) {
	checkSplitPush(t, 10, 3, 5, 15000, 500)
}

func TestSplitPush_WithCommonValues(t *testing.T) {
	checkSplitPush(t, 3, 3, 5, 100, 51)
}

func TestSplitPush_WithCommonValues_2(t *testing.T) {
	checkSplitPush(t, 3, 3, 5, 50, 51)
}

func TestSplitPush_WithCommonValues_3(t *testing.T) {
	checkSplitPush(t, 3, 3, 5, 52, 51)
}

func TestSplitPush_With_Much_Partitions(t *testing.T) {
	checkSplitPush(t, 100, 2, 3, 11, 100000)
}

func TestSplitPush_With_Much_Partitions_2(t *testing.T) {
	checkSplitPush(t, 100, 51, 53, 10, 100000)
}

func TestSplitPush_WithManyErrors(t *testing.T) {
	checkSplitPush(t, 10, 150, 500, 5, 10000)
}

func TestSplitPush_WithLessErrors(t *testing.T) {
	checkSplitPush(t, 10, 150, 500, 250, 10000)
}

func TestSplitPush_With_Small_Errors(t *testing.T) {
	checkSplitPush(t, 10, 131, 537, 7967, 100*100)
}

func TestSplitPush_With_Small_Errors_2(t *testing.T) {
	checkSplitPush(t, 10, 131, 6312, 5113, 100000)
}

func TestSplitPush_With_Small_Errors_3(t *testing.T) {
	checkSplitPush(t, 10, 6312, 100, 5113, 100000)
}

func TestSplitPush_With_Small_Errors_4(t *testing.T) {
	checkSplitPush(t, 10, 111, 112, 5113, 100000)
}

func TestSplitPush_With_Small_Errors_5(t *testing.T) {
	checkSplitPush(t, 10, 111, 112, 4712, 100000)
}

func TestSplitPush_With_Small_Errors_6(t *testing.T) {
	checkSplitPush(t, 5, 132, 112, 10457, 100000)
}

func TestSplitPush_With_Much_Errors(t *testing.T) {
	checkSplitPush(t, 10, 131, 537, 100, 100000)
}

func TestSplitPush_With_Much_Errors_2(t *testing.T) {
	checkSplitPush(t, 10, 131, 537, 121, 100000)
}

func TestSplitPush_With_Much_Errors_3(t *testing.T) {
	checkSplitPush(t, 10, 131, 537, 100, 100000)
}

func TestSplitPush_With_Much_Errors_4(t *testing.T) {
	checkSplitPush(t, 10, 131, 537, 101, 100000)
}

func TestSplitPush_With_Much_Errors_5(t *testing.T) {
	checkSplitPush(t, 10, 5, 537, 6, 100000)
}

func TestSplitPush_With_Much_Errors_6(t *testing.T) {
	checkSplitPush(t, 10, 97, 5, 323, 100000)
}

func TestSplitPush_With_Much_Errors_7(t *testing.T) {
	checkSplitPush(t, 11, 5, 17, 7, 10000)
}

func TestSplitPush_Synchronous_With_Error(t *testing.T) {
	checkSplitPush(t, 1, 3, 5, 50, 51)
}

func TestSplitPush_Synchronous_With_Error_2(t *testing.T) {
	checkSplitPush(t, 1, 11, 7, 3, 10000)
}

func TestSplitPush_MaxFileSize_Equal_BlockSize(t *testing.T) {
	checkSplitPush(t, 3, 7919, 7919, 0, 1000*1000)
}
