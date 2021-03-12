package testtools

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/memory"
	"github.com/wal-g/storages/s3"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/fsutil"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/utility"
)

func MakeDefaultInMemoryStorageFolder() *memory.Folder {
	return memory.NewFolder("in_memory/", memory.NewStorage())
}

func MakeDefaultUploader(uploaderAPI s3manageriface.UploaderAPI) *s3.Uploader {
	return s3.NewUploader(uploaderAPI, "", "", "STANDARD")
}

func NewMockUploader(apiMultiErr, apiErr bool) *internal.Uploader {
	s3Uploader := MakeDefaultUploader(NewMockS3Uploader(apiMultiErr, apiErr, nil))
	return internal.NewUploader(
		&MockCompressor{},
		s3.NewFolder(*s3Uploader, NewMockS3Client(false, true), "bucket/", "server/", false),
	)
}

func NewStoringMockUploader(storage *memory.Storage, deltaDataFolder fsutil.DataFolder) *internal.Uploader {
	return internal.NewUploader(
		&MockCompressor{},
		memory.NewFolder("in_memory/", storage),
	)
}

func NewMockWalUploader(apiMultiErr, apiErr bool) *internal.WalUploader {
	s3Uploader := MakeDefaultUploader(NewMockS3Uploader(apiMultiErr, apiErr, nil))
	return internal.NewWalUploader(
		&MockCompressor{},
		s3.NewFolder(*s3Uploader, NewMockS3Client(false, true), "bucket/", "server/", false),
		nil,
	)
}

func CreateMockStorageFolder() storage.Folder {
	var folder = MakeDefaultInMemoryStorageFolder()
	subFolder := folder.GetSubFolder(utility.BaseBackupPath)
	subFolder.PutObject("base_123_backup_stop_sentinel.json", &bytes.Buffer{})
	subFolder.PutObject("base_456_backup_stop_sentinel.json", strings.NewReader("{}"))
	subFolder.PutObject("base_000_backup_stop_sentinel.json", &bytes.Buffer{}) // last put
	subFolder.PutObject("base_123312", &bytes.Buffer{})                        // not a sentinel
	subFolder.PutObject("base_321/nop", &bytes.Buffer{})
	subFolder.PutObject("folder123/nop", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/1", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/2", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/3", &bytes.Buffer{})
	return folder
}

func CreateMockStorageFolderWithDeltaBackups(t *testing.T) storage.Folder {
	var folder = MakeDefaultInMemoryStorageFolder()
	subFolder := folder.GetSubFolder(utility.BaseBackupPath)
	sentinelData := map[string]interface{}{
		"DeltaFrom":     "",
		"DeltaFullName": "base_000000010000000000000007",
		"DeltaFromLSN":  0,
		"DeltaCount":    0,
	}
	emptySentinelData := map[string]interface{}{}
	backupNames := map[string]interface{}{
		"base_000000010000000000000003":                            emptySentinelData,
		"base_000000010000000000000005_D_000000010000000000000003": sentinelData,
		"base_000000010000000000000007":                            emptySentinelData,
		"base_000000010000000000000009_D_000000010000000000000007": sentinelData}
	for backupName, sentinelD := range backupNames {
		bytesSentinel, err := json.Marshal(&sentinelD)
		assert.NoError(t, err)
		sentinelString := string(bytesSentinel)
		err = subFolder.PutObject(backupName+utility.SentinelSuffix, strings.NewReader(sentinelString))
		assert.NoError(t, err)
	}
	return folder
}

var EmptyData = map[string]interface{}{}

var BackupNamesDefault = map[string]interface{}{
	"base_000000010000000000000002": map[string]interface{}{
		"start_time":   utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
		"finish_time":  utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
		"hostname":     "",
		"data_dir":     "",
		"pg_version":   0,
		"start_lsn":    16777216, // logSegNo = 1
		"finish_lsn":   33554432, // logSegNo = 2
		"is_permanent": true,
	},
	"base_000000010000000000000004_D_000000010000000000000002": map[string]interface{}{
		"start_time":   utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
		"finish_time":  utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
		"hostname":     "",
		"data_dir":     "",
		"pg_version":   0,
		"start_lsn":    16777217, // logSegNo = 1
		"finish_lsn":   33554433, // logSegNo = 2
		"is_permanent": true,
	},
	"base_000000010000000000000006_D_000000010000000000000004": EmptyData,
}

var BackupNamesCreationTime = map[string]interface{}{
	"base_000000010000000000000002": map[string]interface{}{
		"start_time":   time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
	},
	"base_000000010000000000000004_D_000000010000000000000002": map[string]interface{}{
		"start_time":   time.Date(2020, 4, 25, 14, 48, 0, 0, time.UTC),
	},
	"base_000000010000000000000006_D_000000010000000000000004": map[string]interface{}{
		"start_time":   time.Date(2018, 4, 25, 14, 48, 0, 0, time.UTC),
	},
}

var WalNames = map[string]interface{}{
	"000000010000000000000001": EmptyData,
	"000000010000000000000002": EmptyData,
	"000000010000000000000003": EmptyData,
}

func CreateMockStorageFolderWithPermanentBackups(t *testing.T, backupNames map[string]interface{}, walNames map[string]interface{}) storage.Folder {
	folder := MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	walBackupFolder := folder.GetSubFolder(utility.WalPath)
	for backupName, metadata := range backupNames {
		// empty sentinel
		empty, err := json.Marshal(&EmptyData)
		assert.NoError(t, err)
		sentinelString := string(empty)
		err = baseBackupFolder.PutObject(backupName+utility.SentinelSuffix, strings.NewReader(sentinelString))

		// metadata
		assert.NoError(t, err)
		bytesMetadata, err := json.Marshal(&metadata)
		assert.NoError(t, err)
		metadataString := string(bytesMetadata)
		err = baseBackupFolder.PutObject(backupName+"/"+utility.MetadataFileName, strings.NewReader(metadataString))
		assert.NoError(t, err)
	}
	for walName, data := range walNames {
		bytes, err := json.Marshal(&data)
		assert.NoError(t, err)
		walString := string(bytes)
		err = walBackupFolder.PutObject(walName+".lz4", strings.NewReader(walString))
		assert.NoError(t, err)
	}
	return folder
}

func CreateWalPageWithContinuation() []byte {
	pageHeader := walparser.XLogPageHeader{
		Info:             walparser.XlpFirstIsContRecord,
		RemainingDataLen: 12312,
	}
	data := make([]byte, 20)
	binary.LittleEndian.PutUint16(data, pageHeader.Magic)
	binary.LittleEndian.PutUint16(data, pageHeader.Info)
	binary.LittleEndian.PutUint32(data, uint32(pageHeader.TimeLineID))
	binary.LittleEndian.PutUint64(data, uint64(pageHeader.PageAddress))
	binary.LittleEndian.PutUint32(data, pageHeader.RemainingDataLen)
	for len(data) < int(walparser.WalPageSize) {
		data = append(data, 2)
	}
	return data
}

func GetXLogRecordData() (walparser.XLogRecord, []byte) {
	imageData := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
	}
	blockData := []byte{
		0x0a, 0x0b, 0x0c,
	}
	mainData := []byte{
		0x0d, 0x0e, 0x0f, 0x10,
	}
	data := []byte{ // block header data
		0xfd, 0x01, 0xfe,
		0x00, 0x30, 0x03, 0x00, 0x0a, 0x00, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
		0xff, 0x04,
	}
	data = utility.ConcatByteSlices(utility.ConcatByteSlices(utility.ConcatByteSlices(data, imageData), blockData), mainData)
	recordHeader := walparser.XLogRecordHeader{
		TotalRecordLength: uint32(walparser.XLogRecordHeaderSize + len(data)),
		XactID:            0x00000243,
		PrevRecordPtr:     0x000000002affedc8,
		Info:              0xb0,
		ResourceManagerID: 0x00,
		Crc32Hash:         0xecf5203c,
	}
	var recordHeaderData bytes.Buffer
	recordHeaderData.Write(utility.ToBytes(&recordHeader.TotalRecordLength))
	recordHeaderData.Write(utility.ToBytes(&recordHeader.XactID))
	recordHeaderData.Write(utility.ToBytes(&recordHeader.PrevRecordPtr))
	recordHeaderData.Write(utility.ToBytes(&recordHeader.Info))
	recordHeaderData.Write(utility.ToBytes(&recordHeader.ResourceManagerID))
	recordHeaderData.Write([]byte{0, 0})
	recordHeaderData.Write(utility.ToBytes(&recordHeader.Crc32Hash))
	recordData := utility.ConcatByteSlices(recordHeaderData.Bytes(), data)
	record, _ := walparser.ParseXLogRecordFromBytes(recordData)
	return *record, recordData
}

type ReadWriteNopCloser struct {
	io.ReadWriter
}

func (readWriteNopCloser *ReadWriteNopCloser) Close() error {
	return nil
}

func AssertReaderIsEmpty(t *testing.T, reader io.Reader) {
	buf := make([]byte, 1)
	_, err := reader.Read(buf)
	assert.Equal(t, io.EOF, err)
}

type NopCloserWriter struct {
	io.Writer
}

func (NopCloserWriter) Close() error {
	return nil
}

type NopCloser struct{}

func (closer *NopCloser) Close() error {
	return nil
}

type NopSeeker struct{}

func (seeker *NopSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

var MockCloseError = errors.New("mock close: close error")
var MockReadError = errors.New("mock reader: read error")
var MockWriteError = errors.New("mock writer: write error")

//ErrorWriter struct implements io.Writer interface.
//Its Write method returns zero and non-nil error on every call
type ErrorWriter struct{}

func (w ErrorWriter) Write(b []byte) (int, error) {
	return 0, MockWriteError
}

//ErrorReader struct implements io.Reader interface.
//Its Read method returns zero and non-nil error on every call
type ErrorReader struct{}

func (r ErrorReader) Read(b []byte) (int, error) {
	return 0, MockReadError
}

type BufCloser struct {
	*bytes.Buffer
	Err bool
}

func (w *BufCloser) Close() error {
	if w.Err {
		return MockCloseError
	}
	return nil
}

type ErrorWriteCloser struct{}

func (ew ErrorWriteCloser) Write(p []byte) (int, error) {
	return -1, MockWriteError
}

func (ew ErrorWriteCloser) Close() error {
	return MockCloseError
}
