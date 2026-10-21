package internal_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

type staleDeletedObjectState struct {
	folderPath      string
	objects         []storage.Object
	listedVersionID string
	readObjectName  string
	readObjectError error
}

// staleDeletedObjectFolder simulates a versioned object store returning an object version from
// ListFolder immediately after a delete marker made an unversioned ReadObject return not found.
type staleDeletedObjectFolder struct {
	storage.Folder
	state *staleDeletedObjectState
}

func newStaleDeletedObjectFolder(folder storage.Folder) *staleDeletedObjectFolder {
	return &staleDeletedObjectFolder{
		Folder: folder,
		state: &staleDeletedObjectState{
			listedVersionID: "stale-version",
		},
	}
}

func (folder *staleDeletedObjectFolder) GetSubFolder(relativePath string) storage.Folder {
	return &staleDeletedObjectFolder{
		Folder: folder.Folder.GetSubFolder(relativePath),
		state:  folder.state,
	}
}

func (folder *staleDeletedObjectFolder) ListFolder(
	ctx context.Context,
) ([]storage.Object, []storage.Folder, error) {
	objects, subFolders, err := folder.Folder.ListFolder(ctx)
	if err == nil && folder.GetPath() == folder.state.folderPath {
		objects = append(objects, folder.state.objects...)
	}
	return objects, subFolders, err
}

func (folder *staleDeletedObjectFolder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	if err := folder.Folder.DeleteObjects(ctx, objects); err != nil {
		return err
	}
	folder.state.folderPath = folder.GetPath()
	for _, object := range objects {
		folder.state.objects = append(folder.state.objects, storage.NewLocalObjectWithVersion(
			object.GetName(),
			object.GetLastModified(),
			object.GetSize(),
			folder.state.listedVersionID,
			object.GetAdditionalInfo(),
		))
	}
	return nil
}

func (folder *staleDeletedObjectFolder) ReadObject(
	ctx context.Context,
	objectRelativePath string,
) (io.ReadCloser, error) {
	if objectRelativePath == folder.state.readObjectName && folder.state.readObjectError != nil {
		return nil, folder.state.readObjectError
	}
	return folder.Folder.ReadObject(ctx, objectRelativePath)
}

var (
	JournalFmt              = "%09d"
	BackupName              = "stream"
	MinimalJournalNumber    = time.Now()
	DefaultJournalDirectory = "journals_005"
	journalTimestamps       = map[int]time.Time{}
)

func NewEmptyTestJournal(
	JournalName string,
	start, end string,
) internal.JournalInfo {
	return internal.JournalInfo{
		JournalDirectoryName: DefaultJournalDirectory,
	}
}

func initTestS3() (storage.Folder, internal.Uploader) {
	root := memory.NewFolder("", memory.NewKVS())
	mockUploader := internal.NewRegularUploader(
		&testtools.MockCompressor{},
		root,
	)
	return root, mockUploader
}

func numberToJournalTimestamp(num int) time.Time {
	return journalTimestamps[num]
}

func generateAndUploadData(t *testing.T, mockUploader internal.Uploader) {
	recordCount := 100
	recordSize := 1

	record := strings.Repeat("a", recordSize)
	for i := 1; i <= recordCount; i++ {
		journalPath := fmt.Sprintf("%s/"+JournalFmt, DefaultJournalDirectory, i)

		r := bytes.NewReader([]byte(record))
		err := mockUploader.Upload(t.Context(), journalPath, r)
		assert.NoError(t, err)

		time.Sleep(time.Millisecond)
	}

	objs, _, err := mockUploader.Folder().GetSubFolder(DefaultJournalDirectory).ListFolder(t.Context())
	assert.NoError(t, err)
	for _, obj := range objs {
		value, err := strconv.Atoi(obj.GetName())
		assert.NoError(t, err)

		journalTimestamps[value] = obj.GetLastModified()
	}
}

func CreateThreeJournals(
	t *testing.T,
	folder storage.Folder,
) (internal.JournalInfo, internal.JournalInfo, internal.JournalInfo) {
	ji1 := internal.NewEmptyJournalInfo(
		fmt.Sprintf(
			"%s_%s",
			BackupName,
			time.
				Now().
				Add(time.Second*5).
				Format(internal.JournalTimeLayout),
		),
		MinimalJournalNumber, MinimalJournalNumber,
		DefaultJournalDirectory,
	)
	assert.NoError(t, ji1.Upload(t.Context(), folder))

	ji2 := internal.NewEmptyJournalInfo(
		fmt.Sprintf(
			"%s_%s",
			BackupName,
			time.
				Now().
				Add(time.Second*10).
				Format(internal.JournalTimeLayout),
		),
		MinimalJournalNumber, numberToJournalTimestamp(33),
		DefaultJournalDirectory,
	)

	assert.NoError(t, ji2.Upload(t.Context(), folder))
	assert.NoError(t, ji2.UpdateIntervalSize(t.Context(), folder, &internal.JournalFiles{}))
	assert.NoError(t, ji1.Read(t.Context(), folder))

	ji3 := internal.NewEmptyJournalInfo(
		fmt.Sprintf(
			"%s_%s",
			BackupName,
			time.
				Now().
				Add(time.Second*15).
				Format(internal.JournalTimeLayout),
		),
		numberToJournalTimestamp(33), numberToJournalTimestamp(66),
		DefaultJournalDirectory,
	)

	assert.NoError(t, ji3.Upload(t.Context(), folder))
	assert.NoError(t, ji3.UpdateIntervalSize(t.Context(), folder, &internal.JournalFiles{}))
	assert.NoError(t, ji2.Read(t.Context(), folder))
	assert.NoError(t, ji1.Read(t.Context(), folder))

	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
	assert.Equal(t, int64(33), ji2.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)

	return ji1, ji2, ji3
}

func TestCreateThreeJournals(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	CreateThreeJournals(t, folder)
}

func TestDeleteJournalInMiddle(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	assert.NoError(t, ji2.Delete(t.Context(), folder))
	assert.NoError(t, ji1.Read(t.Context(), folder))
	assert.NoError(t, ji3.Read(t.Context(), folder))
	assert.Equal(t, int64(66), ji1.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}

func TestDeleteJournalInMiddleWithStaleDeletedObjectListing(t *testing.T) {
	baseFolder, uploader := initTestS3()
	generateAndUploadData(t, uploader)
	folder := newStaleDeletedObjectFolder(baseFolder)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	require.NoError(t, ji2.Delete(t.Context(), folder))
	require.NoError(t, ji1.Read(t.Context(), folder))
	require.NoError(t, ji3.Read(t.Context(), folder))
	assert.Equal(t, int64(66), ji1.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}

func TestDeleteJournalDoesNotIgnoreOtherReadErrors(t *testing.T) {
	baseFolder, uploader := initTestS3()
	generateAndUploadData(t, uploader)
	folder := newStaleDeletedObjectFolder(baseFolder)

	_, ji2, _ := CreateThreeJournals(t, folder)
	folder.state.readObjectName = ji2.JournalName
	folder.state.readObjectError = assert.AnError

	require.ErrorIs(t, ji2.Delete(t.Context(), folder), assert.AnError)
}

func TestDeleteJournalDoesNotIgnoreNotFoundFromUnversionedListing(t *testing.T) {
	baseFolder, uploader := initTestS3()
	generateAndUploadData(t, uploader)
	folder := newStaleDeletedObjectFolder(baseFolder)
	folder.state.listedVersionID = ""

	_, ji2, _ := CreateThreeJournals(t, folder)
	err := ji2.Delete(t.Context(), folder)

	var notFoundErr storage.ObjectNotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func TestDeleteJournalInBegin(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	assert.NoError(t, ji1.Delete(t.Context(), folder))
	assert.NoError(t, ji2.Read(t.Context(), folder))
	assert.NoError(t, ji3.Read(t.Context(), folder))
	assert.Equal(t, int64(33), ji2.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}

func TestDeleteJournalInEnd(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	assert.NoError(t, ji3.Delete(t.Context(), folder))
	assert.NoError(t, ji1.Read(t.Context(), folder))
	assert.NoError(t, ji2.Read(t.Context(), folder))
	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
	assert.Equal(t, int64(0), ji2.SizeToNextBackup)
	fmt.Println(ji1.JournalName, ji2.JournalName, ji3.JournalName)
}

// An empty journal directory means the size was not measured, not that it is zero: overwriting a
// previously computed SizeToNextBackup with 0 would silently lose it.
func TestEmptyJournalDirectoryKeepsPreviousSize(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, _, ji3 := CreateThreeJournals(t, folder)

	emptyDirFolder := memory.NewFolder("", memory.NewKVS())
	require.NoError(t, ji1.Upload(t.Context(), emptyDirFolder))
	require.NoError(t, ji3.Upload(t.Context(), emptyDirFolder))

	assert.NoError(t, ji3.UpdateIntervalSize(t.Context(), emptyDirFolder, &internal.JournalFiles{}))

	require.NoError(t, ji1.Read(t.Context(), emptyDirFolder))
	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
}

func TestSafetyOfRepeatingMethodCalls(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	// There are random method calls
	for i := 0; i < 10; i++ {
		assert.NoError(t, ji1.UpdateIntervalSize(t.Context(), folder, &internal.JournalFiles{}))
		assert.NoError(t, ji1.Upload(t.Context(), folder))
		assert.NoError(t, ji3.Read(t.Context(), folder))
		assert.NoError(t, ji2.Upload(t.Context(), folder))
		assert.NoError(t, ji2.UpdateIntervalSize(t.Context(), folder, &internal.JournalFiles{}))
		assert.NoError(t, ji3.Upload(t.Context(), folder))
		assert.NoError(t, ji3.UpdateIntervalSize(t.Context(), folder, &internal.JournalFiles{}))
		assert.NoError(t, ji2.Read(t.Context(), folder))
		assert.NoError(t, ji1.Read(t.Context(), folder))
	}

	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
	assert.Equal(t, int64(33), ji2.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}
