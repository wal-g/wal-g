package gcs

import (
	"errors"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"

	"github.com/wal-g/storages/storage"

	"github.com/stretchr/testify/assert"
)

func TestNewFolder(t *testing.T) {
	testCases := []struct {
		bucketHandle    *gcs.BucketHandle
		path            string
		timeout         int
		normalizePrefix bool
		encryptionKey   []byte
		folder          *Folder
		uploaderOptions []UploaderOption
	}{
		{
			path:    "path",
			timeout: 10,
			folder:  &Folder{path: "path", contextTimeout: 10, encryptionKey: []byte{}},
		},
		{
			path:            "path",
			timeout:         10,
			normalizePrefix: true,
			encryptionKey:   []byte("test"),
			folder:          &Folder{path: "path", contextTimeout: 10, normalizePrefix: true, encryptionKey: []byte("test")},
		},
	}

	for _, tc := range testCases {
		newFolder := NewFolder(tc.bucketHandle, tc.path, tc.timeout, tc.normalizePrefix, tc.encryptionKey, tc.uploaderOptions)
		assert.Equal(t, tc.folder, newFolder)
	}
}

func TestGSFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	storageFolder, err := ConfigureFolder("gs://x4m-test/walg-bucket",
		nil)

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}

func TestGSExactFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	//os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/x4mmm/Downloads/mdb-tests-d0uble-0b98813b622b.json")
	//os.Setenv("GCS_CONTEXT_TIMEOUT", "1024000000")

	storageFolder, err := ConfigureFolder("gs://x4m-test//walg-bucket////strange_folder",
		map[string]string{
			NormalizePrefix: "false",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}

func TestGSFolderWithEncryptionKey(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	storageFolder, err := ConfigureFolder("gs://x4m-test/walg-bucket",
		map[string]string{
			EncryptionKey: "F2F90NxJ2LrC/ujDQVGFfHetdDgjIMyrDkkN1VqGNnw=",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}

type fakeReader struct{}

func (f fakeReader) Read(_ []byte) (int, error) {
	return 0, errors.New("failed to fake read")
}

func TestUploadingReaderFails(t *testing.T) {
	folder := Folder{
		bucket: &gcs.BucketHandle{},
		path:   "path",
	}

	err := folder.PutObject("name", fakeReader{})
	assert.EqualError(t, err, "GCS error : Unable to read a chunk of data to upload: failed to fake read")
}

func TestJitterDelay(t *testing.T) {
	baseDelay := time.Second
	delay := getJitterDelay(baseDelay)

	assert.GreaterOrEqual(t, int64(delay), int64(baseDelay))
	assert.LessOrEqual(t, int64(delay), int64(2*baseDelay))
}

func TestMinDuration(t *testing.T) {
	testCases := []struct {
		duration1           time.Duration
		duration2           time.Duration
		expectedMinDuration time.Duration
	}{
		{
			duration1:           time.Second,
			duration2:           5 * time.Second,
			expectedMinDuration: time.Second,
		},
		{
			duration1:           3 * time.Second,
			duration2:           2 * time.Second,
			expectedMinDuration: 2 * time.Second,
		},
		{
			duration1:           time.Second,
			duration2:           time.Second,
			expectedMinDuration: time.Second,
		},
	}

	for _, tc := range testCases {
		result := minDuration(tc.duration1, tc.duration2)
		assert.Equal(t, tc.expectedMinDuration, result)
	}
}

func TestUploaderOptions(t *testing.T) {
	testCases := []struct {
		settings          map[string]string
		expectedChunkSize int64
		expectedRetries   int
	}{
		{
			settings:          map[string]string{},
			expectedChunkSize: 50 << 20,
			expectedRetries:   16,
		},
		{
			settings: map[string]string{
				"GCS_MAX_CHUNK_SIZE": "100",
				"GCS_MAX_RETRIES":    "5",
			},
			expectedChunkSize: 100,
			expectedRetries:   5,
		},
	}

	for _, tc := range testCases {
		uploaderOptions, err := getUploaderOptions(tc.settings)
		require.Nil(t, err)

		uploader := NewUploader(nil, uploaderOptions...)

		assert.Equal(t, tc.expectedChunkSize, uploader.maxChunkSize)
		assert.Equal(t, tc.expectedRetries, uploader.maxUploadRetries)
	}
}

func TestInvalidUploaderOptions(t *testing.T) {
	testCases := []struct {
		settings  map[string]string
		errString string
	}{
		{
			settings:  map[string]string{"GCS_MAX_CHUNK_SIZE": "invalid"},
			errString: `invalid maximum chunk size setting: strconv.ParseInt: parsing "invalid": invalid syntax`,
		},
		{
			settings:  map[string]string{"GCS_MAX_RETRIES": "test"},
			errString: `invalid maximum retries setting: strconv.Atoi: parsing "test": invalid syntax`,
		},
	}

	for _, tc := range testCases {
		uploaderOptions, err := getUploaderOptions(tc.settings)
		assert.Nil(t, uploaderOptions)
		assert.EqualError(t, err, tc.errString)
	}
}
