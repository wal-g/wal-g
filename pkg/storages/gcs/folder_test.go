package gcs

import (
	"errors"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/stretchr/testify/assert"
)

func TestGSFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	st, err := ConfigureStorage("gs://x4m-test/walg-bucket", nil)
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

func TestGSExactFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	//os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/x4mmm/Downloads/mdb-tests-d0uble-0b98813b622b.json")
	//os.Setenv("GCS_CONTEXT_TIMEOUT", "1024000000")

	st, err := ConfigureStorage("gs://x4m-test//walg-bucket////strange_folder",
		map[string]string{
			normalizePrefixSetting: "false",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

func TestGSFolderWithEncryptionKey(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	st, err := ConfigureStorage("gs://x4m-test/walg-bucket",
		map[string]string{
			encryptionKeySetting: "F2F90NxJ2LrC/ujDQVGFfHetdDgjIMyrDkkN1VqGNnw=",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

type fakeReader struct{}

func (f fakeReader) Read(_ []byte) (int, error) {
	return 0, errors.New("failed to fake read")
}

func TestUploadingReaderFails(t *testing.T) {
	folder := Folder{
		bucket:        &gcs.BucketHandle{},
		path:          "path",
		encryptionKey: make([]byte, 32),
		config: &Config{
			ContextTimeout: time.Hour,
			Uploader:       &UploaderConfig{MaxRetries: 1, MaxChunkSize: defaultMaxChunkSize},
		},
	}

	err := folder.PutObject("name", fakeReader{})
	assert.EqualError(t, err, `read a chunk of object "path/name" to upload to GCS: failed to fake read`)
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
