package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	CreateFileWithPath = "./testdata/createFileWith"
)

var times = []struct {
	input internal.BackupTime
}{
	{internal.BackupTime{
		BackupName:  "second",
		Time:        time.Date(2017, 2, 2, 30, 48, 39, 651387233, time.UTC),
		WalFileName: "",
	}},
	{internal.BackupTime{
		BackupName:  "fourth",
		Time:        time.Date(2009, 2, 27, 20, 8, 33, 651387235, time.UTC),
		WalFileName: "",
	}},
	{internal.BackupTime{
		BackupName:  "fifth",
		Time:        time.Date(2008, 11, 20, 16, 34, 58, 651387232, time.UTC),
		WalFileName: "",
	}},
	{internal.BackupTime{
		BackupName:  "first",
		Time:        time.Date(2020, 11, 31, 20, 3, 58, 651387237, time.UTC),
		WalFileName: "",
	}},
	{internal.BackupTime{
		BackupName:  "third",
		Time:        time.Date(2009, 3, 13, 4, 2, 42, 651387234, time.UTC),
		WalFileName: "",
	}},
}

func TestSortLatestTime(t *testing.T) {
	correct := [5]string{"first", "second", "third", "fourth", "fifth"}
	sortTimes := make([]internal.BackupTime, 5)

	for i, val := range times {
		sortTimes[i] = val.input
	}

	sort.Sort(internal.TimeSlice(sortTimes))

	for i, val := range sortTimes {
		assert.Equal(t, correct[i], val.BackupName)
	}
}

// Tests that backup name is successfully extracted from
// return values of pg_stop_backup(false)
func TestCheckType(t *testing.T) {
	var fileNames = []struct {
		input    string
		expected string
	}{
		{"mock.lzo", "lzo"},
		{"mock.tar.lzo", "lzo"},
		{"mock.gzip", "gzip"},
		{"mockgzip", ""},
	}
	for _, f := range fileNames {
		actual := internal.GetFileExtension(f.input)
		assert.Equal(t, f.expected, actual)
	}
}

func TestGetSentinelUserData(t *testing.T) {

	os.Setenv("WALG_SENTINEL_USER_DATA", "1.0")

	data := internal.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, 1.0, data.(float64), "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Setenv("WALG_SENTINEL_USER_DATA", "\"1\"")

	data = internal.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, "1", data.(string), "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Setenv("WALG_SENTINEL_USER_DATA", `{"x":123,"y":["asdasd",123]}`)

	data = internal.GetSentinelUserData()
	t.Log(data)
	assert.NotNilf(t, data, "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestCreateFileWith(t *testing.T) {
	content := "content"
	err := internal.CreateFileWith(CreateFileWithPath, strings.NewReader(content))
	assert.NoError(t, err)
	actualContent, err := ioutil.ReadFile(CreateFileWithPath)
	assert.NoError(t, err)
	assert.Equal(t, []byte(content), actualContent)
	os.Remove(CreateFileWithPath)
}

func TestCreateFileWith_ExistenceError(t *testing.T) {
	file, err := os.Create(CreateFileWithPath)
	assert.NoError(t, err)
	file.Close()
	err = internal.CreateFileWith(CreateFileWithPath, strings.NewReader("error"))
	assert.Equal(t, os.IsExist(err), true)
	os.Remove(CreateFileWithPath)
}

func TestStripBackupName(t *testing.T) {
	var testCases = []struct {
		input    string
		expected string
	}{
		{"file_backup", "file"},
		{"backup", "backup"},
		{"/other_backup", "other"},
		{"path/to/tables_backup", "tables"},
		{"anotherPath/to/document_backup_backup", "document"},
		{"anotherPath/to/fileBackup", "fileBackup"},
	}

	for _, testCase := range testCases {
		actual := internal.StripBackupName(testCase.input)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestGetMaxConcurrency_InvalidKeyAndValidDefaultValue(t *testing.T) {
	actual, err := internal.GetMaxConcurrency("INVALID_KEY", 3)

	assert.NoError(t, err)
	assert.Equal(t, 3, actual)
}

func TestGetMaxConcurrency_InvalidKeyAndInvalidDefaultValue(t *testing.T) {
	actual, err := internal.GetMaxConcurrency("INVALID_KEY", -1)

	assert.NoError(t, err)
	assert.Equal(t, 10, actual)
}

func TestGetMaxConcurrency_ValidKey(t *testing.T) {
	os.Setenv("WALG_UPLOAD_CONCURRENCY", "100")
	actual, err := internal.GetMaxConcurrency("WALG_UPLOAD_CONCURRENCY", 1)

	assert.NoError(t, err)
	assert.Equal(t, 100, actual)
	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestGetMaxConcurrency_ValidKeyAndInvalidDefaultValue(t *testing.T) {
	os.Setenv("WALG_UPLOAD_CONCURRENCY", "100")
	actual, err := internal.GetMaxConcurrency("WALG_UPLOAD_CONCURRENCY", -1)

	assert.NoError(t, err)
	assert.Equal(t, 100, actual)
	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestGetMaxConcurrency_ValidKeyAndNegativeValue(t *testing.T) {
	os.Setenv("WALG_UPLOAD_CONCURRENCY", "-5")
	actual, err := internal.GetMaxConcurrency("WALG_UPLOAD_CONCURRENCY", 0)

	assert.NoError(t, err)
	assert.Equal(t, 1, actual)
	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestGetMaxConcurrency_ValidKeyAndInvalidValue(t *testing.T) {
	os.Setenv("WALG_UPLOAD_CONCURRENCY", "invalid")
	_, err := internal.GetMaxConcurrency("WALG_UPLOAD_CONCURRENCY", 1)

	assert.Error(t, err)
	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}
