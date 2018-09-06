package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
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
	input walg.BackupTime
}{
	{walg.BackupTime{
		Name:        "second",
		Time:        time.Date(2017, 2, 2, 30, 48, 39, 651387233, time.UTC),
		WalFileName: "",
	}},
	{walg.BackupTime{
		Name:        "fourth",
		Time:        time.Date(2009, 2, 27, 20, 8, 33, 651387235, time.UTC),
		WalFileName: "",
	}},
	{walg.BackupTime{
		Name:        "fifth",
		Time:        time.Date(2008, 11, 20, 16, 34, 58, 651387232, time.UTC),
		WalFileName: "",
	}},
	{walg.BackupTime{
		Name:        "first",
		Time:        time.Date(2020, 11, 31, 20, 3, 58, 651387237, time.UTC),
		WalFileName: "",
	}},
	{walg.BackupTime{
		Name:        "third",
		Time:        time.Date(2009, 3, 13, 4, 2, 42, 651387234, time.UTC),
		WalFileName: "",
	}},
}

func TestSortLatestTime(t *testing.T) {
	correct := [5]string{"first", "second", "third", "fourth", "fifth"}
	sortTimes := make([]walg.BackupTime, 5)

	for i, val := range times {
		sortTimes[i] = val.input
	}

	sort.Sort(walg.TimeSlice(sortTimes))

	for i, val := range sortTimes {
		assert.Equal(t, correct[i], val.Name)
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
		actual := walg.GetFileExtension(f.input)
		assert.Equal(t, f.expected, actual)
	}
}

func TestGetSentinelUserData(t *testing.T) {

	os.Setenv("WALG_SENTINEL_USER_DATA", "1.0")

	data := walg.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, 1.0, data.(float64), "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Setenv("WALG_SENTINEL_USER_DATA", "\"1\"")

	data = walg.GetSentinelUserData()
	t.Log(data)
	assert.Equalf(t, "1", data.(string), "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Setenv("WALG_SENTINEL_USER_DATA", `{"x":123,"y":["asdasd",123]}`)

	data = walg.GetSentinelUserData()
	t.Log(data)
	assert.NotNilf(t, data, "Unable to parse WALG_SENTINEL_USER_DATA")

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestCreateFileWith(t *testing.T) {
	content := "content"
	err := walg.CreateFileWith(CreateFileWithPath, strings.NewReader(content))
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
	err = walg.CreateFileWith(CreateFileWithPath, strings.NewReader("error"))
	assert.Equal(t, os.IsExist(err), true)
	os.Remove(CreateFileWithPath)
}
