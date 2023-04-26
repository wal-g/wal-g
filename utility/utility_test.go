package utility_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

const (
	CreateFileWithPath = "../test/testdata/createFileWith"
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

	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})

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
		actual := utility.GetFileExtension(f.input)
		assert.Equal(t, f.expected, actual)
	}
}

func TestCreateFileWith(t *testing.T) {
	content := "content"
	err := ioextensions.CreateFileWith(CreateFileWithPath, strings.NewReader(content))
	assert.NoError(t, err)
	actualContent, err := os.ReadFile(CreateFileWithPath)
	assert.NoError(t, err)
	assert.Equal(t, []byte(content), actualContent)
	os.Remove(CreateFileWithPath)
}

func TestCreateFileWith_ExistenceError(t *testing.T) {
	file, err := os.Create(CreateFileWithPath)
	assert.NoError(t, err)
	file.Close()
	err = ioextensions.CreateFileWith(CreateFileWithPath, strings.NewReader("error"))
	assert.Equal(t, os.IsExist(err), true)
	os.Remove(CreateFileWithPath)
}

func TestStripRightmostBackupName(t *testing.T) {
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
		actual := utility.StripRightmostBackupName(testCase.input)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestStripPrefixName(t *testing.T) {
	var testCases = []struct {
		input    string
		expected string
	}{
		{"//path/path1//", "path1"},
		{"//path//path1/", "path1"},
		{"path/path1", "path1"},
		{"path/path1/path2", "path2"},
		{"path/path1//	/path2", "path2"},
		{"", ""},
		{"/", ""},
	}

	for _, testCase := range testCases {
		actual := utility.StripPrefixName(testCase.input)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestStripLeftmostBackupName(t *testing.T) {
	var testCases = []struct {
		input    string
		expected string
	}{
		{"stream_20210329T125616Z/metadata.json", "stream_20210329T125616Z"},
		{"/stream_20210329T125616Z/metadata.json", "stream_20210329T125616Z"},
		{"stream_20210329T125616Z_backup_stop_sentinel.json", "stream_20210329T125616Z"},
		{"/stream_20210329T125616Z_backup_stop_sentinel.json", "stream_20210329T125616Z"},
		{"/stream_20210329T125616Z/random_folder/random_subfolder/random_file", "stream_20210329T125616Z"},
		{"/stream_20210329T125616Z/random_folder/random_subfolder/random_file", "stream_20210329T125616Z"},
		{"base_0000000100000000000000C4/tar_partitions/part_001.tar.lz4", "base_0000000100000000000000C4"},
		{"base_0000000100000000000000C4_backup_stop_sentinel.json", "base_0000000100000000000000C4"},
		{"base_0000000100000000000000C9_D_0000000100000000000000C4", "base_0000000100000000000000C9_D_0000000100000000000000C4"},
		{"/stream_20210329T125616Z/random_folder/random_backup/random_file", "stream_20210329T125616Z"},
	}

	for _, testCase := range testCases {
		actual := utility.StripLeftmostBackupName(testCase.input)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestCeilTimeUpToMicroseconds_Works_When_Nanoseconds_Greater_Than_Zero(t *testing.T) {
	timeToCeil := time.Date(2000, 1, 1, 1, 1, 1, 1, time.UTC)
	expectedTime := time.Date(2000, 1, 1, 1, 1, 1, 1000, time.UTC)
	assert.Equal(t, expectedTime, utility.CeilTimeUpToMicroseconds(timeToCeil))
}

func TestCeilTimeUpToMicroseconds_Works_When_Nanoseconds_Equal_Zero(t *testing.T) {
	timeToCeil := time.Date(2000, 1, 1, 1, 1, 1, 0, time.UTC)
	assert.Equal(t, timeToCeil, utility.CeilTimeUpToMicroseconds(timeToCeil))
}

func TestFastCopy_NormalCases(t *testing.T) {
	var testDataLengths = []int64{
		utility.CopiedBlockMaxSize / 2,
		utility.CopiedBlockMaxSize,
		utility.CopiedBlockMaxSize * 2,
		utility.CopiedBlockMaxSize * 2.5,
	}

	for _, dataLength := range testDataLengths {
		currentData := make([]byte, dataLength)
		rand.Read(currentData)
		currentReader := bytes.NewReader(currentData)
		currentBuffer := new(bytes.Buffer)
		readLength, err := utility.FastCopy(currentBuffer, currentReader)
		assert.Equal(t, dataLength, readLength)
		assert.NoError(t, err)
		assert.Equal(t, currentData, currentBuffer.Bytes())
	}
}

func TestFastCopy_NotFails_OnEmptyData(t *testing.T) {
	emptyData := make([]byte, 0)
	reader := bytes.NewReader(emptyData)
	buffer := new(bytes.Buffer)
	readLength, err := utility.FastCopy(buffer, reader)
	result := buffer.Bytes()
	assert.Equal(t, int64(0), readLength)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestFastCopy_ReturnsError_WhenReaderFails(t *testing.T) {
	reader := new(testtools.ErrorReader)
	buffer := new(bytes.Buffer)
	_, err := utility.FastCopy(buffer, reader)
	assert.Error(t, err)
}

func TestFastCopy_ReturnsError_WhenWriterFails(t *testing.T) {
	reader := strings.NewReader("data")
	writer := new(testtools.ErrorWriter)
	_, err := utility.FastCopy(writer, reader)
	assert.Error(t, err)
}

func TestTryFetchTimeRFC3999_Valid(t *testing.T) {
	var testCases = []struct {
		input    string
		expected string
	}{
		{"20191015T211200Z", "20191015T211200Z"},
		{"20191015T211200Z22221015T211200Z", "20191015T211200Z"},
		{"         20191015T211200Z", "20191015T211200Z"},
		{"000000020191015T211200Z", "20191015T211200Z"},
		{"20191015T211200ZZZZZ", "20191015T211200Z"},
	}

	for _, testCase := range testCases {
		actual, ok := utility.TryFetchTimeRFC3999(testCase.input)
		assert.Equal(t, true, ok)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestTryFetchTimeRFC3999_Invalid(t *testing.T) {
	var testCases = []struct {
		input string
	}{
		{""},
		{"20191015T211200"},
		{"20191015211200Z"},
		{"20191015:211200Z"},
		{"20191015 211200Z"},
		{"TotallyBadTimeString"},
	}

	for _, testCase := range testCases {
		actual, ok := utility.TryFetchTimeRFC3999(testCase.input)
		assert.Equal(t, actual, "")
		assert.Equal(t, ok, false)
	}
}

func TestSelectMatchingFiles_EmptyMask(t *testing.T) {
	files := map[string]bool{
		"/a":   true,
		"/b/c": true,
		"d":    true,
	}
	selected, err := utility.SelectMatchingFiles("", files)
	assert.NoError(t, err)
	assert.Equal(t, files, selected)
}

func TestSelectMatchingFiles_InvalidMask(t *testing.T) {
	files := map[string]bool{
		"/a":   true,
		"/b/c": true,
		"d":    true,
	}
	_, err := utility.SelectMatchingFiles("[a-c", files)
	assert.Error(t, err)
}

func TestSelectMatchingFiles_ValidMask(t *testing.T) {
	files := map[string]bool{
		"/a":   true,
		"/b/c": true,
		"/b/e": true,
		"d":    true,
	}
	selected, err := utility.SelectMatchingFiles("b/*", files)
	assert.NoError(t, err)
	assert.Equal(t, map[string]bool{
		"/b/c": true,
		"/b/e": true,
	}, selected)
}

func TestSanitizePath_Sanitize(t *testing.T) {
	assert.Equal(t, "home", utility.SanitizePath("/home"))
}

func TestSanitizePath_LeaveSame(t *testing.T) {
	assert.Equal(t, "home", utility.SanitizePath("home"))
}

func TestNormalizePath_Normalize(t *testing.T) {
	assert.Equal(t, "home", utility.NormalizePath("home/"))
}

func TestNormalizePath_LeaveSame(t *testing.T) {
	assert.Equal(t, "home", utility.NormalizePath("home"))
}

func TestPathsEqual_SamePaths(t *testing.T) {
	assert.True(t, utility.PathsEqual("/home/ismirn0ff", "/home/ismirn0ff"))
}

func TestPathsEqual_NeedNormalization(t *testing.T) {
	assert.True(t, utility.PathsEqual("/home/ismirn0ff", "/home/ismirn0ff/"))
}

func TestPathsEqual_SubdirectoryDoesNotEquate(t *testing.T) {
	assert.False(t, utility.PathsEqual("/home/ismirn0ff", "/home/"))
	assert.False(t, utility.PathsEqual("/home/", "/home/ismirn0ff"))
}

func TestPathsEqual_RelativeDoesNotEqualAbsolute(t *testing.T) {
	assert.False(t, utility.PathsEqual("home/ismirn0ff", "/home/ismirn0ff"))
}

func TestIsInDirectory_SamePaths(t *testing.T) {
	assert.True(t, utility.IsInDirectory("/home", "/home"))
}

func TestIsInDirectory_NeedPathNormalization(t *testing.T) {
	assert.True(t, utility.IsInDirectory("/home/", "/home"))
}

func TestIsInDirectory_NeedDirectoryNormalization(t *testing.T) {
	assert.True(t, utility.IsInDirectory("/home", "/home/"))
}

func TestIsInDirectory_NeedBothNormalization(t *testing.T) {
	assert.True(t, utility.IsInDirectory("/home/", "/home/"))
}

func TestIsInDirectory_IsSubdirectory(t *testing.T) {
	assert.True(t, utility.IsInDirectory("/home/ismirn0ff", "/home/"))
}

func TestIsInDirectory_IsDirectoryAbove(t *testing.T) {
	assert.False(t, utility.IsInDirectory("/home", "/home/ismirn0ff"))
}

func TestIsInDirectory_DifferentDirectories(t *testing.T) {
	assert.False(t, utility.IsInDirectory("/tmp", "/home/ismirn0ff"))
}

func TestTrimFileExtension_EmptyFilePath(t *testing.T) {
	assert.Equal(t, "", utility.TrimFileExtension(""))
}

func TestTrimFileExtension_FileWithoutFilename(t *testing.T) {
	assert.Equal(t, "", utility.TrimFileExtension(".hidden"))
}

func TestTrimFileExtension_PathWithoutFilename(t *testing.T) {
	assert.Equal(t, "path/", utility.TrimFileExtension("path/.hidden"))
}

func TestTrimFileExtension_FileWithoutExtension(t *testing.T) {
	assert.Equal(t, "index", utility.TrimFileExtension("index"))
}

func TestTrimFileExtension_PathWithoutExtension(t *testing.T) {
	assert.Equal(t, "path/index", utility.TrimFileExtension("path/index"))
}

func TestTrimFileExtension_FileWithExtension(t *testing.T) {
	assert.Equal(t, "index", utility.TrimFileExtension("index.js"))
}

func TestTrimFileExtension_FileWithComplexExtension(t *testing.T) {
	assert.Equal(t, "index.test", utility.TrimFileExtension("index.test.js"))
}

func TestTrimFileExtension_PathWithExtension(t *testing.T) {
	assert.Equal(t, "/path/index", utility.TrimFileExtension("/path/index.js"))
}

func TestGetSubdirectoryRelativePath_NormalizedDirectory(t *testing.T) {
	assert.Equal(t, "ismirn0ff/documents", utility.GetSubdirectoryRelativePath("/home/ismirn0ff/documents", "/home"))
}

func TestGetSubdirectoryRelativePath_NotNormalizedDirectory(t *testing.T) {
	assert.Equal(t, "ismirn0ff/documents", utility.GetSubdirectoryRelativePath("/home/ismirn0ff/documents/", "/home/"))
}

func TestStripWalFileName_NonValidInput(t *testing.T) {
	var lsn = "---"
	var expected = strings.Repeat("Z", 24)

	result := utility.StripWalFileName(lsn)

	assert.Equal(t, expected, result)
}

func TestStripWalFileName_ValidLsn(t *testing.T) {
	var path = RandomLsn()
	result := utility.StripWalFileName(path)

	assert.Equal(t, path, result)
}

func TestLsnRegex(t *testing.T) {
	lsns := []string{RandomLsn(), RandomLsn()}
	tests := []struct {
		name        string
		lsn         string
		expected    []string
		expectedLen int
	}{
		{
			name:     "LsnRegex_ReturnLsnFromString",
			lsn:      lsns[0],
			expected: []string{lsns[0]},
		},
		{
			name:     "LsnRegex_ReturnLsnFromStringWithAnotherText",
			lsn:      fmt.Sprintf("some text %s or 43567", lsns[0]),
			expected: []string{lsns[0]},
		},
		{
			name:     "LsnRegex_ReturnEmptyArrayWhenLsnIsIncorrect",
			lsn:      GetRandomizedString(23),
			expected: nil,
		},
		{
			name:     "LsnRegex_ReturnLsnWhenItIsAllF",
			lsn:      strings.Repeat("F", 24),
			expected: []string{strings.Repeat("F", 24)},
		},
		{
			name:     "LsnRegex_ReturnAllLsnWhenHasSeparator",
			lsn:      strings.Join(lsns[:], "-"),
			expected: lsns,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result := utility.RegexpLSN.FindAllString(tt.lsn, -1)

			assert.Equalf(t, len(tt.expected), len(result), "Expected different array length")
			assert.Equalf(t, tt.expected, result, "Expected different result")
		})
	}
}

func TestStripWalFileName_ReturnFirstLsn(t *testing.T) {
	var paths = [3]string{RandomLsn(), RandomLsn(), RandomLsn()}
	var path = strings.Join(paths[:], "-")

	result := utility.StripWalFileName(path)

	assert.Equal(t, paths[0], result)
}

func RandomLsn() string {
	const LSNLength = 24
	return GetRandomizedString(LSNLength)
}

func GetRandomizedString(length int) string {
	var letter = []rune("ABCDEF0123456789")
	b := make([]rune, length)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func TestLoggedCloseWithoutError(t *testing.T) {
	defer tracelog.ErrorLogger.SetOutput(tracelog.ErrorLogger.Writer())

	var buf bytes.Buffer
	tracelog.ErrorLogger.SetOutput(&buf)

	utility.LoggedClose(&testtools.NopCloser{}, "")

	loggedData, err := io.ReadAll(&buf)
	if err != nil {
		t.Logf("failed read from pipe: %v", err)
	}

	assert.Equal(t, "", string(loggedData))
}

func TestLoggedCloseWithErrorAndDefaultMessage(t *testing.T) {
	defer tracelog.ErrorLogger.SetOutput(tracelog.ErrorLogger.Writer())
	defer tracelog.ErrorLogger.SetPrefix(tracelog.ErrorLogger.Prefix())
	defer tracelog.ErrorLogger.SetFlags(tracelog.ErrorLogger.Flags())

	var buf bytes.Buffer

	tracelog.ErrorLogger.SetPrefix("")
	tracelog.ErrorLogger.SetOutput(&buf)
	tracelog.ErrorLogger.SetFlags(0)

	utility.LoggedClose(&testtools.ErrorWriteCloser{}, "")

	loggedData, err := io.ReadAll(&buf)
	if err != nil {
		t.Logf("failed read from buffer: %v", err)
	}

	assert.Equal(t, "Problem with closing object: mock close: close error\n", string(loggedData))
}

func TestLoggedCloseWithErrorAndCustomMessage(t *testing.T) {
	defer tracelog.ErrorLogger.SetOutput(tracelog.ErrorLogger.Writer())
	defer tracelog.ErrorLogger.SetPrefix(tracelog.ErrorLogger.Prefix())
	defer tracelog.ErrorLogger.SetFlags(tracelog.ErrorLogger.Flags())

	var buf bytes.Buffer

	tracelog.ErrorLogger.SetPrefix("")
	tracelog.ErrorLogger.SetOutput(&buf)
	tracelog.ErrorLogger.SetFlags(0)

	utility.LoggedClose(&testtools.ErrorWriteCloser{}, "custom error message")

	loggedData, err := io.ReadAll(&buf)
	if err != nil {
		t.Logf("failed read from buffer: %v", err)
	}

	assert.Equal(t, "custom error message: mock close: close error\n", string(loggedData))
}
