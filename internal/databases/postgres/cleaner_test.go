package postgres_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
)

var (
	inputSimpleFiles = []string{
		"000000010000000100000056",
		"000000010000000100000057",
		"000000010000000100000058",
		"000000010000000100000059",
		"00000001000000010000005A",
	}
	inputSimpleFile   = "000000010000000100000058"
	wantSimpleDeleted = []string{
		"/A/.wal-g/prefetch/000000010000000100000056",
		"/A/.wal-g/prefetch/000000010000000100000057",
		"/A/.wal-g/prefetch/running/000000010000000100000056",
		"/A/.wal-g/prefetch/running/000000010000000100000057",
	}

	inputNotWALFile = "228"

	inputWALFileWithIncorrectTimeline = "Z00000010000000100000058"

	inputWALFileWithIncorrectLogSegNoHi = "00000001Z000000100000056"

	inputWALFileWithIncorrectLogSegNoLo = "0000000100000001Z0000056"

	inputFilesWithDiffInTimeline = []string{
		"000000010000000100000088",
		"000000020000000100000088",
		"000000030000000100000088",
		"000000040000000100000088",
		"000000050000000100000088",
	}
	inputFileWithDiffInTimeline   = "000000030000000100000088"
	wantDeletedWithDiffInTimeline = []string{
		"/A/.wal-g/prefetch/000000010000000100000088",
		"/A/.wal-g/prefetch/000000020000000100000088",
		"/A/.wal-g/prefetch/running/000000010000000100000088",
		"/A/.wal-g/prefetch/running/000000020000000100000088",
	}

	inputWALFileWithTooMuchLogSegNoLo = "0000000100000001FFFFFFFF"
)

type MockCleaner struct {
	files   []string
	deleted []string
	err     error
}

func (cl *MockCleaner) GetFiles(directory string) (files []string, err error) {
	return cl.files, cl.err
}

func (cl *MockCleaner) Remove(file string) {
	cl.deleted = append(cl.deleted, file)
}

func (cl *MockCleaner) setFilesAndErrorAndClearDeleted(newFiles []string, err error) {
	cl.files = newFiles
	cl.err = err
	cl.deleted = []string{}
}

func TestCleanupSimpleFiles(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputSimpleFiles, nil)
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, len(wantSimpleDeleted), len(cleaner.deleted))
	for _, delFile := range wantSimpleDeleted {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}

func TestCleanupByNotWALFile(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputSimpleFiles, nil)
	postgres.CleanupPrefetchDirectories(inputNotWALFile, "/A", &cleaner)

	assert.Equal(t, 0, len(cleaner.deleted))
}

func TestCleanupButInFilesNotWALFile(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(append(inputSimpleFiles, inputNotWALFile), nil)
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, len(wantSimpleDeleted), len(cleaner.deleted))
	for _, delFile := range wantSimpleDeleted {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}

func TestCleanupByWALWithIncorrectTimeline(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputSimpleFiles, nil)
	postgres.CleanupPrefetchDirectories(inputWALFileWithIncorrectTimeline, "/A", &cleaner)

	assert.Equal(t, 0, len(cleaner.deleted))
}

func TestCleanupButInFilesWALWithIncorrectTimeline(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(append(inputSimpleFiles, inputWALFileWithIncorrectTimeline), nil)
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, len(wantSimpleDeleted), len(cleaner.deleted))
	for _, delFile := range wantSimpleDeleted {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}

func TestCleanupByWALWithIncorrectLogSegNoHi(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputSimpleFiles, nil)
	postgres.CleanupPrefetchDirectories(inputWALFileWithIncorrectLogSegNoHi, "/A", &cleaner)

	assert.Equal(t, 0, len(cleaner.deleted))
}

func TestCleanupButInFilesWALWithIncorrectLogSegNoHi(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(append(inputSimpleFiles, inputWALFileWithIncorrectLogSegNoHi), nil)
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, len(wantSimpleDeleted), len(cleaner.deleted))
	for _, delFile := range wantSimpleDeleted {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}

func TestCleanupByWALWithIncorrectLogSegNoLo(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputSimpleFiles, nil)
	postgres.CleanupPrefetchDirectories(inputWALFileWithIncorrectLogSegNoLo, "/A", &cleaner)

	assert.Equal(t, 0, len(cleaner.deleted))
}

func TestCleanupButInFilesWALWithIncorrectLogSegNoLo(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(append(inputSimpleFiles, inputWALFileWithIncorrectLogSegNoLo), nil)
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, len(wantSimpleDeleted), len(cleaner.deleted))
	for _, delFile := range wantSimpleDeleted {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}

func TestCleanupWithErrorOnGetFiles(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(nil, errors.New("some error"))
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, 0, len(cleaner.deleted))
}

func TestCleanupFilesWithDiffInTimeline(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputFilesWithDiffInTimeline, nil)
	postgres.CleanupPrefetchDirectories(inputFileWithDiffInTimeline, "/A", &cleaner)

	assert.Equal(t, len(wantDeletedWithDiffInTimeline), len(cleaner.deleted))
	for _, delFile := range wantDeletedWithDiffInTimeline {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}

func TestCleanupByWALWithTooMuchLogSegNoLo(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(inputSimpleFiles, nil)
	postgres.CleanupPrefetchDirectories(inputWALFileWithTooMuchLogSegNoLo, "/A", &cleaner)

	assert.Equal(t, 0, len(cleaner.deleted))
}

func TestCleanupButInFilesWALWithTooMuchLogSegNoLo(t *testing.T) {
	cleaner := MockCleaner{}
	cleaner.setFilesAndErrorAndClearDeleted(append(inputSimpleFiles, inputWALFileWithTooMuchLogSegNoLo), nil)
	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", &cleaner)

	assert.Equal(t, len(wantSimpleDeleted), len(cleaner.deleted))
	for _, delFile := range wantSimpleDeleted {
		assert.Contains(t, cleaner.deleted, delFile)
	}
}
