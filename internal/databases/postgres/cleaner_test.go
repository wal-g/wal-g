package postgres_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/postgres/mocks"

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

func TestCleanupSimpleFiles(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().GetFiles(gomock.Any()).Return(inputSimpleFiles, nil).AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantSimpleDeleted))

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantSimpleDeleted,
		actualDeleted,
		"deleted wrong files",
	)
}

func TestCleanupByNotWALFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().GetFiles(gomock.Any()).Return(inputSimpleFiles, nil).AnyTimes()
	cleaner.EXPECT().Remove(gomock.Any()).Times(0)

	postgres.CleanupPrefetchDirectories(inputNotWALFile, "/A", cleaner)
}

func TestCleanupButInFilesNotWALFile(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().GetFiles(gomock.Any()).Return(append(inputSimpleFiles, inputNotWALFile), nil).AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantSimpleDeleted))

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantSimpleDeleted,
		actualDeleted,
		"deleted wrong files",
	)
}

func TestCleanupByWALWithIncorrectTimeline(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().GetFiles(gomock.Any()).Return(inputSimpleFiles, nil).AnyTimes()
	cleaner.EXPECT().Remove(gomock.Any()).Times(0)

	postgres.CleanupPrefetchDirectories(inputWALFileWithIncorrectTimeline, "/A", cleaner)
}

func TestCleanupButInFilesWALWithIncorrectTimeline(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(append(inputSimpleFiles, inputWALFileWithIncorrectTimeline), nil).AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantSimpleDeleted))

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantSimpleDeleted,
		actualDeleted,
		"deleted wrong files",
	)
}

func TestCleanupByWALWithIncorrectLogSegNoHi(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().GetFiles(gomock.Any()).Return(inputSimpleFiles, nil).AnyTimes()
	cleaner.EXPECT().Remove(gomock.Any()).Times(0)

	postgres.CleanupPrefetchDirectories(inputWALFileWithIncorrectLogSegNoHi, "/A", cleaner)
}

func TestCleanupButInFilesWALWithIncorrectLogSegNoHi(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(append(inputSimpleFiles, inputWALFileWithIncorrectLogSegNoHi), nil).
		AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantSimpleDeleted))

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantSimpleDeleted,
		actualDeleted,
		"deleted wrong files",
	)
}

func TestCleanupByWALWithIncorrectLogSegNoLo(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().GetFiles(gomock.Any()).Return(inputSimpleFiles, nil).AnyTimes()
	cleaner.EXPECT().Remove(gomock.Any()).Times(0)

	postgres.CleanupPrefetchDirectories(inputWALFileWithIncorrectLogSegNoLo, "/A", cleaner)
}

func TestCleanupButInFilesWALWithIncorrectLogSegNoLo(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(append(inputSimpleFiles, inputWALFileWithIncorrectLogSegNoLo), nil).AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantSimpleDeleted))

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantSimpleDeleted,
		actualDeleted,
		"deleted wrong files",
	)
}

func TestCleanupWithErrorOnGetFiles(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(nil, errors.New("some error")).AnyTimes()
	cleaner.EXPECT().Remove(gomock.Any()).Times(0)

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)
}

func TestCleanupFilesWithDiffInTimeline(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(inputFilesWithDiffInTimeline, nil).AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantDeletedWithDiffInTimeline))

	postgres.CleanupPrefetchDirectories(inputFileWithDiffInTimeline, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantDeletedWithDiffInTimeline,
		actualDeleted,
		"deleted wrong files",
	)
}

func TestCleanupByWALWithTooMuchLogSegNoLo(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(inputSimpleFiles, nil).AnyTimes()
	cleaner.EXPECT().Remove(gomock.Any()).Times(0)

	postgres.CleanupPrefetchDirectories(inputWALFileWithTooMuchLogSegNoLo, "/A", cleaner)
}

func TestCleanupButInFilesWALWithTooMuchLogSegNoLo(t *testing.T) {
	ctrl := gomock.NewController(t)

	cleaner := mocks.NewMockCleaner(ctrl)
	cleaner.EXPECT().
		GetFiles(gomock.Any()).
		Return(append(inputSimpleFiles, inputWALFileWithTooMuchLogSegNoLo), nil).AnyTimes()

	var actualDeleted []string
	cleaner.EXPECT().Remove(gomock.Any()).Do(func(toDelete string) {
		actualDeleted = append(actualDeleted, toDelete)
	}).Times(len(wantSimpleDeleted))

	postgres.CleanupPrefetchDirectories(inputSimpleFile, "/A", cleaner)

	assert.ElementsMatchf(
		t,
		wantSimpleDeleted,
		actualDeleted,
		"deleted wrong files",
	)
}
