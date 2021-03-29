package fsutil_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/fsutil"
)

func TestGetFiles_DirectoryNotExist(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}

	_, err := cleaner.GetFiles("test_data/NotExist")

	assert.Error(t, err)
}

func TestGetFiles_OneFile(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}
	expected := []string{"file.txt"}

	result, err := cleaner.GetFiles("test_data/directory_with_one_file")

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetFiles_SeveralFiles(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}
	expected := []string{"file_1.txt", "file_2"}

	result, err := cleaner.GetFiles("test_data/directory_with_several_files")

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetFiles_SkippingDirectories(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}
	expected := []string{"file.txt"}

	result, err := cleaner.GetFiles("test_data/directory_with_one_file_and_directory")

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetFiles_SkippingInnerFiles(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}
	expected := []string{"file.txt"}

	result, err := cleaner.GetFiles("test_data/directory_with_one_file_and_directory_with_file")

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetFiles_EmptyDirectory(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}

	result, err := cleaner.GetFiles("test_data/empty_directory")

	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetFiles_AllDirectories(t *testing.T) {
	var cleaner = fsutil.FileSystemCleaner{}

	result, err := cleaner.GetFiles("test_data")

	assert.NoError(t, err)
	assert.Empty(t, result)
}