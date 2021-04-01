package fsutil_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/fsutil"
)

const Filename1 = "file"
const Filename2 = "file_2"
var cleaner = fsutil.FileSystemCleaner{}

func TestGetFiles_DirectoryNotExist(t *testing.T) {
	_, err := cleaner.GetFiles("NotExist")

	assert.Error(t, err)
}

func TestGetFiles_OneFile(t *testing.T) {
	var directory = createTempFolderInCurrent(t)
	createTempFile(t, directory, Filename1)
	expected := []string{Filename1}

	result, err := cleaner.GetFiles(directory)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
	cleanup(t, directory)
}

func TestGetFiles_SeveralFiles(t *testing.T) {
	var directory = createTempFolderInCurrent(t)
	createTempFile(t, directory, Filename1)
	createTempFile(t, directory, Filename2)
	expected := []string{Filename1, Filename2}

	result, err := cleaner.GetFiles(directory)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
	cleanup(t, directory)
}

func TestGetFiles_SkippingDirectories(t *testing.T) {
	var directory = createTempFolderInCurrent(t)
	createTempFolder(t, directory)
	createTempFile(t, directory, Filename1)
	expected := []string{Filename1}

	result, err := cleaner.GetFiles(directory)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
	cleanup(t, directory)
}

func TestGetFiles_SkippingInnerFiles(t *testing.T) {
	var directory = createTempFolderInCurrent(t)
	var subdirectory = createTempFolder(t, directory)
	createTempFile(t, directory, Filename1)
	createTempFile(t, subdirectory, Filename2)
	expected := []string{Filename1}

	result, err := cleaner.GetFiles(directory)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
	cleanup(t, directory)
}

func TestGetFiles_EmptyDirectory(t *testing.T) {
	var directory = createTempFolderInCurrent(t)

	result, err := cleaner.GetFiles(directory)

	assert.NoError(t, err)
	assert.Empty(t, result)
	cleanup(t, directory)
}

func TestGetFiles_AllDirectories(t *testing.T) {
	var directory = createTempFolderInCurrent(t)
	createTempFolder(t, directory)
	createTempFolder(t, directory)

	result, err := cleaner.GetFiles(directory)

	assert.NoError(t, err)
	assert.Empty(t, result)
	cleanup(t, directory)
}

func createTempFolderInCurrent(t *testing.T) string {
	return createTempFolder(t, "./")
}

func createTempFolder(t *testing.T, path string) string {
	cwd, err := filepath.Abs(path)
	check(t, err)

	dir, err := ioutil.TempDir(cwd, "test")
	check(t, err)

	return dir
}

func createTempFile(t *testing.T, directory string, name string)  {
	err := ioutil.WriteFile(filepath.Join(directory, name), []byte{}, 0700)
	check(t, err)
}

func check(t *testing.T, err error){
	if err != nil {
		t.Log(err)
	}
}

func cleanup(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Log("temporary data directory was not deleted ", err)
	}
}
