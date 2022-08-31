package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/utility"
)

func createTempDir(prefix string) (name string, err error) {
	cwd, err := filepath.Abs("./")
	if err != nil {
		return "", err
	}

	dir, err := os.MkdirTemp(cwd, prefix)
	if err != nil {
		return "", err
	}

	return dir, nil
}

func TestIsDirectoryEmpty_ReturnsTrue_WhenDirectoryIsEmpty(t *testing.T) {
	dir, err := createTempDir("empty")
	assert.NoError(t, err)
	defer os.Remove(dir)

	actual, _ := utility.IsDirectoryEmpty(dir)

	assert.True(t, actual)
}

func TestIsDirectoryEmpty_ReturnsFalse_WhenOneFileIsInDirectory(t *testing.T) {
	dir, err := createTempDir("not_empty")
	assert.NoError(t, err)
	defer os.Remove(dir)

	file, err := os.CreateTemp(dir, "file")
	assert.NoError(t, err)
	defer os.Remove(file.Name())

	actual, _ := utility.IsDirectoryEmpty(dir)

	assert.False(t, actual)
}

func TestIsDirectoryEmpty_ReturnsFalse_WhenSeveralFilesAreInDirectory(t *testing.T) {
	dir, err := createTempDir("not_empty")
	assert.NoError(t, err)
	defer os.Remove(dir)

	for i := 0; i < 3; i++ {
		file, err := os.CreateTemp(dir, "file")
		assert.NoError(t, err)
		defer os.Remove(file.Name())
	}

	actual, _ := utility.IsDirectoryEmpty(dir)

	assert.False(t, actual)
}

func TestIsDirectoryEmpty_ReturnsFalse_WhenNestedDirectoryIsInDirectory(t *testing.T) {
	dir, err := createTempDir("not_empty")
	assert.NoError(t, err)
	defer os.Remove(dir)

	nested, err := os.MkdirTemp(dir, "nested")
	assert.NoError(t, err)
	defer os.Remove(nested)

	actual, _ := utility.IsDirectoryEmpty(dir)

	assert.False(t, actual)
}

func TestIsDirectoryEmpty_ReturnsTrue_WhenDirectoryDoesntExist(t *testing.T) {
	dir, err := createTempDir("not_existing")
	assert.NoError(t, err)

	err = os.Remove(dir)
	assert.NoError(t, err)

	actual, _ := utility.IsDirectoryEmpty(dir)

	assert.True(t, actual)
}
