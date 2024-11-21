package postgres_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
)

func testInterpret(t *testing.T,
	dbDataDirectory, name string, typeflag byte,
	create, delete func(string) error) (os.FileInfo, os.FileInfo) {

	tarInterpreter := &postgres.FileTarInterpreter{
		DBDataDirectory: dbDataDirectory,
	}

	err := create(name)

	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, delete(name))
	}()

	err = tarInterpreter.Interpret(
		&bytes.Buffer{},
		&tar.Header{
			Name:     name,
			Typeflag: typeflag,
		},
	)

	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, delete(path.Join(dbDataDirectory, name)))
	}()

	srcFileInfo, err := os.Lstat(name)

	assert.NoError(t, err)

	dstFileInfo, err := os.Lstat(path.Join(dbDataDirectory, name))

	assert.NoError(t, err)

	return srcFileInfo, dstFileInfo
}

func createDir(path string) error {
	return os.MkdirAll(path, 0766)
}

func createFile(path string) error {
	_, err := os.Create(path)

	return err
}

func TestInterpretTypeReg(t *testing.T) {
	_, dstFileInfo := testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeReg,

		createFile,
		os.Remove,
	)

	assert.False(t, dstFileInfo.IsDir())
	assert.False(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
}

func TestInterpretTypeRegA(t *testing.T) {
	_, dstFileInfo := testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeRegA,

		createFile,
		os.Remove,
	)

	assert.False(t, dstFileInfo.IsDir())
	assert.False(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
}

func TestInterpretTypeDir(t *testing.T) {
	_, dstFileInfo := testInterpret(t,
		os.TempDir(),

		"test_dir",
		tar.TypeDir,

		createDir,
		os.RemoveAll,
	)

	assert.True(t, dstFileInfo.IsDir())
}

func TestInterpretTypeDirNested(t *testing.T) {
	dbDataDirectory := path.Join(os.TempDir(), "nested")

	_, dstFileInfo := testInterpret(t,
		dbDataDirectory,

		"test_dir",
		tar.TypeDir,

		createDir,
		os.RemoveAll,
	)

	defer func() {
		assert.NoError(t, os.RemoveAll(dbDataDirectory))
	}()

	assert.True(t, dstFileInfo.IsDir())
}

func TestInterpretTypeLink(t *testing.T) {
	srcFileInfo, dstFileInfo := testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeLink,

		createFile,
		os.Remove,
	)

	assert.True(t, os.SameFile(srcFileInfo, dstFileInfo))
}

func TestInterpretTypeSymlink(t *testing.T) {
	_, dstFileInfo := testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeSymlink,

		createFile,
		os.Remove,
	)

	assert.True(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
}

func TestMultiInterpretTypeSymlink(t *testing.T) {
	fileName := "test_file"
	dbDataDirectory := os.TempDir()
	createFile(fileName)
	defer os.Remove(fileName)
	// Since we pass a noop for delete, we need to remove outside testInterpret
	defer os.Remove(path.Join(dbDataDirectory, fileName))
	for i := 1; i < 5; i++ {
		_, dstFileInfo := testInterpret(t,
			os.TempDir(),

			"test_file",
			tar.TypeSymlink,
			func(s string) error {
				return nil
			},
			func(s string) error {
				return nil
			},
		)
		assert.True(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
	}

}

func TestPrepareDirsForLocalDirectory(t *testing.T) {
	err := postgres.PrepareDirs("filename", "filename")
	assert.NoError(t, err)
}
