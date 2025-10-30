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
	create, delete func(string) error, assertFiles func(os.FileInfo, os.FileInfo)) {

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

	assertFiles(srcFileInfo, dstFileInfo)
}

func createDir(path string) error {
	return os.MkdirAll(path, 0766)
}

func createFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	// close the handle so windows can delete
	return f.Close()
}

func TestInterpretTypeReg(t *testing.T) {
	testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeReg,

		createFile,
		os.Remove,
		func(_, dstFileInfo os.FileInfo) {
			assert.False(t, dstFileInfo.IsDir())
			assert.False(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
		},
	)

}

func TestInterpretTypeRegA(t *testing.T) {
	testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeRegA,

		createFile,
		os.Remove,
		func(_, dstFileInfo os.FileInfo) {
			assert.False(t, dstFileInfo.IsDir())
			assert.False(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
		},
	)
}

func TestInterpretTypeDir(t *testing.T) {
	testInterpret(t,
		os.TempDir(),

		"test_dir",
		tar.TypeDir,

		createDir,
		os.RemoveAll,
		func(_, dstFileInfo os.FileInfo) {
			assert.True(t, dstFileInfo.IsDir())
		},
	)

}

func TestInterpretTypeDirNested(t *testing.T) {
	dbDataDirectory := path.Join(os.TempDir(), "nested")

	testInterpret(t,
		dbDataDirectory,

		"test_dir",
		tar.TypeDir,

		createDir,
		os.RemoveAll,
		func(_, dstFileInfo os.FileInfo) {
			assert.True(t, dstFileInfo.IsDir())
		},
	)

	defer func() {
		assert.NoError(t, os.RemoveAll(dbDataDirectory))
	}()

}

func TestInterpretTypeLink(t *testing.T) {
	testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeLink,

		createFile,
		os.Remove,
		func(srcFileInfo, dstFileInfo os.FileInfo) {
			// SameFile on windows has i/o vs unix so it must be called in the test
			assert.True(t, os.SameFile(srcFileInfo, dstFileInfo))
		},
	)

}

func TestInterpretTypeSymlink(t *testing.T) {
	testInterpret(t,
		os.TempDir(),

		"test_file",
		tar.TypeSymlink,

		createFile,
		os.Remove,
		func(_, dstFileInfo os.FileInfo) {
			// Mode on Windows has i/o vs unix so it must be called within the test
			assert.True(t, dstFileInfo.Mode()&os.ModeSymlink != 0)
		},
	)
}

func TestPrepareDirsForLocalDirectory(t *testing.T) {
	err := postgres.PrepareDirs("filename", "filename")
	assert.NoError(t, err)
}
