package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFSFolder(t *testing.T) {
	tmpDir := setupTmpDir(t)

	defer os.RemoveAll(tmpDir)
	var storageFolder internal.StorageFolder

	storageFolder, err := internal.ConfigureFSFolder(tmpDir)

	assert.NoError(t, err)

	testStorageFolder(storageFolder, t)
}

func setupTmpDir(t *testing.T) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	// Create temp directory.
	tmpDir, err := ioutil.TempDir(cwd, "data")
	if err != nil {
		t.Log(err)
	}
	err = os.MkdirAll(tmpDir, 0755)
	if err != nil {
		t.Log(err)
	}
	return tmpDir
}

func testStorageFolder(storageFolder internal.StorageFolder, t *testing.T) {
	sub1 := storageFolder.GetSubFolder("Sub1")

	err := storageFolder.PutObject("file0", strings.NewReader("data0"))
	assert.NoError(t, err)

	err = sub1.PutObject("file1", strings.NewReader("data1"))
	assert.NoError(t, err)

	b, err := storageFolder.Exists("file0")
	assert.NoError(t, err)
	assert.True(t, b)
	b, err = sub1.Exists("file1")
	assert.NoError(t, err)
	assert.True(t, b)

	objects, subFolders, err := storageFolder.ListFolder()
	assert.NoError(t, err)
	t.Log(subFolders[0].GetPath())
	assert.Equal(t, objects[0].GetName(), "file0")
	assert.True(t, strings.HasSuffix(subFolders[0].GetPath(), "Sub1/"))

	sublist, subFolders, err := sub1.ListFolder()
	assert.NoError(t, err)
	assert.Equal(t,len(subFolders),0)
	assert.Equal(t,len(sublist),1)

	data, err := sub1.ReadObject("file1")
	assert.NoError(t, err)
	data0Str, err := ioutil.ReadAll(data)
	assert.NoError(t, err)
	assert.Equal(t, "data1", string(data0Str))
	err = data.Close()
	assert.NoError(t, err)

	err = sub1.DeleteObjects([]string{"file1"})
	assert.NoError(t, err)
	err = storageFolder.DeleteObjects([]string{"Sub1"})
	assert.NoError(t, err)
	err = storageFolder.DeleteObjects([]string{"file0"})
	assert.NoError(t, err)


	b, err = storageFolder.Exists("file0")
	assert.NoError(t, err)
	assert.False(t, b)
	b, err = sub1.Exists("file1")
	assert.NoError(t, err)
	assert.False(t, b)
}
