package postgres

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestClassifyDataDirectory_Empty(t *testing.T) {
	dir := t.TempDir()
	assert.Equal(t, pgDataStateEmpty, classifyDataDirectory(dir))
}

func TestClassifyDataDirectory_Normal(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "global"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "global", "pg_control"), []byte("fake"), 0600))
	assert.Equal(t, pgDataStateNormal, classifyDataDirectory(dir))
}

func TestClassifyDataDirectory_Corrupt(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "PG_VERSION"), []byte("14\n"), 0600))
	assert.Equal(t, pgDataStateCorrupt, classifyDataDirectory(dir))
}

func TestClassifyDataDirectory_Interrupted(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "global"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "global", "pg_control.catchup"), []byte{}, 0600))
	assert.Equal(t, pgDataStateInterrupted, classifyDataDirectory(dir))
}

func TestClassifyDataDirectory_NormalBeatsInterrupted(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "global"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "global", "pg_control"), []byte("fake"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "global", "pg_control.catchup"), []byte("old"), 0600))
	assert.Equal(t, pgDataStateNormal, classifyDataDirectory(dir))
}

func TestSendControlAndFileList_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	sendControlAndFileList(dir, enc)

	dec := gob.NewDecoder(&buf)

	var ctrl PgControlData
	require.NoError(t, dec.Decode(&ctrl))
	assert.Equal(t, uint64(0), ctrl.SystemIdentifier,
		"empty directory must signal full-copy via zero SystemIdentifier")

	var fl internal.BackupFileList
	require.NoError(t, dec.Decode(&fl))
	assert.Empty(t, fl, "empty directory must send an empty file list")
}
