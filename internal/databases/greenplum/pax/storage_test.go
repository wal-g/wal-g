package pax

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeFileStorageKey_DataFile(t *testing.T) {
	key := MakeFileStorageKey("abcdef0123456789", FileKey{
		SpcNode:     1663,
		DBNode:      16384,
		RelFileNode: 16385,
		Filename:    "3",
	}, "1700000000000000000")

	// Stable shape: <spc>_<db>_<md5>_<rel>_<filename>_<id>_pax
	assert.True(t, strings.HasSuffix(key, "_pax"), "key must end with _pax suffix, got %q", key)
	assert.Contains(t, key, "1663_16384_abcdef0123456789_16385_3_")
	assert.Contains(t, key, "1700000000000000000")
}

func TestMakeFileStorageKey_ToastFile(t *testing.T) {
	key := MakeFileStorageKey("md5val", FileKey{
		SpcNode: 1663, DBNode: 16384, RelFileNode: 16385, Filename: "12.toast",
	}, "id1")
	// Dot in filename is replaced with `_` so path.Ext on the storage path returns "".
	assert.Contains(t, key, "12_toast_id1")
	assert.NotContains(t, key, ".")
}

func TestMakeFileStorageKey_VisimapFile(t *testing.T) {
	key := MakeFileStorageKey("md5val", FileKey{
		SpcNode: 1663, DBNode: 16384, RelFileNode: 16385, Filename: "12_0_2a.visimap",
	}, "id1")
	// Dot in filename is replaced with `_` so path.Ext on the storage path returns "".
	assert.Contains(t, key, "12_0_2a_visimap_id1")
	assert.NotContains(t, key, ".")
}

func TestMakeFileStorageKey_DistinctPerFile(t *testing.T) {
	key1 := MakeFileStorageKey("md", FileKey{SpcNode: 1, DBNode: 1, RelFileNode: 1, Filename: "1"}, "id")
	key2 := MakeFileStorageKey("md", FileKey{SpcNode: 1, DBNode: 1, RelFileNode: 1, Filename: "2"}, "id")
	assert.NotEqual(t, key1, key2)
}

func TestMakeFileStorageKey_DistinctPerBackupID(t *testing.T) {
	key := FileKey{SpcNode: 1, DBNode: 1, RelFileNode: 1, Filename: "1"}
	a := MakeFileStorageKey("md", key, "a")
	b := MakeFileStorageKey("md", key, "b")
	assert.NotEqual(t, a, b)
}
