package pax

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFilePath_DefaultTablespace_DataFile(t *testing.T) {
	rfn, filename, ok := ParseFilePath("/var/lib/postgresql/13/data/base/16384/16385_pax/3")
	assert.True(t, ok)
	assert.Equal(t, "3", filename)
	assert.Equal(t, uint32(16384), uint32(rfn.DBNode))
	assert.Equal(t, uint32(16385), uint32(rfn.RelNode))
}

func TestParseFilePath_DefaultTablespace_ToastFile(t *testing.T) {
	rfn, filename, ok := ParseFilePath("/data/base/16384/16385_pax/12.toast")
	assert.True(t, ok)
	assert.Equal(t, "12.toast", filename)
	assert.Equal(t, uint32(16385), uint32(rfn.RelNode))
}

func TestParseFilePath_DefaultTablespace_VisimapFile(t *testing.T) {
	rfn, filename, ok := ParseFilePath("/data/base/16384/16385_pax/12_0_2a.visimap")
	assert.True(t, ok)
	assert.Equal(t, "12_0_2a.visimap", filename)
	assert.Equal(t, uint32(16385), uint32(rfn.RelNode))
}

func TestParseFilePath_NotPaxDirectory(t *testing.T) {
	_, _, ok := ParseFilePath("/data/base/16384/16385")
	assert.False(t, ok)
}

func TestParseFilePath_PaxSuffixNoRelfilenode(t *testing.T) {
	// "_pax" is the bare suffix — there is no relfilenode prefix to parse
	_, _, ok := ParseFilePath("/data/base/16384/_pax/3")
	assert.False(t, ok)
}

func TestParseFilePath_NonNumericRelfilenode(t *testing.T) {
	_, _, ok := ParseFilePath("/data/base/16384/abc_pax/3")
	assert.False(t, ok)
}

func TestParseFilePath_NonDefaultTablespace(t *testing.T) {
	rfn, filename, ok := ParseFilePath("/data/pg_tblspc/16500/PG_13_202007201/16384/16385_pax/3")
	assert.True(t, ok)
	assert.Equal(t, "3", filename)
	assert.Equal(t, uint32(16500), uint32(rfn.SpcNode))
	assert.Equal(t, uint32(16384), uint32(rfn.DBNode))
	assert.Equal(t, uint32(16385), uint32(rfn.RelNode))
}
