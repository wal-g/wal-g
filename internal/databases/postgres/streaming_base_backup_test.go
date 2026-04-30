package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseArchiveHeader(t *testing.T) {
	tests := []struct {
		name     string
		body     []byte
		wantName string
		wantPath string
		wantErr  bool
	}{
		{
			name:     "data dir, empty path",
			body:     []byte{'b', 'a', 's', 'e', '.', 't', 'a', 'r', 0, 0},
			wantName: "base.tar",
			wantPath: "",
		},
		{
			name:     "tablespace with path",
			body:     append(append([]byte("16384.tar"), 0), append([]byte("/var/lib/pg/ts1"), 0)...),
			wantName: "16384.tar",
			wantPath: "/var/lib/pg/ts1",
		},
		{
			name:    "missing NUL after name",
			body:    []byte{'b', 'a', 's', 'e'},
			wantErr: true,
		},
		{
			name:    "missing NUL after path",
			body:    append([]byte("base.tar"), append([]byte{0}, []byte("/path-no-nul")...)...),
			wantErr: true,
		},
		{
			name:    "empty body",
			body:    []byte{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotPath, err := parseArchiveHeader(tt.body)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantName, gotName)
			assert.Equal(t, tt.wantPath, gotPath)
		})
	}
}

func TestRemapsForArchive(t *testing.T) {
	dataDir := &archive{name: "base.tar"}
	remaps, tee, err := remapsForArchive(dataDir)
	assert.NoError(t, err)
	assert.Empty(t, remaps)
	assert.Equal(t, []string{"global/pg_control"}, tee)

	tbs := &archive{name: "16384.tar", oid: 16384}
	remaps, tee, err = remapsForArchive(tbs)
	assert.NoError(t, err)
	assert.Empty(t, tee)
	assert.Len(t, remaps, 1)
	// regex remap: "" → "pg_tblspc/16384/"
	assert.Equal(t, "pg_tblspc/16384/", remaps[0].to)
}

func TestMakeArchive(t *testing.T) {
	bb := &StreamingBaseBackup{}

	a, err := bb.makeArchive("base.tar", "")
	assert.NoError(t, err)
	assert.Equal(t, "base.tar", a.name)
	assert.True(t, a.isDataDir())
	assert.Equal(t, int32(0), a.oid)

	// unknown OID rejected
	_, err = bb.makeArchive("99999.tar", "/nowhere")
	assert.Error(t, err)

	// unrecognized name rejected
	_, err = bb.makeArchive("manifest.json", "")
	assert.Error(t, err)

	// non-numeric OID rejected
	_, err = bb.makeArchive("xyz.tar", "")
	assert.Error(t, err)
}
