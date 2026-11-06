package postgres

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInterpretSymlinkAcrossBackupVersions restores the same symlink from two backups with different targets
// and checks that the target from the newest backup wins, for both the normal and the reverse delta unpack.
func TestInterpretSymlinkAcrossBackupVersions(t *testing.T) {
	const oldTarget, newTarget = "target_old", "target_new"
	tests := []struct {
		name          string
		reverseUnpack bool
		// order in which the backups are unpacked
		unpackOrder []string
	}{
		{name: "normal unpack: base backup first", reverseUnpack: false, unpackOrder: []string{oldTarget, newTarget}},
		{name: "reverse unpack: newest backup first", reverseUnpack: true, unpackOrder: []string{newTarget, oldTarget}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func(prev bool) { useNewUnwrapImplementation = prev }(useNewUnwrapImplementation)
			useNewUnwrapImplementation = tt.reverseUnpack

			dbDataDirectory := t.TempDir()
			tarInterpreter := &FileTarInterpreter{DBDataDirectory: dbDataDirectory}
			for _, linkTarget := range tt.unpackOrder {
				err := tarInterpreter.Interpret(&bytes.Buffer{}, &tar.Header{
					Name:     "postgresql.auto.conf",
					Linkname: linkTarget,
					Typeflag: tar.TypeSymlink,
				})
				require.NoError(t, err)
			}

			target, err := os.Readlink(filepath.Join(dbDataDirectory, "postgresql.auto.conf"))
			require.NoError(t, err)
			assert.Equal(t, newTarget, target)
		})
	}
}
