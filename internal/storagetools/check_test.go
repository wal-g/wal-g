package storagetools

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func TestHandleCheckRead(t *testing.T) {
	folder := memory.NewFolder("test/", memory.NewKVS())
	require.NoError(t, folder.PutObject(t.Context(), "present", bytes.NewBufferString("data")))

	require.NoError(t, HandleCheckRead(t.Context(), folder, nil))
	require.NoError(t, HandleCheckRead(t.Context(), folder, []string{"present"}))

	err := HandleCheckRead(t.Context(), folder, []string{"present", "missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "files are missing: missing")
}
