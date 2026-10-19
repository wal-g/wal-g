package postgres

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func TestBackupAttemptRetryDoesNotOverwritePartialOrCompletedBackup(t *testing.T) {
	folder := memory.NewFolder("", memory.NewKVS())
	const base = "base_000000010000000000000002"
	completed, err := newBackupAttemptName(base)
	require.NoError(t, err)
	partial, err := newBackupAttemptName(base)
	require.NoError(t, err)
	retry, err := newBackupAttemptName(base)
	require.NoError(t, err)
	for i, name := range []string{completed, partial, retry} {
		payload := []byte{byte(i)}
		require.NoError(t, folder.PutObject(t.Context(), name+"/tar_partitions/part_001.tar", bytes.NewReader(payload)))
		if name != partial {
			require.NoError(t, folder.PutObject(t.Context(), internal.SentinelNameFromBackup(name), bytes.NewReader(payload)))
		}
	}
	for i, name := range []string{completed, partial, retry} {
		reader, err := folder.ReadObject(t.Context(), name+"/tar_partitions/part_001.tar")
		require.NoError(t, err)
		payload, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Equal(t, []byte{byte(i)}, payload)
		finished, err := folder.Exists(t.Context(), internal.SentinelNameFromBackup(name))
		require.NoError(t, err)
		require.Equal(t, name != partial, finished)
	}
}

func TestBackupAttemptNamesConcurrentSamePosition(t *testing.T) {
	const attempts = 256
	const base = "base_000000010000000000000002"
	names := make(chan string, attempts)
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, err := newBackupAttemptName(base)
			names <- name
			errs <- err
		}()
	}
	wg.Wait()
	close(names)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	seen := make(map[string]bool)
	for name := range names {
		require.Regexp(t, "^"+base+"_U_[0-9a-f]{32}$", name)
		require.False(t, seen[name], "duplicate backup attempt: %s", name)
		seen[name] = true
		require.Equal(t, strings.TrimPrefix(base, "base_"), utility.StripWalFileName(name))
	}
	require.Len(t, seen, attempts)
}

func TestDeduceBackupAttemptName(t *testing.T) {
	const wal = "000000010000000000000002"
	const parent = "000000010000000000000001"
	const id = "_U_0123456789abcdef0123456789abcdef"
	for _, legacy := range []string{"base_" + wal, "base_" + wal + "_D_" + parent, "base_" + wal + "_00000028"} {
		for _, name := range []string{legacy, legacy + id} {
			for _, suffix := range []string{"", utility.SentinelSuffix, "/tar_partitions/part_001.tar.lz4"} {
				object := storage.NewLocalObject(name+suffix, time.Time{}, 0)
				require.Equal(t, name, DeduceBackupName(object))
			}
			if strings.Contains(name, "_U_") {
				backup := Backup{SentinelDto: &BackupSentinelDto{}}
				backup.Name = name
				require.True(t, IsPgControlRequired(backup))
			}
		}
	}
}

func TestStreamingBackupAttemptNameStableAcrossParts(t *testing.T) {
	// Start assigns one identity before Upload. Advancing part numbers or the
	// finish LSN must not change it, including for the pg_control tar.
	name, err := newBackupAttemptName("base_000000010000000000000002")
	require.NoError(t, err)
	bb := &StreamingBaseBackup{backupName: name}
	first := bb.Path()
	bb.fileNo++
	bb.EndLSN = 12345
	require.NotEqual(t, first, bb.Path())
	require.True(t, strings.HasPrefix(first, name+"/"))
	require.True(t, strings.HasPrefix(bb.Path(), name+"/"))
	require.Equal(t, name, bb.BackupName())
}

func TestBackupAttemptMetadataKeepsExactDeltaParent(t *testing.T) {
	parent, err := newBackupAttemptName("base_000000010000000000000002")
	require.NoError(t, err)
	start, finish := LSN(10), LSN(20)
	meta := NewExtendedMetadataDto(false, "", time.Time{}, BackupSentinelDto{
		BackupStartLSN: &start, BackupFinishLSN: &finish, IncrementFrom: &parent,
	})
	require.Equal(t, &parent, meta.IncrementFrom)
	encoded, err := json.Marshal(BackupDetail{ExtendedMetadataDto: meta})
	require.NoError(t, err)
	var listed struct {
		Parent string `json:"delta_from"`
	}
	require.NoError(t, json.Unmarshal(encoded, &listed))
	require.Equal(t, parent, listed.Parent)
}
