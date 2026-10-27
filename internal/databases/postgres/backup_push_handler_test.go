package postgres

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func journalInfoExists(t *testing.T, folder storage.Folder, backupName string) bool {
	t.Helper()
	exists, err := folder.GetSubFolder(utility.BaseBackupPath).Exists(t.Context(), internal.JournalPrefix+backupName)
	require.NoError(t, err)
	return exists
}

func TestHandleJournalInfo(t *testing.T) {
	// handleJournalInfo stamps CurrentBackupEnd using real wall-clock time
	// (utility.TimeNowCrossPlatformUTC), so the folder's LastModified clock must be real too.
	newFolder := func() storage.Folder {
		return memory.NewFolder("", memory.NewKVS())
	}

	t.Run("does nothing when count journals is disabled", func(t *testing.T) {
		folder := newFolder()

		bh := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000001"},
			Arguments:     BackupArguments{countJournals: false},
		}
		bh.handleJournalInfo(t.Context(), folder)

		assert.False(t, journalInfoExists(t, folder, bh.CurBackupInfo.Name))
	})

	t.Run("does nothing for permanent backups", func(t *testing.T) {
		folder := newFolder()

		bh := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000001"},
			Arguments:     BackupArguments{countJournals: true, isPermanent: true},
		}
		bh.handleJournalInfo(t.Context(), folder)

		assert.False(t, journalInfoExists(t, folder, bh.CurBackupInfo.Name))
	})

	t.Run("computes WAL volume accumulated between two backups", func(t *testing.T) {
		folder := newFolder()

		bh1 := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000001"},
			Arguments:     BackupArguments{countJournals: true},
		}
		bh1.handleJournalInfo(t.Context(), folder)
		require.True(t, journalInfoExists(t, folder, bh1.CurBackupInfo.Name))

		// WAL segments archived between the two backups; storage object size is what
		// should end up counted (i.e. compressed size, not any uncompressed estimate).
		time.Sleep(time.Millisecond)
		err := folder.GetSubFolder(utility.WalPath).PutObject(
			t.Context(), "000000010000000000000001.lz4", bytes.NewBufferString(strings.Repeat("a", 5)))
		require.NoError(t, err)

		time.Sleep(time.Millisecond)
		err = folder.GetSubFolder(utility.WalPath).PutObject(
			t.Context(), "000000010000000000000002.lz4", bytes.NewBufferString(strings.Repeat("a", 10)))
		require.NoError(t, err)

		time.Sleep(time.Millisecond)
		bh2 := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000003"},
			Arguments:     BackupArguments{countJournals: true},
		}
		bh2.handleJournalInfo(t.Context(), folder)

		ji1, err := internal.NewJournalInfo(t.Context(), bh1.CurBackupInfo.Name, folder, utility.WalPath)
		require.NoError(t, err)
		assert.Equal(t, int64(15), ji1.SizeToNextBackup)

		ji2, err := internal.NewJournalInfo(t.Context(), bh2.CurBackupInfo.Name, folder, utility.WalPath)
		require.NoError(t, err)
		assert.Equal(t, int64(0), ji2.SizeToNextBackup)
	})
}

func TestWaitForWALArchive(t *testing.T) {
	const timeline = uint32(3)
	firstSegment := WalSegmentNo(10)
	lastSegment := WalSegmentNo(12)

	newHandler := func(folder storage.Folder) *BackupHandler {
		return &BackupHandler{
			CurBackupInfo: CurBackupInfo{
				Name:     "base_" + firstSegment.GetFilename(timeline),
				startLSN: firstSegment.firstLsn() + 1,
				endLSN:   lastSegment.firstLsn() + 1,
			},
			Arguments: BackupArguments{
				Uploader:          internal.NewRegularUploader(lz4.Compressor{}, folder),
				waitForWALArchive: true,
			},
		}
	}

	t.Run("accepts every registered compression format and bare WAL files", func(t *testing.T) {
		extensions := []string{""}
		for _, decompressor := range compression.Decompressors {
			extensions = append(extensions, decompressor.FileExtension())
		}
		for _, extension := range extensions {
			t.Run(extension, func(t *testing.T) {
				folder := memory.NewFolder("", memory.NewKVS())
				walFolder := folder.GetSubFolder(utility.WalPath)
				for segment := firstSegment; segment <= lastSegment; segment++ {
					name := utility.AddFileExtension(segment.GetFilename(timeline), extension)
					require.NoError(t, walFolder.PutObject(t.Context(), name, strings.NewReader("WAL")))
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				require.NoError(t, newHandler(folder).waitForWALArchive(ctx, folder))
			})
		}
	})

	t.Run("tries the previous WAL extension first and adapts to format changes", func(t *testing.T) {
		folder := &archiveWaitRecordingFolder{
			Folder: memory.NewFolder("", memory.NewKVS()),
			calls:  &[]string{},
		}
		// Use formats registered on both Unix and Windows; gzip is Unix-only.
		extensions := []string{"lzma", "lzma", "lz4", "lz4", "", ""}
		walFolder := folder.GetSubFolder(utility.WalPath)
		for i, extension := range extensions {
			name := utility.AddFileExtension((firstSegment + WalSegmentNo(i)).GetFilename(timeline), extension)
			require.NoError(t, walFolder.PutObject(t.Context(), name, strings.NewReader("WAL")))
		}
		handler := newHandler(folder)
		handler.CurBackupInfo.endLSN = (firstSegment + WalSegmentNo(len(extensions)-1)).firstLsn() + 1
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.NoError(t, handler.waitForWALArchive(ctx, folder))
		require.Equal(t, firstSegment.GetFilename(timeline)+".lz4", (*folder.calls)[0])
		for i := 1; i < len(extensions); i++ {
			filename := (firstSegment + WalSegmentNo(i)).GetFilename(timeline)
			var calls []string
			for _, name := range *folder.calls {
				if strings.HasPrefix(name, filename) {
					calls = append(calls, name)
				}
			}
			require.NotEmpty(t, calls)
			require.Equal(t, utility.AddFileExtension(filename, extensions[i-1]), calls[0])
			if extensions[i] == extensions[i-1] {
				require.Len(t, calls, 1)
			}
		}
	})

	t.Run("returns storage errors without trying other formats", func(t *testing.T) {
		storageErr := errors.New("storage unavailable")
		folder := &archiveWaitRecordingFolder{
			Folder: memory.NewFolder("", memory.NewKVS()),
			calls:  &[]string{},
			err:    storageErr,
		}
		require.ErrorIs(t, newHandler(folder).waitForWALArchive(t.Context(), folder), storageErr)
		require.Len(t, *folder.calls, 1)
	})

	t.Run("wait is canceled while a required WAL segment is missing", func(t *testing.T) {
		folder := &archiveWaitRecordingFolder{
			Folder: memory.NewFolder("", memory.NewKVS()),
			calls:  &[]string{},
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := newHandler(folder).waitForWALArchive(ctx, folder)
		require.ErrorIs(t, err, context.Canceled)
		// The memory folder ignores cancellation, so a missing segment exercises
		// every candidate before the wait observes the canceled context.
		filename := firstSegment.GetFilename(timeline)
		expected := map[string]bool{filename: true}
		for _, decompressor := range compression.Decompressors {
			expected[utility.AddFileExtension(filename, decompressor.FileExtension())] = true
		}
		require.Len(t, *folder.calls, len(expected))
		for _, name := range *folder.calls {
			require.True(t, expected[name], "unexpected or duplicate lookup: %s", name)
			delete(expected, name)
		}
	})

	t.Run("does nothing when the option is disabled", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewKVS())
		handler := newHandler(folder)
		handler.Arguments.waitForWALArchive = false

		require.NoError(t, handler.waitForWALArchive(t.Context(), folder))
	})
}

type archiveWaitRecordingFolder struct {
	storage.Folder
	calls *[]string
	err   error
}

func (f *archiveWaitRecordingFolder) GetSubFolder(path string) storage.Folder {
	return &archiveWaitRecordingFolder{Folder: f.Folder.GetSubFolder(path), calls: f.calls, err: f.err}
}

func (f *archiveWaitRecordingFolder) StatObject(ctx context.Context, name string) (storage.Object, error) {
	*f.calls = append(*f.calls, name)
	if f.err != nil {
		return nil, f.err
	}
	return f.Folder.StatObject(ctx, name)
}
