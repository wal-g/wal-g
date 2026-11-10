package mongo

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	archivemocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
)

func TestPurgeGarbageWithUnreadableSentinel(t *testing.T) {
	readErr := errors.New("sentinel is unavailable")
	tests := []struct {
		name      string
		options   []PurgeOption
		retention bool
		dryRun    bool
	}{
		{name: "confirmed"},
		{name: "dry_run", dryRun: true},
		{name: "retain_count", options: []PurgeOption{PurgeRetainCount(1)}, retention: true},
		{name: "retain_after", options: []PurgeOption{PurgeRetainAfter(time.Now())}, retention: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			downloader := archivemocks.NewDownloader(t)
			purger := archivemocks.NewPurger(t)
			backupName := "binary_20261005T023101Z"
			garbage := []string{"binary_20261110T033138Z"}
			downloader.On("ListBackups", mock.Anything).
				Return([]internal.BackupTime{{BackupName: backupName}}, garbage, nil).Once()
			if tt.retention {
				downloader.On("LoadBackups", mock.Anything, []string{backupName}).Return(nil, readErr).Once()
			} else if !tt.dryRun {
				purger.On("DeleteGarbage", mock.Anything, garbage).Return(nil).Once()
			}

			options := append([]PurgeOption{PurgeGarbage(true), PurgeDryRun(tt.dryRun)}, tt.options...)
			err := HandlePurge(t.Context(), downloader, purger, options...)
			if tt.retention {
				require.ErrorIs(t, err, readErr)
			} else {
				require.NoError(t, err)
				downloader.AssertNotCalled(t, "LoadBackups", mock.Anything, mock.Anything)
			}
		})
	}
}
