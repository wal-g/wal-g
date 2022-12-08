package mongo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	mocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func TestHandleBackupDelete(t *testing.T) {
	type args struct {
		backupName string
		downloader *mocks.Downloader
		purger     *mocks.Purger
		dryRun     bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "delete_first",
			args: args{
				backupName: "first",
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("BackupMeta", mock.MatchedBy(func(backupName string) bool { return backupName == "first" })).
						Return(&models.Backup{BackupName: "first"}, nil).Once()
					return dl
				}(),
				purger: func() *mocks.Purger {
					pr := &mocks.Purger{}
					pr.On("DeleteBackups", mock.MatchedBy(func(backups []*models.Backup) bool { return len(backups) == 1 && backups[0].BackupName == "first" })).
						Return(nil).Once()
					return pr
				}(),
				dryRun: false,
			},
			wantErr: nil,
		},
		{
			name: "delete_first_dryrun",
			args: args{
				backupName: "first",
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("BackupMeta", mock.MatchedBy(func(backupName string) bool { return backupName == "first" })).
						Return(&models.Backup{BackupName: "first"}, nil).Once()
					return dl
				}(),
				purger: &mocks.Purger{},
				dryRun: true,
			},
			wantErr: nil,
		},
		{
			name: "delete_nonexistent",
			args: args{
				backupName: "nonexistent",
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("BackupMeta", mock.MatchedBy(func(backupName string) bool { return backupName == "nonexistent" })).
						Return(nil, fmt.Errorf("can not fetch stream sentinel: test")).Once()
					return dl
				}(),
				purger: &mocks.Purger{},
				dryRun: false,
			},
			wantErr: fmt.Errorf("can not fetch stream sentinel: test"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := HandleBackupDelete(tt.args.backupName, tt.args.downloader, tt.args.purger, tt.args.dryRun)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
				return
			}
			assert.Nil(t, err)
			tt.args.downloader.AssertExpectations(t)
			tt.args.purger.AssertExpectations(t)
		})
	}
}
