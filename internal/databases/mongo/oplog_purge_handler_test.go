package mongo

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/wal-g/wal-g/internal"
	mocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

var (
	BackupTimes = make([]internal.BackupTime, 4)
	Backups     = []models.Backup{
		{
			StartLocalTime: time.Unix(800, 0), FinishLocalTime: time.Unix(900, 0),
			MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 800}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 900}}},
		},
		{
			StartLocalTime: time.Unix(600, 0), FinishLocalTime: time.Unix(700, 0),
			MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 600}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 700}}},
		},
		{
			StartLocalTime: time.Unix(300, 0), FinishLocalTime: time.Unix(400, 0),
			MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 300}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 400}}},
		},
	}
	Archives = []models.Archive{
		{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
		{Start: models.Timestamp{TS: 200}, End: models.Timestamp{TS: 500}},
		{Start: models.Timestamp{TS: 500}, End: models.Timestamp{TS: 550}},
		{Start: models.Timestamp{TS: 550}, End: models.Timestamp{TS: 910}},
		{Start: models.Timestamp{TS: 910}, End: models.Timestamp{TS: 950}},
	}
)

func TimePtr(t time.Time) *time.Time {
	return &t
}

func TestHandleOplogPurge(t *testing.T) {
	type args struct {
		downloader  *mocks.Downloader
		purger      *mocks.Purger
		retainAfter *time.Time
		dryRun      bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "purge",
			args: args{
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("ListOplogArchives").Return(Archives, nil).Once().
						On("ListBackups").Return(make([]internal.BackupTime, 4), []string{}, nil).Once().
						On("LoadBackups",
							mock.MatchedBy(func(backupNames []string) bool { return len(backupNames) == 4 })).
						Return(Backups, nil).Once()
					return dl
				}(),
				purger: func() *mocks.Purger {
					pr := &mocks.Purger{}
					pr.On("DeleteOplogArchives", mock.MatchedBy(func(archives []models.Archive) bool {
						return reflect.DeepEqual(archives,
							[]models.Archive{
								{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
								{Start: models.Timestamp{TS: 500}, End: models.Timestamp{TS: 550}}})
					})).Return(nil).Once()
					return pr
				}(),
				retainAfter: TimePtr(time.Unix(650, 0)),
				dryRun:      false,
			},
			wantErr: nil,
		},
		{
			name: "purge_dry_run",
			args: args{
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("ListOplogArchives").Return(Archives, nil).Once().
						On("ListBackups").Return(make([]internal.BackupTime, 4), []string{}, nil).Once().
						On("LoadBackups",
							mock.MatchedBy(func(backupNames []string) bool { return len(backupNames) == 4 })).
						Return(Backups, nil).Once()
					return dl
				}(),
				purger:      &mocks.Purger{},
				retainAfter: TimePtr(time.Unix(650, 0)),
				dryRun:      true,
			},
			wantErr: nil,
		},
		{
			name: "list_oplogs_error",
			args: args{
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("ListOplogArchives").Return(nil, fmt.Errorf("listing error")).Once()
					return dl
				}(),
				purger:      &mocks.Purger{},
				retainAfter: TimePtr(time.Unix(650, 0)),
				dryRun:      true,
			},
			wantErr: fmt.Errorf("can not load oplog archives: listing error"),
		},
		{
			name: "list_backup_times_error",
			args: args{
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("ListOplogArchives").Return(Archives, nil).Once().
						On("ListBackups").Return(nil, []string{}, fmt.Errorf("listing backup times failed")).Once()
					return dl
				}(),
				purger:      &mocks.Purger{},
				retainAfter: TimePtr(time.Unix(650, 0)),
				dryRun:      true,
			},
			wantErr: fmt.Errorf("can not load backups: listing backup times failed"),
		},
		{
			name: "load_backup_error",
			args: args{
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("ListOplogArchives").Return(Archives, nil).Once().
						On("ListBackups").Return(make([]internal.BackupTime, 4), []string{}, nil).Once().
						On("LoadBackups",
							mock.MatchedBy(func(backupNames []string) bool { return len(backupNames) == 4 })).
						Return(nil, fmt.Errorf("backup loading failed")).Once()
					return dl
				}(),
				purger:      &mocks.Purger{},
				retainAfter: TimePtr(time.Unix(650, 0)),
				dryRun:      true,
			},
			wantErr: fmt.Errorf("can not load backups: backup loading failed"),
		},
		{
			name: "no_backups_error",
			args: args{
				downloader: func() *mocks.Downloader {
					dl := &mocks.Downloader{}
					dl.On("ListOplogArchives").Return(Archives, nil).Once().
						On("ListBackups").Return([]internal.BackupTime{}, []string{}, nil).Once().
						On("LoadBackups",
							mock.MatchedBy(func(backupNames []string) bool { return len(backupNames) == 0 })).
						Return([]models.Backup{}, nil).Once()
					return dl
				}(),
				purger:      &mocks.Purger{},
				retainAfter: TimePtr(time.Unix(650, 0)),
				dryRun:      true,
			},
			wantErr: fmt.Errorf("can not find any existed backups"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := HandleOplogPurge(tt.args.downloader, tt.args.purger, tt.args.retainAfter, tt.args.dryRun)
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
