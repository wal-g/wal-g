package discovery

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	archivemocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func RawDocFromTimestamp(ts models.Timestamp) ([]byte, error) {
	firstDocMeta := struct {
		TS primitive.Timestamp `bson:"ts"`
	}{
		TS: models.BsonTimestampFromOplogTS(ts),
	}
	return bson.Marshal(firstDocMeta)
}

type MongoDriverFields struct {
	client    *clientmocks.MongoDriver
	firstCur  *clientmocks.OplogCursor
	secondCur *clientmocks.OplogCursor
}

func (mdf *MongoDriverFields) AssertExpectations(t *testing.T) {
	if mdf.client != nil {
		mdf.client.AssertExpectations(t)
	}
	if mdf.firstCur != nil {
		mdf.firstCur.AssertExpectations(t)
	}
	if mdf.secondCur != nil {
		mdf.secondCur.AssertExpectations(t)
	}
}

func TestBuildCursorFromFolderTS(t *testing.T) {
	type args struct {
		ctx      context.Context
		since    models.Timestamp
		uploader *archivemocks.Uploader
		mongo    MongoDriverFields
	}
	tests := []struct {
		name          string
		args          args
		expectedSince models.Timestamp
		err           error
	}{
		{
			name: "gap_no_error",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}
				oldestTS := models.Timestamp{TS: 1579003001, Inc: 1}
				newestTS := models.Timestamp{TS: 1579005001, Inc: 1}

				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						upl := archivemocks.Uploader{}
						upl.On("UploadGapArchive",
							mock.Anything,
							reqTS,
							newestTS).
							Return(nil).Once()
						return &upl
					}(),
					mongo: func() MongoDriverFields {

						firstCurDoc, err := RawDocFromTimestamp(oldestTS)
						if err != nil {
							panic(err)
						}
						firstCur := &clientmocks.OplogCursor{}
						firstCur.On("Data").Return(firstCurDoc).Once().
							On("Next", mock.Anything).Return(true).Once().
							On("Push", firstCurDoc).Return(nil).Once()

						secondCurDoc, err := RawDocFromTimestamp(newestTS)
						if err != nil {
							panic(err)
						}
						secondCur := &clientmocks.OplogCursor{}
						secondCur.On("Data").Return(secondCurDoc).Once()

						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).Return(firstCur, nil).Once().
							On("IsMaster", mock.Anything).
							Return(models.IsMaster{LastWrite: models.IsMasterLastWrite{MajorityOpTime: models.OpTime{TS: newestTS}}}, nil).
							On("TailOplogFrom", mock.Anything, newestTS).Return(secondCur, nil).Once()
						return MongoDriverFields{
							client:    md,
							firstCur:  firstCur,
							secondCur: secondCur,
						}
					}(),
				}
			}(),
			expectedSince: models.Timestamp{TS: 1579005001, Inc: 1},
			err:           nil,
		},
		{
			name: "no_gap_no_error",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}

				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						return &archivemocks.Uploader{}
					}(),
					mongo: func() MongoDriverFields {
						firstDoc, err := RawDocFromTimestamp(reqTS)
						if err != nil {
							panic(err)
						}
						cur := &clientmocks.OplogCursor{}
						cur.On("Data").Return(firstDoc).Twice().
							On("Next", mock.Anything).Return(true).Once().
							On("Push", firstDoc).Return(nil).Once()

						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).Return(cur, nil).Once()
						return MongoDriverFields{
							client:   md,
							firstCur: cur,
						}
					}(),
				}
			}(),
			expectedSince: models.Timestamp{TS: 1579001001, Inc: 1},
			err:           nil,
		},
		{
			name: "first_TailOplogFrom_error",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}
				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						return &archivemocks.Uploader{}
					}(),
					mongo: func() MongoDriverFields {
						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).
							Return(nil, fmt.Errorf("can not create first cursor")).Once()
						return MongoDriverFields{
							client: md,
						}
					}(),
				}
			}(),
			err: fmt.Errorf("can not build oplog cursor from ts '1579001001.1': can not create first cursor"),
		},
		{
			name: "first_cursor_next_false",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}

				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						return &archivemocks.Uploader{}
					}(),
					mongo: func() MongoDriverFields {
						firstCur := &clientmocks.OplogCursor{}
						firstCur.On("Next", mock.Anything).Return(false).Once().
							On("Err").Return(fmt.Errorf("next failed")).Once()

						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).Return(firstCur, nil).Once()
						return MongoDriverFields{
							client:   md,
							firstCur: firstCur,
						}
					}(),
				}
			}(),
			err: fmt.Errorf("can not fetch first document: next failed"),
		},
		{
			name: "isMaster_error",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}
				oldestTS := models.Timestamp{TS: 1579003001, Inc: 1}

				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						return &archivemocks.Uploader{}
					}(),
					mongo: func() MongoDriverFields {
						firstCurDoc, err := RawDocFromTimestamp(oldestTS)
						if err != nil {
							panic(err)
						}
						firstCur := &clientmocks.OplogCursor{}
						firstCur.On("Data").Return(firstCurDoc).Once().
							On("Next", mock.Anything).Return(true).Once().
							On("Push", firstCurDoc).Return(nil).Once()

						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).Return(firstCur, nil).Once().
							On("IsMaster", mock.Anything).Return(models.IsMaster{}, fmt.Errorf("isMaster error"))

						return MongoDriverFields{
							client:   md,
							firstCur: firstCur,
						}
					}(),
				}
			}(),
			err: fmt.Errorf("can not fetch LastWrite.MajorityOpTime: isMaster error"),
		},
		{
			name: "UploadGapArchive_no_error",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}
				oldestTS := models.Timestamp{TS: 1579003001, Inc: 1}
				newestTS := models.Timestamp{TS: 1579005001, Inc: 1}

				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						upl := archivemocks.Uploader{}
						upl.On("UploadGapArchive",
							mock.Anything,
							reqTS,
							newestTS).
							Return(fmt.Errorf("gap upload error")).Once()
						return &upl
					}(),
					mongo: func() MongoDriverFields {
						firstCurDoc, err := RawDocFromTimestamp(oldestTS)
						if err != nil {
							panic(err)
						}
						firstCur := &clientmocks.OplogCursor{}
						firstCur.On("Data").Return(firstCurDoc).Once().
							On("Next", mock.Anything).Return(true).Once().
							On("Push", firstCurDoc).Return(nil).Once()

						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).Return(firstCur, nil).Once().
							On("IsMaster", mock.Anything).
							Return(models.IsMaster{LastWrite: models.IsMasterLastWrite{MajorityOpTime: models.OpTime{TS: newestTS}}}, nil)
						return MongoDriverFields{
							client:   md,
							firstCur: firstCur,
						}
					}(),
				}
			}(),
			err: fmt.Errorf("gap upload error"),
		},
		{
			name: "second_TailOplogFrom_error_error",
			args: func() args {
				reqTS := models.Timestamp{TS: 1579001001, Inc: 1}
				oldestTS := models.Timestamp{TS: 1579003001, Inc: 1}
				newestTS := models.Timestamp{TS: 1579005001, Inc: 1}

				return args{
					ctx:   context.TODO(),
					since: reqTS,
					uploader: func() *archivemocks.Uploader {
						upl := archivemocks.Uploader{}
						upl.On("UploadGapArchive",
							mock.Anything,
							reqTS,
							newestTS).
							Return(nil).Once()
						return &upl
					}(),
					mongo: func() MongoDriverFields {

						firstCurDoc, err := RawDocFromTimestamp(oldestTS)
						if err != nil {
							panic(err)
						}
						firstCur := &clientmocks.OplogCursor{}
						firstCur.On("Data").Return(firstCurDoc).Once().
							On("Next", mock.Anything).Return(true).Once().
							On("Push", firstCurDoc).Return(nil).Once()

						md := &clientmocks.MongoDriver{}
						md.On("TailOplogFrom", mock.Anything, reqTS).Return(firstCur, nil).Once().
							On("IsMaster", mock.Anything).
							Return(models.IsMaster{LastWrite: models.IsMasterLastWrite{MajorityOpTime: models.OpTime{TS: newestTS}}}, nil).
							On("TailOplogFrom", mock.Anything, newestTS).
							Return(nil, fmt.Errorf("can not create second cursor")).Once()
						return MongoDriverFields{
							client:   md,
							firstCur: firstCur,
						}
					}(),
				}
			}(),
			err: fmt.Errorf("can not build oplog cursor from ts '1579005001.1': can not create second cursor"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCur, gotSince, err := BuildCursorFromTS(tc.args.ctx, tc.args.since, tc.args.uploader, tc.args.mongo.client)
			if tc.err != nil {
				assert.EqualError(t, err, tc.err.Error())
				tc.args.uploader.AssertExpectations(t)
				tc.args.mongo.AssertExpectations(t)
				return
			}
			assert.Nil(t, err)

			assert.Equal(t, tc.expectedSince, gotSince)

			gotOp, err := models.OplogFromRaw(gotCur.Data())
			assert.Nil(t, err)

			expectedRaw, err := RawDocFromTimestamp(tc.expectedSince)
			assert.Nil(t, err)
			expectedDoc, err := models.OplogFromRaw(expectedRaw)

			assert.Equal(t, expectedDoc, gotOp)
			assert.Equal(t, tc.expectedSince, gotSince)

			tc.args.uploader.AssertExpectations(t)
			tc.args.mongo.AssertExpectations(t)
		})
	}
}

func TestResolveStartingTS(t *testing.T) {
	type args struct {
		ctx         context.Context
		downloader  *archivemocks.Downloader
		mongoClient *clientmocks.MongoDriver
	}
	tests := []struct {
		name       string
		args       args
		expectedTS models.Timestamp
		err        error
	}{
		{
			name: "last_storage_ts_fetched,_no_error",
			args: func() args {
				return args{
					ctx: context.TODO(),
					downloader: func() *archivemocks.Downloader {
						dl := &archivemocks.Downloader{}
						dl.On("LastKnownArchiveTS").Return(models.Timestamp{TS: 1579002001, Inc: 1}, nil).Once()
						return dl
					}(),
					mongoClient: &clientmocks.MongoDriver{},
				}
			}(),
			expectedTS: models.Timestamp{TS: 1579002001, Inc: 1},
		},
		{
			name: "last_storage_ts_fetch_error",
			args: func() args {
				return args{
					ctx: context.TODO(),
					downloader: func() *archivemocks.Downloader {
						dl := &archivemocks.Downloader{}
						dl.On("LastKnownArchiveTS").Return(models.Timestamp{}, fmt.Errorf("ts fetch failed")).Once()
						return dl
					}(),
					mongoClient: &clientmocks.MongoDriver{},
				}
			}(),
			err: fmt.Errorf("can not fetch last-known storage timestamp: ts fetch failed"),
		},
		{
			name: "initial_last_maj_ts_fetched,_no_error",
			args: func() args {
				return args{
					ctx: context.TODO(),
					downloader: func() *archivemocks.Downloader {
						dl := &archivemocks.Downloader{}
						dl.On("LastKnownArchiveTS").Return(models.Timestamp{}, nil).Once()
						return dl
					}(),
					mongoClient: func() *clientmocks.MongoDriver {
						md := &clientmocks.MongoDriver{}
						md.On("IsMaster", mock.Anything).
							Return(models.IsMaster{LastWrite: models.IsMasterLastWrite{MajorityOpTime: models.OpTime{TS: models.Timestamp{TS: 1579004001, Inc: 1}}}}, nil).
							Once()
						return md
					}(),
				}
			}(),
			expectedTS: models.Timestamp{TS: 1579004001, Inc: 1},
		},
		{
			name: "initial_last_maj_ts_fetch_error",
			args: func() args {
				return args{
					ctx: context.TODO(),
					downloader: func() *archivemocks.Downloader {
						dl := &archivemocks.Downloader{}
						dl.On("LastKnownArchiveTS").Return(models.Timestamp{}, nil).Once()
						return dl
					}(),
					mongoClient: func() *clientmocks.MongoDriver {
						md := &clientmocks.MongoDriver{}
						md.On("IsMaster", mock.Anything).
							Return(models.IsMaster{}, fmt.Errorf("is master error")).Once()
						return md
					}(),
				}
			}(),
			err: fmt.Errorf("can not fetch LastWrite.MajorityOpTime: is master error"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.args.downloader.AssertExpectations(t)
			defer tc.args.mongoClient.AssertExpectations(t)

			ts, err := ResolveStartingTS(tc.args.ctx, tc.args.downloader, tc.args.mongoClient)
			if tc.err != nil {
				assert.EqualError(t, err, tc.err.Error())
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedTS, ts)
		})
	}
}
