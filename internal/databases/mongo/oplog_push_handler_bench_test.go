package mongo

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	mongoMocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

// TODO: fix duplicates piece from fetcher_test
func SetupMongoDriverOkMock() *mongoMocks.MongoDriver {
	md := &mongoMocks.MongoDriver{}

	tsInFuture := models.OpTime{TS: models.Timestamp{TS: uint32(time.Now().Add(24 * time.Hour).Unix()), Inc: 1}}
	isMaster := models.IsMaster{
		IsMaster: true,
		LastWrite: models.IsMasterLastWrite{
			OpTime:         tsInFuture,
			MajorityOpTime: tsInFuture,
		},
	}
	md.On("IsMaster", mock.Anything).Return(isMaster, nil)
	return md
}

func buildPerfBsonFetcher(b *testing.B, bsonFname string) (stages.Fetcher, io.Closer) {
	bsonFile, err := os.Open(bsonFname)
	if err != nil {
		b.Fatalf("Can not open bson file %s: %v\n", bsonFname, err)
	}

	fetcher := stages.NewCursorMajFetcher(
		SetupMongoDriverOkMock(),
		client.NewBsonCursor(bsonFile),
		time.Microsecond,
	)

	return fetcher, bsonFile
}

func BenchmarkHandleOplogPush(b *testing.B) {
	tests := []struct {
		name             string
		bsonFname        string
		compression      compression.Compressor
		readerFrom       io.ReaderFrom
		archiveAfterSize int
		archiveAfterTime time.Duration
	}{
		{
			name:             "testdata/10_2048_oplog.bson",
			compression:      nil,
			readerFrom:       nil,
			archiveAfterSize: 16 << (10 * 2),
			archiveAfterTime: 60 * time.Second,
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				fetcher, fileCloser := buildPerfBsonFetcher(b, tc.name)
				uploader := archive.NewDiscardUploader(tc.compression, tc.readerFrom)

				membuf := stages.NewMemoryBuffer()
				applier := stages.NewStorageApplier(uploader, membuf, tc.archiveAfterSize, tc.archiveAfterTime, nil)

				err := HandleOplogPush(context.TODO(), fetcher, applier)

				assert.Nil(b, fileCloser.Close())
				assert.Nil(b, membuf.Close())
				assert.NotNil(b, err)
				assert.EqualError(b, fmt.Errorf("oplog cursor error: EOF"), err.Error())
			}
		})
	}
}
