package multistorage

import (
	"bytes"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
)

func Test_reportReadCloser(t *testing.T) {
	t.Run("wraps a reader", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		collectorMock := stats.NewMockCollector(mockCtrl)
		collectorMock.EXPECT().ReportOperationResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		rc := io.NopCloser(bytes.NewReader([]byte("hello world!")))
		rrc := newReportReadCloser(rc, collectorMock, "test")

		buf := make([]byte, 7)
		n, err := rrc.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 7, n)
		assert.Equal(t, []byte("hello w"), buf)

		buf = make([]byte, 7)
		n, err = rrc.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("orld!\000\000"), buf)

		buf = make([]byte, 0)
		n, err = rrc.Read(buf)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 0, n)
	})

	t.Run("report operation result on close", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		collectorMock := stats.NewMockCollector(mockCtrl)
		collectorMock.EXPECT().ReportOperationResult("test", stats.OperationRead(7), true).Times(1)

		rc := io.NopCloser(bytes.NewReader([]byte("hello world!")))
		rrc := newReportReadCloser(rc, collectorMock, "test")

		_, err := rrc.Read(make([]byte, 7))
		require.NoError(t, err)

		err = rrc.Close()
		require.NoError(t, err)
	})

	t.Run("report operation result on EOF", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		collectorMock := stats.NewMockCollector(mockCtrl)
		collectorMock.EXPECT().ReportOperationResult("test", stats.OperationRead(12), true).Times(1)

		rc := io.NopCloser(bytes.NewReader([]byte("hello world!")))
		rrc := newReportReadCloser(rc, collectorMock, "test")

		_, err := rrc.Read(make([]byte, 100))
		require.NoError(t, err)

		_, err = rrc.Read(make([]byte, 100))
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("report operation result on error", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		collectorMock := stats.NewMockCollector(mockCtrl)
		collectorMock.EXPECT().ReportOperationResult("test", stats.OperationRead(0), false).Times(1)

		rc := io.NopCloser(new(errorReader))
		rrc := newReportReadCloser(rc, collectorMock, "test")

		_, err := rrc.Read(make([]byte, 100))
		require.Error(t, err)
	})

	t.Run("do not report operation result twice", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		collectorMock := stats.NewMockCollector(mockCtrl)
		collectorMock.EXPECT().ReportOperationResult("test", stats.OperationRead(12), true).Times(1)

		rc := io.NopCloser(bytes.NewReader([]byte("hello world!")))
		rrc := newReportReadCloser(rc, collectorMock, "test")

		_, err := rrc.Read(make([]byte, 100))
		require.NoError(t, err)

		_, err = rrc.Read(make([]byte, 100))
		assert.ErrorIs(t, err, io.EOF)

		err = rrc.Close()
		require.NoError(t, err)
	})
}

type errorReader struct{}

func (er *errorReader) Read(_ []byte) (n int, err error) {
	return 100500, errors.New("TEST ERROR")
}
