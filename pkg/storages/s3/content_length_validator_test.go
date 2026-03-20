package s3_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/s3"
)

type ErrOnCloseCloser struct{ io.Reader }

func (ErrOnCloseCloser) Close() error { return errors.New("close error") }

func TestContentLengthValidator_PassThrough(t *testing.T) {
	body := io.NopCloser(strings.NewReader("hello"))
	result := s3.NewContentLengthValidator(body, 0, "bucket/key")
	assert.Equal(t, body, result)
}

func TestContentLengthValidator_ExactMatchReadAll(t *testing.T) {
	data := "hello world"
	v := s3.NewContentLengthValidator(io.NopCloser(strings.NewReader(data)), int64(len(data)), "bucket/key")
	got, err := io.ReadAll(v)
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestContentLengthValidator_ExactMatchRead(t *testing.T) {
	data := "hello world"
	v := s3.NewContentLengthValidator(io.NopCloser(strings.NewReader(data)), int64(len(data)), "bucket/key")
	buf := make([]byte, len(data))
	_, err := v.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, data, string(buf))

	_, err = v.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
}

func TestContentLengthValidator_TooFewBytes(t *testing.T) {
	data := "short"
	v := s3.NewContentLengthValidator(io.NopCloser(strings.NewReader(data)), 100, "bucket/key")

	_, err := io.ReadAll(v)
	require.Error(t, err)

	mismatchError, ok := err.(s3.ContentLengthMismatchError)
	assert.True(t, ok)
	assert.Equal(t, 100, mismatchError.Expected)
	assert.Equal(t, len(data), mismatchError.Actual)
	assert.Equal(t, "bucket/key", mismatchError.ObjectPath)
}

func TestContentLengthValidator_TooManyBytes(t *testing.T) {
	data := "too long to fit in expected length"
	v := s3.NewContentLengthValidator(io.NopCloser(strings.NewReader(data)), 10, "bucket/key")

	_, err := io.ReadAll(v)
	require.Error(t, err)

	mismatchError, ok := err.(s3.ContentLengthMismatchError)
	assert.True(t, ok)
	assert.Equal(t, 10, mismatchError.Expected)
	assert.Equal(t, len(data), mismatchError.Actual)
	assert.Equal(t, "bucket/key", mismatchError.ObjectPath)
}

func TestContentLengthValidator_Close(t *testing.T) {
	v := s3.NewContentLengthValidator(io.NopCloser(strings.NewReader("hello")), 5, "bucket/key")
	_, err := io.ReadAll(v)
	require.NoError(t, err)
	assert.NoError(t, v.Close())
}

func TestContentLengthValidator_CloseError(t *testing.T) {
	body := ErrOnCloseCloser{strings.NewReader("hello")}
	v := s3.NewContentLengthValidator(body, 5, "bucket/key")
	_, err := io.ReadAll(v)
	require.NoError(t, err)
	assert.EqualError(t, v.Close(), "close error")
}
