package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type rangeAPI struct {
	API
	get func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error)
}

func (a *rangeAPI) GetObject(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return a.get(input)
}

type rangeBody struct {
	data   string
	err    error
	closed bool
}

func (b *rangeBody) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	if b.data == "" {
		return n, b.err
	}
	return n, nil
}
func (b *rangeBody) Close() error                      { b.closed = true; return nil }
func noRangeWait(context.Context, time.Duration) error { return nil }

func TestRangeRecoveryPreservesBytesAndIdentity(t *testing.T) {
	for _, readErr := range []error{io.ErrUnexpectedEOF, io.EOF} {
		t.Run(readErr.Error(), func(t *testing.T) {
			initial := &rangeBody{data: "abc", err: readErr}
			resumed := &rangeBody{data: "def", err: io.EOF}
			api := &rangeAPI{get: func(in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
				require.True(t, initial.closed)
				require.Equal(t, "bytes=3-", aws.ToString(in.Range))
				require.Equal(t, "version", aws.ToString(in.VersionId))
				require.Equal(t, "etag", aws.ToString(in.IfMatch))
				require.Equal(t, "AES256", aws.ToString(in.SSECustomerAlgorithm))
				require.Equal(t, "test-key", aws.ToString(in.SSECustomerKey))
				require.Equal(t, "test-md5", aws.ToString(in.SSECustomerKeyMD5))
				return &awss3.GetObjectOutput{Body: resumed, ContentRange: aws.String("bytes 3-5/6"),
					ContentLength: aws.Int64(3), ETag: aws.String("etag"), VersionId: aws.String("version")}, nil
			}}
			folder := NewFolder(api, nil, "", &Config{})
			input := &awss3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String("object"),
				SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String("test-key"), SSECustomerKeyMD5: aws.String("test-md5")}
			r := newRangeReader(t.Context(), input, &awss3.GetObjectOutput{Body: initial,
				ContentLength: aws.Int64(6), ETag: aws.String("etag"), VersionId: aws.String("version")}, 1, folder)
			r.wait = noRangeWait
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, "abcdef", string(data))
			require.Equal(t, 1, r.reconnects)
			require.NoError(t, r.Close())
			require.NoError(t, r.Close())
			require.True(t, resumed.closed)
		})
	}
}

func TestRangeRecoveryBudgetIsCumulative(t *testing.T) {
	for _, budget := range []int{0, 2} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			gets := 0
			bodies := []*rangeBody{{data: "a", err: io.ErrUnexpectedEOF}}
			api := &rangeAPI{get: func(in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
				gets++
				require.Equal(t, fmt.Sprintf("bytes=%d-", gets), aws.ToString(in.Range))
				body := &rangeBody{data: "b", err: io.ErrUnexpectedEOF}
				bodies = append(bodies, body)
				return &awss3.GetObjectOutput{Body: body, ContentRange: aws.String(fmt.Sprintf("bytes %d-9/10", gets)),
					ContentLength: aws.Int64(int64(10 - gets))}, nil
			}}
			folder := NewFolder(api, nil, "", &Config{})
			r := newRangeReader(t.Context(), folder.getObjectInput("object"),
				&awss3.GetObjectOutput{Body: bodies[0], ContentLength: aws.Int64(10)}, budget, folder)
			r.wait = noRangeWait
			data, err := io.ReadAll(r)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.Len(t, data, budget+1)
			require.Equal(t, budget, gets)
			require.NoError(t, r.Close())
			for _, body := range bodies {
				require.True(t, body.closed)
			}
		})
	}
}

func TestRangeRecoveryRejectsInconsistentResponses(t *testing.T) {
	for _, variant := range []string{"missing range", "wrong offset", "changed length", "changed etag", "changed version"} {
		t.Run(variant, func(t *testing.T) {
			body := &rangeBody{data: "bc", err: io.EOF}
			response := &awss3.GetObjectOutput{Body: body, ContentRange: aws.String("bytes 1-2/3"),
				ContentLength: aws.Int64(2), ETag: aws.String("original"), VersionId: aws.String("original")}
			switch variant {
			case "missing range":
				response.ContentRange = nil
			case "wrong offset":
				response.ContentRange = aws.String("bytes 0-1/3")
			case "changed length":
				response.ContentRange = aws.String("bytes 1-2/4")
			case "changed etag":
				response.ETag = aws.String("other")
			case "changed version":
				response.VersionId = aws.String("other")
			}
			api := &rangeAPI{get: func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error) { return response, nil }}
			folder := NewFolder(api, nil, "", &Config{})
			r := newRangeReader(t.Context(), folder.getObjectInput("object"), &awss3.GetObjectOutput{
				Body: &rangeBody{data: "a", err: io.ErrUnexpectedEOF}, ContentLength: aws.Int64(3),
				ETag: aws.String("original"), VersionId: aws.String("original")}, 2, folder)
			r.wait = noRangeWait
			data, err := io.ReadAll(r)
			require.Error(t, err)
			require.False(t, folder.IsRetryableError(err))
			require.Equal(t, "a", string(data))
			require.True(t, body.closed)
			require.NoError(t, r.Close())
		})
	}
}

func TestRangeRecoveryCancellationAndGetFailures(t *testing.T) {
	t.Run("cancel during wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		body := &rangeBody{err: io.ErrUnexpectedEOF}
		folder := NewFolder(&rangeAPI{get: func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
			t.Error("GET after cancellation")
			return nil, context.Canceled
		}}, nil, "", &Config{})
		r := NewRangeReader(ctx, body, "object", 2, folder)
		r.wait = func(ctx context.Context, _ time.Duration) error { cancel(); return waitForRangeRetry(ctx, time.Hour) }
		_, err := io.ReadAll(r)
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, body.closed)
		require.NoError(t, r.Close())
	})
	for _, failure := range []error{io.ErrUnexpectedEOF, errors.New("AccessDenied")} {
		t.Run(failure.Error(), func(t *testing.T) {
			calls := 0
			folder := NewFolder(&rangeAPI{get: func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
				calls++
				return nil, failure
			}}, nil, "", &Config{})
			r := NewRangeReader(t.Context(), &rangeBody{err: io.ErrUnexpectedEOF}, "object", 2, folder)
			r.wait = noRangeWait
			_, err := io.ReadAll(r)
			require.ErrorIs(t, err, failure)
			want := 1
			if errors.Is(failure, io.ErrUnexpectedEOF) {
				want = 2
			}
			require.Equal(t, want, calls)
			require.NoError(t, r.Close())
		})
	}
	t.Run("close before first read", func(t *testing.T) {
		r := NewRangeReader(t.Context(), nil, "object", 0, NewFolder(nil, nil, "", &Config{}))
		require.NoError(t, r.Close())
		_, err := io.ReadAll(r)
		require.ErrorIs(t, err, io.ErrClosedPipe)
	})
	t.Run("normal eof", func(t *testing.T) {
		r := NewRangeReader(t.Context(), io.NopCloser(strings.NewReader("ok")), "object", 0, NewFolder(nil, nil, "", &Config{}))
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, "ok", string(data))
		require.NoError(t, r.Close())
	})
}
