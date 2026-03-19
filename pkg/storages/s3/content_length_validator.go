package s3

import (
	"fmt"
	"io"
)

// ContentLengthMismatchError is returned when the actual number of bytes read
// from an S3 object body does not match the Content-Length header value.
type ContentLengthMismatchError struct {
	ObjectPath string
	Expected   int
	Actual     int
}

func (e ContentLengthMismatchError) Error() string {
	return fmt.Sprintf(
		"content length mismatch for S3 object %q: Content-Length header says %d bytes, but got %d bytes",
		e.ObjectPath, e.Expected, e.Actual,
	)
}

// СontentLengthValidator wraps an io.ReadCloser and validates that the total
// number of bytes read matches the expected Content-Length when EOF is reached.
// If expectedSize is 0 (e.g. Content-Length was not set), the validator
// is a transparent pass-through and performs no length check.
type ContentLengthValidator struct {
	underlying     io.ReadCloser
	objectPath     string
	expectedLength int
	actualLengts   int
}

func NewContentLengthValidator(body io.ReadCloser, expectedSize int64, objectPath string) io.ReadCloser {
	if expectedSize == 0 {
		return body
	}
	return &ContentLengthValidator{
		underlying:     body,
		objectPath:     objectPath,
		expectedLength: int(expectedSize),
	}
}

func (v *ContentLengthValidator) Read(p []byte) (int, error) {
	n, err := v.underlying.Read(p)
	v.actualLengts += n
	if err == io.EOF && v.actualLengts != v.expectedLength {
		return n, ContentLengthMismatchError{
			ObjectPath: v.objectPath,
			Expected:   v.expectedLength,
			Actual:     v.actualLengts,
		}
	}
	return n, err
}

func (v *ContentLengthValidator) Close() error {
	return v.underlying.Close()
}
