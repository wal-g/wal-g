package s3

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/wal-g/tracelog"
)

// shortRequestBodyMessage is the normalized Backblaze InvalidRequest text for a
// multipart body that arrived shorter than Content-Length.
const shortRequestBodyMessage = "the request body was too small"

// shortRequestBodyTerminalPunct is stripped so "too small." and "too small"
// classify the same way. The set is sentence-ending marks, including an ellipsis.
const shortRequestBodyTerminalPunct = ".!?\u2026"

// newRetryerFunc returns a factory that builds a retryer per attempt, as
// required by aws.Config.Retryer. v2 retryers are stateless from the SDK's
// perspective; the standard retryer is composed with custom retryables that
// preserve wal-g's v1 behavior (retry on transient network errors and on the
// S3 409/429/499 responses and malformed HTTP 400 error responses). HTTP 400
// short-body rejections are retried too: Backblaze InvalidRequest "The request
// body was too small", and MinIO IncompleteBody, which the standard retryer
// does not classify.
func newRetryerFunc(cfg *Config) func() aws.Retryer {
	return func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			if cfg.MaxRetries > 0 {
				o.MaxAttempts = cfg.MaxRetries + 1
			}
			if cfg.MaxThrottlingRetryDelay > 0 {
				o.MaxBackoff = cfg.MaxThrottlingRetryDelay
			}
			o.Retryables = append(o.Retryables, walgRetryables{})
		})
	}
}

// walgRetryables encodes wal-g's additional retry rules as an aws/retry.IsErrorRetryable check.
type walgRetryables struct{}

func (walgRetryables) IsErrorRetryable(err error) aws.Ternary {
	if err == nil {
		return aws.UnknownTernary
	}
	if isTransientNetworkErr(err) {
		tracelog.InfoLogger.Printf("Retrying S3 request due to transient network error: %v", err)
		return aws.TrueTernary
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		switch respErr.HTTPStatusCode() {
		case http.StatusBadRequest:
			// Proxies may return HTML instead of an S3 XML error. SDK v1 retried
			// the resulting parse error, but v2 does not do so by default.
			if _, ok := errors.AsType[*xml.SyntaxError](respErr); ok {
				tracelog.InfoLogger.Printf("S3 returned HTTP 400 with malformed XML, retrying request: %v", err)
				return aws.TrueTernary
			}
			// Backblaze and MinIO sometimes reject one buffered multipart part.
			// Retry that part inside the SDK. Replaying the whole upload would
			// read an already-consumed body.
			if code, ok := retryableShortRequestBody(respErr); ok {
				tracelog.InfoLogger.Printf(
					"S3 returned HTTP 400 for a short request body (%s), retrying request: %v", code, err)
				return aws.TrueTernary
			}
		case 409:
			tracelog.InfoLogger.Printf("S3 returned HTTP 409 (OperationAborted), retrying request")
			return aws.TrueTernary
		case 429:
			tracelog.InfoLogger.Printf("S3 returned HTTP 429 (TooManyRequests), retrying request")
			return aws.TrueTernary
		// Some S3-compatible servers (e.g. MinIO) return HTTP 499 with error code
		// "ClientDisconnected" when the request context is canceled server-side
		// (including on the server's own shutdown), not necessarily because the
		// client actually disconnected. Treat it as a transient failure worth
		// retrying rather than aborting the whole backup on a single occurrence.
		case 499:
			tracelog.InfoLogger.Printf("S3 returned HTTP 499 (ClientDisconnected), retrying request")
			return aws.TrueTernary
		}
	}
	return aws.UnknownTernary
}

// retryableShortRequestBody returns the API error code when err is a transient
// short-body rejection. InvalidRequest matches only the normalized Backblaze
// message. IncompleteBody matches on the structured code; the standard
// retryer's DefaultRetryableErrorCodes set does not include it. Callers must
// already have restricted this to HTTP 400 so other statuses keep the standard
// policy.
func retryableShortRequestBody(err error) (string, bool) {
	apiErr, ok := errors.AsType[smithy.APIError](err)
	if !ok {
		return "", false
	}
	switch apiErr.ErrorCode() {
	case "IncompleteBody":
		return apiErr.ErrorCode(), true
	case "InvalidRequest":
		if isShortRequestBodyMessage(apiErr.ErrorMessage()) {
			return apiErr.ErrorCode(), true
		}
	}
	return "", false
}

// isShortRequestBodyMessage reports whether message is the Backblaze
// InvalidRequest text. Comparison ignores case, surrounding space, and
// terminal punctuation. A longer message that merely contains the phrase does
// not match.
func isShortRequestBodyMessage(message string) bool {
	normalized := strings.ToLower(strings.TrimSpace(message))
	normalized = strings.TrimRight(normalized, shortRequestBodyTerminalPunct)
	normalized = strings.TrimSpace(normalized)
	return normalized == shortRequestBodyMessage
}

func isTransientNetworkErr(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "i/o timeout")
}
