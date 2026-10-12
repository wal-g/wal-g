package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// shortRequestBodyMessage is the normalized Backblaze InvalidRequest text for a
// multipart body that arrived shorter than Content-Length.
const shortRequestBodyMessage = "the request body was too small"

// shortRequestBodyTerminalPunct is stripped so "too small." and "too small"
// classify the same way. The set is sentence-ending marks, including an ellipsis.
const shortRequestBodyTerminalPunct = ".!?\u2026"

// newRetryerFunc builds a retryer per client, with a separate retry quota.
// The standard retryer is composed with custom retryables that
// preserve wal-g's v1 behavior (retry on transient network errors and on the
// S3 409/429/499 responses and malformed HTTP 400 error responses). HTTP 400
// short-body rejections are retried too: Backblaze InvalidRequest "The request
// body was too small", and MinIO IncompleteBody, which the standard retryer
// does not classify. Responses with an error code from
// S3_ADDITIONAL_RETRYABLE_ERROR_CODES are retried for any HTTP status.
func newRetryerFunc(cfg *Config) func() aws.Retryer {
	return func() aws.Retryer {
		standard := retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = cfg.MaxRetries + 1
			if cfg.MaxThrottlingRetryDelay > 0 {
				o.MaxBackoff = cfg.MaxThrottlingRetryDelay
			}
			o.Retryables = append(o.Retryables, walgRetryables{})
			if len(cfg.RetryableErrorCodes) > 0 {
				o.Retryables = append(o.Retryables, retry.RetryableErrorCode{Codes: cfg.RetryableErrorCodes})
			}
		})
		return &throttlingRetryer{Standard: standard, minDelay: cfg.MinThrottlingRetryDelay,
			maxDelay: cfg.MaxThrottlingRetryDelay}
	}
}

type throttlingRetryer struct {
	*retry.Standard
	minDelay time.Duration
	maxDelay time.Duration
}

func (r *throttlingRetryer) RetryDelay(attempt int, err error) (time.Duration, error) {
	delay, delayErr := r.Standard.RetryDelay(attempt, err)
	if delayErr != nil {
		return 0, delayErr
	}
	isThrottle := retry.IsErrorThrottles(retry.DefaultThrottles).IsErrorThrottle(err) == aws.TrueTernary
	var response interface{ HTTPStatusCode() int }
	if errors.As(err, &response) && response.HTTPStatusCode() == http.StatusTooManyRequests {
		isThrottle = true
	}
	if isThrottle {
		delay = max(delay, r.minDelay)
	}
	if r.maxDelay > 0 {
		delay = min(delay, r.maxDelay)
	}
	return delay, nil
}

func (folder *Folder) IsRetryableError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return storage.IsTransientIOError(err) || newRetryerFunc(folder.config)().IsErrorRetryable(err)
}

// walgRetryables encodes wal-g's additional retry rules as an aws/retry.IsErrorRetryable check.
type walgRetryables struct{}

func (walgRetryables) IsErrorRetryable(err error) aws.Ternary {
	if err == nil {
		return aws.UnknownTernary
	}
	if isTransientNetworkErr(err) {
		return aws.TrueTernary
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		switch respErr.HTTPStatusCode() {
		case http.StatusBadRequest:
			// Proxies may return HTML instead of an S3 XML error. SDK v1 retried
			// the resulting parse error, but v2 does not do so by default.
			if _, ok := errors.AsType[*xml.SyntaxError](respErr); ok {
				return aws.TrueTernary
			}
			// Backblaze and MinIO sometimes reject one buffered multipart part.
			// Retry the buffered part inside the SDK first.
			if _, ok := retryableShortRequestBody(respErr); ok {
				return aws.TrueTernary
			}
		case 409:
			return aws.TrueTernary
		case 429:
			return aws.TrueTernary
		// Some S3-compatible servers (e.g. MinIO) return HTTP 499 with error code
		// "ClientDisconnected" when the request context is canceled server-side
		// (including on the server's own shutdown), not necessarily because the
		// client actually disconnected. Treat it as a transient failure worth
		// retrying rather than aborting the whole backup on a single occurrence.
		case 499:
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
