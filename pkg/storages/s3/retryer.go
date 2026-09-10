package s3

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/wal-g/tracelog"
)

// newRetryerFunc returns a factory that builds a retryer per attempt, as
// required by aws.Config.Retryer. v2 retryers are stateless from the SDK's
// perspective; the standard retryer is composed with custom retryables that
// preserve wal-g's v1 behavior (retry on transient network errors and on the
// S3 409/429/499 responses).
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

// walgRetryables encodes wal-g's "retry on transient network errors and 409/429/499"
// rules as an aws/retry.IsErrorRetryable check.
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

func isTransientNetworkErr(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "i/o timeout")
}
