package s3

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/wal-g/tracelog"
)

func NewConnResetRetryer(baseRetryer request.Retryer) *ConnResetRetryer {
	return &ConnResetRetryer{
		baseRetryer,
	}
}

type ConnResetRetryer struct {
	request.Retryer
}

func (r ConnResetRetryer) ShouldRetry(req *request.Request) bool {
	if req.Error != nil {
		errMsg := req.Error.Error()
		if strings.Contains(errMsg, "connection reset by peer") ||
			strings.Contains(errMsg, "connection refused") ||
			strings.Contains(errMsg, "connection timed out") ||
			strings.Contains(errMsg, "i/o timeout") {
			tracelog.InfoLogger.Printf("Retrying S3 request due to transient network error: %v", req.Error)
			return true
		}
	}

	if req.HTTPResponse != nil && req.HTTPResponse.StatusCode == 409 {
		tracelog.InfoLogger.Printf("S3 returned HTTP 409 (OperationAborted), retrying request")
		return true
	}

	// Some S3-compatible servers (e.g. MinIO) return HTTP 499 with error code
	// "ClientDisconnected" when the request context is canceled server-side
	// (including on the server's own shutdown), not necessarily because the
	// client actually disconnected. Treat it as a transient failure worth
	// retrying rather than aborting the whole backup on a single occurrence.
	if req.HTTPResponse != nil && req.HTTPResponse.StatusCode == 499 {
		tracelog.InfoLogger.Printf("S3 returned HTTP 499 (ClientDisconnected), retrying request")
		return true
	}

	return r.Retryer.ShouldRetry(req)
}
