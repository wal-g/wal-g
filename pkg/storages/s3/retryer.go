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
		if strings.Contains(req.Error.Error(), "connection reset by peer") ||
			strings.Contains(req.Error.Error(), "connection refused") {
			return true
		}
	}

	if req.HTTPResponse != nil && req.HTTPResponse.StatusCode == 409 {
		tracelog.InfoLogger.Printf("S3 returned HTTP 409 (OperationAborted), retrying request")
		return true
	}

	return r.Retryer.ShouldRetry(req)
}
