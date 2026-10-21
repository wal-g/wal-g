package s3

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/wal-g/tracelog"
)

func NewConnResetRetryer(baseRetryer request.Retryer) *ConnResetRetryer {
	return &ConnResetRetryer{
		Retryer: baseRetryer,
	}
}

type ConnResetRetryer struct {
	request.Retryer
	additionalErrorCodes map[string]struct{}
}

func (r ConnResetRetryer) ShouldRetry(req *request.Request) bool {
	if len(r.additionalErrorCodes) > 0 {
		var apiError awserr.Error
		if errors.As(req.Error, &apiError) {
			if _, ok := r.additionalErrorCodes[apiError.Code()]; ok {
				return true
			}
		}
	}
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

	return r.Retryer.ShouldRetry(req)
}
