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
	if req.Error != nil && strings.Contains(req.Error.Error(), "connection reset by peer") {
		return true
	}

	if req.Error != nil && strings.Contains(req.Error.Error(), "SignatureDoesNotMatch") {
		// It looks like we have some rare issues with request. Sign one more time
		err := req.Sign()
		if err != nil {
			tracelog.ErrorLogger.Printf("Cannot re-sign request: %v", err)
			return false
		}
		return true
	}

	return r.Retryer.ShouldRetry(req)
}
