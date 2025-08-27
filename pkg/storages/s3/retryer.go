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
		auth := r.getAuthHeader(req)

		stash := req.Error
		req.Error = nil // hide req.Error - so, req.Sign() will not fail
		err := req.Sign()
		req.Error = stash

		if err != nil {
			tracelog.ErrorLogger.Printf("Cannot re-sign request: %v", err)
			return false
		}
		tracelog.WarningLogger.Printf("Old signature '%v', new signature: '%v'", auth, r.getAuthHeader(req))
		return true
	}

	return r.Retryer.ShouldRetry(req)
}

func (r ConnResetRetryer) getAuthHeader(req *request.Request) string {
	if req.HTTPRequest == nil {
		return ""
	}
	return req.HTTPRequest.Header.Get("Authorization")
}
