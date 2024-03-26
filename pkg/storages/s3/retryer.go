package s3

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/request"
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

	return r.Retryer.ShouldRetry(req)
}
