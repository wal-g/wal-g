package s3

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/assert"
)

func TestConnResetRetryerRetry(t *testing.T) {
	retryer := NewConnResetRetryer(client.DefaultRetryer{})
	err := &net.OpError{
		Op:     "mock",
		Net:    "mock",
		Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
		Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
		Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
	}

	req := &request.Request{
		Error: err,
	}

	assert.True(t, retryer.ShouldRetry(req))
}

func TestConnResetRetryerRandomError(t *testing.T) {
	retryer := NewConnResetRetryer(client.DefaultRetryer{})
	req := &request.Request{
		Error: fmt.Errorf("some strange unknown error"),
	}
	assert.False(t, retryer.ShouldRetry(req))
}

func TestConnResetRetryerNoError(t *testing.T) {
	retryer := NewConnResetRetryer(client.DefaultRetryer{})
	assert.False(t, retryer.ShouldRetry(&request.Request{}))
}
