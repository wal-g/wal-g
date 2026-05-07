package s3

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
)

func TestWalgRetryablesConnReset(t *testing.T) {
	err := &net.OpError{
		Op:     "mock",
		Net:    "mock",
		Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
		Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
		Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
	}
	assert.Equal(t, aws.TrueTernary, walgRetryables{}.IsErrorRetryable(err))
}

func TestWalgRetryablesRandomError(t *testing.T) {
	err := fmt.Errorf("some strange unknown error")
	assert.Equal(t, aws.UnknownTernary, walgRetryables{}.IsErrorRetryable(err))
}

func TestWalgRetryablesNoError(t *testing.T) {
	assert.Equal(t, aws.UnknownTernary, walgRetryables{}.IsErrorRetryable(nil))
}

func TestWalgRetryablesOperationAborted(t *testing.T) {
	respErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 409}},
		Err:      fmt.Errorf("operation aborted"),
	}
	assert.Equal(t, aws.TrueTernary, walgRetryables{}.IsErrorRetryable(respErr))
}
