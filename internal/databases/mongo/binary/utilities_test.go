package binary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedFixOplog(t *testing.T) {
	assert.False(t, NeedFixOplog("4.0"))
	assert.False(t, NeedFixOplog("4.2"))
	assert.True(t, NeedFixOplog("4.4"))
	assert.True(t, NeedFixOplog("5.0"))
	assert.True(t, NeedFixOplog("6.0"))
}
