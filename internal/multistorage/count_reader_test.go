package multistorage

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_countReader(t *testing.T) {
	t.Run("wraps simple reads and counts bytes", func(t *testing.T) {
		r := bytes.NewReader([]byte("hello world!"))
		cr := newCountReader(r)

		buf := make([]byte, 3)
		n, err := cr.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		assert.Equal(t, []byte("hel"), buf)
		assert.Equal(t, int64(3), cr.ReadBytes())

		buf = make([]byte, 6)
		n, err = cr.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, []byte("lo wor"), buf)
		assert.Equal(t, int64(9), cr.ReadBytes())

		buf = make([]byte, 6)
		n, err = cr.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		assert.Equal(t, []byte("ld!\000\000\000"), buf)
		assert.Equal(t, int64(12), cr.ReadBytes())

		buf = make([]byte, 3)
		n, err = cr.Read(buf)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 0, n)
		assert.Equal(t, make([]byte, 3), buf)
		assert.Equal(t, int64(12), cr.ReadBytes())
	})

	t.Run("works with empty reader", func(t *testing.T) {
		r := bytes.NewReader(make([]byte, 0))
		cr := newCountReader(r)

		buf := make([]byte, 3)
		n, err := cr.Read(buf)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 0, n)
		assert.Equal(t, make([]byte, 3), buf)
		assert.Equal(t, int64(0), cr.ReadBytes())
	})
}
