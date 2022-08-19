package stages

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildFileBuffer(path string) *FileBuffer {
	b, err := NewFileBuffer(path)
	if err != nil {
		panic(err)
	}
	return b
}

func TestBuffers_ReadWriteLen(t *testing.T) {
	tests := []struct {
		name string
		b    Buffer
	}{
		{
			name: "memory_buffer",
			b:    NewMemoryBuffer(),
		},
		{
			name: "file_buffer",
			b:    buildFileBuffer("/tmp"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.b
			assert.Equal(t, 0, buf.Len())

			w10, err := buf.Write(make([]byte, 10))
			assert.Nil(t, err)
			assert.Equal(t, 10, w10)
			assert.Equal(t, buf.Len(), 10)

			w13, err := buf.Write(make([]byte, 13))
			assert.Nil(t, err)
			assert.Equal(t, 13, w13)
			assert.Equal(t, 23, buf.Len())

			reader, err := buf.Reader()
			assert.Nil(t, err)
			assert.NotNil(t, reader)

			rbuf15 := make([]byte, 15)
			r15, err := reader.Read(rbuf15)
			assert.Nil(t, err)
			assert.Equal(t, 15, r15)
			assert.Equal(t, 8, buf.Len())

			_, err = buf.Write(make([]byte, 17))
			assert.Error(t, err, fmt.Errorf("buffer is not reset after reading"))

			r8, err := reader.Read(rbuf15)
			assert.Nil(t, err)
			assert.Equal(t, 8, r8)
			assert.Equal(t, 0, buf.Len())

			_, err = reader.Read(rbuf15)
			assert.Error(t, err, io.EOF)

			_, err = buf.Write(make([]byte, 17))
			assert.Error(t, err, fmt.Errorf("buffer is not reset after reading"))

			err = buf.Reset()
			assert.Nil(t, err)

			w23, err := buf.Write(make([]byte, 23))
			assert.Nil(t, err)
			assert.Equal(t, 23, w23)
			assert.Equal(t, 23, buf.Len())

			err = buf.Reset()
			assert.Nil(t, err)
			assert.Equal(t, 0, buf.Len())

			err = buf.Close()
			assert.Nil(t, err)
		})
	}
}

func TestFileBuffer_NewClose(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		expectedErr bool
	}{
		{
			name:        "tmp_dir",
			path:        "/tmp/",
			expectedErr: false,
		},
		{
			name:        "non_existent_dir",
			path:        "/non_existent_dir",
			expectedErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fb, err := NewFileBuffer(tc.path)
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			assert.Nil(t, err)

			fname := fb.File.Name()
			_, err = os.Stat(fname)
			assert.Nil(t, err)

			err = fb.Close()
			assert.Nil(t, err)
			_, err = os.Stat(fname)
			assert.Error(t, os.ErrExist, err)
		})
	}
}
