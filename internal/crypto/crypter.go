package crypto

import (
	"context"
	"io"
)

// Crypter is responsible for making cryptographical pipeline parts when needed
type Crypter interface {
	Name() string
	Encrypt(ctx context.Context, writer io.Writer) (io.WriteCloser, error)
	Decrypt(ctx context.Context, reader io.Reader) (io.Reader, error)
}
