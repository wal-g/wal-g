package storage

import (
	"context"
	"errors"
	"fmt"
)

var ErrObjectAlreadyExists = errors.New("object already exists")

// ObjectCreator atomically creates a small object only if its name is unused.
// An error, including an ambiguous network failure, never grants ownership.
// Implementations must not emulate this with Exists followed by PutObject.
type ObjectCreator interface {
	CreateObject(ctx context.Context, name string, content []byte) error
}

func CreateObject(ctx context.Context, folder Folder, name string, content []byte) error {
	creator, ok := folder.(ObjectCreator)
	if !ok {
		return fmt.Errorf("storage %T does not support atomic object creation", folder)
	}
	return creator.CreateObject(ctx, name, content)
}
