package storage

import (
	"context"
	"fmt"
)

// PrefixLister lists current objects through a native provider prefix filter.
// The prefix is literal and relative to the folder. Names retain the prefix and
// remain relative to the folder, including any nested path and compression suffix.
// Implementations must complete all pages or return an error without objects.
type PrefixLister interface {
	ListObjectsWithPrefix(ctx context.Context, prefix string) ([]Object, error)
}

// ListObjectsWithPrefix requires native filtering; it never falls back to ListFolder.
func ListObjectsWithPrefix(ctx context.Context, folder Folder, prefix string) ([]Object, error) {
	if prefix == "" {
		return nil, fmt.Errorf("object prefix must not be empty")
	}
	lister, ok := folder.(PrefixLister)
	if !ok {
		return nil, fmt.Errorf("native prefix listing is not supported by %T", folder)
	}
	return lister.ListObjectsWithPrefix(ctx, prefix)
}
