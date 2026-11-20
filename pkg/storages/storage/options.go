package storage

import (
	"context"
	"time"
)

type additionalRetentionKey struct{}

// WithAdditionalRetention extends the destination's compliance retention for one write.
func WithAdditionalRetention(ctx context.Context, extra time.Duration) context.Context {
	return context.WithValue(ctx, additionalRetentionKey{}, extra)
}

func AdditionalRetentionFromContext(ctx context.Context) time.Duration {
	extra, _ := ctx.Value(additionalRetentionKey{}).(time.Duration)
	return extra
}

type storageClassKey struct{}

// WithStorageClass sets the storage class for a write. An empty class uses the destination default.
func WithStorageClass(ctx context.Context, class string) context.Context {
	return context.WithValue(ctx, storageClassKey{}, class)
}

//nolint:revive // StorageClass is the S3 API term.
func StorageClassFromContext(ctx context.Context) string {
	class, _ := ctx.Value(storageClassKey{}).(string)
	return class
}
