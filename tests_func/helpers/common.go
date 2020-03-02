package helpers

import (
	"context"

	"github.com/cenkalti/backoff"
)

func Retry(ctx context.Context, maxRetries uint64, op func() error) error {
	var b backoff.BackOff
	b = backoff.NewExponentialBackOff()
	b = backoff.WithMaxRetries(b, maxRetries)
	b = backoff.WithContext(b, ctx)
	return backoff.Retry(op, b)
}
