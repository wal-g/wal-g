//go:build !goexperiment.simd || !amd64

package postgres

func pgChecksumBlockFast(page *PgDatabasePage) uint32 {
	return pgChecksumBlockScalar(page)
}
