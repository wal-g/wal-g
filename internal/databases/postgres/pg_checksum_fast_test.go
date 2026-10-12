package postgres

import (
	"fmt"
	"testing"
)

func TestPgChecksumBlockFastMatchesScalar(t *testing.T) {
	pages := []PgDatabasePage{{}}

	var incrementing PgDatabasePage
	for i := range incrementing {
		incrementing[i] = byte(i)
	}
	pages = append(pages, incrementing)

	var pseudoRandom PgDatabasePage
	value := uint32(1)
	for i := range pseudoRandom {
		value = value*1664525 + 1013904223
		pseudoRandom[i] = byte(value >> 24)
	}
	pages = append(pages, pseudoRandom)

	for _, page := range pages {
		if got, want := pgChecksumBlockFast(&page), pgChecksumBlockScalar(&page); got != want {
			t.Errorf("pgChecksumBlockFast() = %#08x, want %#08x", got, want)
		}
	}
}

// benchChecksumPages fills n pages with pseudo-random, non-repeating data so
// the benchmark exercises the same bytes the scalar and SIMD kernels would
// see on a real cluster.
func benchChecksumPages(n int) []PgDatabasePage {
	pages := make([]PgDatabasePage, n)
	value := uint32(1)
	for i := range pages {
		for j := range pages[i] {
			value = value*1664525 + 1013904223
			pages[i][j] = byte(value >> 24)
		}
	}
	return pages
}

// BenchmarkPgChecksumBlock compares the scalar and SIMD-accelerated (build
// tag goexperiment.simd) block checksum kernels across batch sizes.
//
// go test -run XXX -bench BenchmarkPgChecksumBlock -benchtime 20x -count 10 ./internal/databases/postgres/
func BenchmarkPgChecksumBlock(b *testing.B) {
	for _, count := range []int{1, 128, 1024} {
		pages := benchChecksumPages(count)
		b.Run(fmt.Sprintf("%dPages", count), func(b *testing.B) {
			b.Run("Scalar", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(count) * DatabasePageSize)
				for i := 0; i < b.N; i++ {
					for p := range pages {
						pgChecksumBlockScalar(&pages[p])
					}
				}
			})
			b.Run("Fast", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(count) * DatabasePageSize)
				for i := 0; i < b.N; i++ {
					for p := range pages {
						pgChecksumBlockFast(&pages[p])
					}
				}
			})
		})
	}
}
