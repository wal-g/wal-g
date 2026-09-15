package postgres

import "testing"

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
