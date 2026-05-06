package pax

import (
	"path"
	"strings"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/walparser"
)

const dirSuffix = "_pax"

// ParseFilePath extracts (RelFileNode, filename) from a path of the form
// `.../base/<dbOid>/<relfilenode>_pax/<filename>` or the equivalent under pg_tblspc.
// Returns ok=false for any path that is not under a `<n>_pax/` directory or whose
// non-PAX-related parts cannot be parsed as a regular relfile path.
func ParseFilePath(filePath string) (relFileNode walparser.RelFileNode, filename string, ok bool) {
	dir, file := path.Split(filePath)
	dir = strings.TrimSuffix(dir, "/")
	relfilenodeWithSuffix := path.Base(dir)
	if !strings.HasSuffix(relfilenodeWithSuffix, dirSuffix) {
		return walparser.RelFileNode{}, "", false
	}
	relfilenode := strings.TrimSuffix(relfilenodeWithSuffix, dirSuffix)
	if relfilenode == "" {
		return walparser.RelFileNode{}, "", false
	}

	// Synthesize a path that postgres.GetRelFileNodeFrom can parse:
	// .../base/<dbOid>/<relfilenode>_pax/<filename>  ->  .../base/<dbOid>/<relfilenode>
	parent := strings.TrimSuffix(dir, relfilenodeWithSuffix)
	parent = strings.TrimSuffix(parent, "/")
	syntheticPath := path.Join(parent, relfilenode)

	rfn, err := postgres.GetRelFileNodeFrom(syntheticPath)
	if err != nil || rfn == nil {
		return walparser.RelFileNode{}, "", false
	}

	return *rfn, file, true
}
