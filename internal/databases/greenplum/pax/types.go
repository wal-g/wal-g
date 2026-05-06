package pax

import (
	"github.com/wal-g/wal-g/internal/walparser"
)

type FileKind string

const (
	FileKindData    FileKind = "data"
	FileKindToast   FileKind = "toast"
	FileKindVisimap FileKind = "visimap"
)

// FileKey identifies a PAX file by its (tablespace, database, relfilenode, filename) tuple.
// The filename is the leaf inside the relation's `<relfilenode>_pax/` directory and uniquely
// identifies the file within that directory across all generations of visimap files.
type FileKey struct {
	SpcNode     walparser.Oid
	DBNode      walparser.Oid
	RelFileNode walparser.Oid
	Filename    string
}

// RelFileMetadata is the catalog-derived metadata for a single PAX file.
type RelFileMetadata struct {
	RelNameMd5 string
	BlockID    int64
	Kind       FileKind
}

// RelFileStorageMap maps a path-derived FileKey to its catalog metadata. A miss means
// the on-disk file is not currently referenced by the aux catalog (e.g. an orphan from
// an aborted transaction or an earlier visimap generation) and should not be routed to
// the dedicated PAX storage.
type RelFileStorageMap map[FileKey]RelFileMetadata

// Lookup parses filePath, classifies it as a PAX file, and returns its catalog metadata
// if the aux catalog references it. Files outside `_pax/` directories and orphan files
// not referenced by any aux row return ok=false.
func (m RelFileStorageMap) Lookup(filePath string) (ok bool, meta RelFileMetadata, key FileKey) {
	rfn, filename, parsed := ParseFilePath(filePath)
	if !parsed {
		return false, RelFileMetadata{}, FileKey{}
	}
	key = FileKey{
		SpcNode:     rfn.SpcNode,
		DBNode:      rfn.DBNode,
		RelFileNode: rfn.RelNode,
		Filename:    filename,
	}
	meta, ok = m[key]
	if !ok {
		return false, RelFileMetadata{}, FileKey{}
	}
	return true, meta, key
}
