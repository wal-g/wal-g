package wal_parser

/* This struct corresponds to postgres struct RelFileNode.
 * For clarification you can find it in postgres:
 * src/include/storage/relfilenode.h
 */
type RelFileNode struct {
	spcNode Oid
	dbNode  Oid
	relNode Oid
}
