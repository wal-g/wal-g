package walparser

/* This struct corresponds to postgres struct RelFileNode.
 * For clarification you can find it in postgres:
 * src/include/storage/relfilenode.h
 */
type RelFileNode struct {
	SpcNode Oid
	DBNode  Oid
	RelNode Oid
}
