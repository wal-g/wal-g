package walparser

var TerminalLocation = *NewBlockLocation(0, 0, 0, 0)


type BlockLocation struct {
	RelationFileNode RelFileNode
	BlockNo          uint32
}

func NewBlockLocation(spcNode, dbNode, relNode Oid, blockNo uint32) *BlockLocation {
	return &BlockLocation{
		RelationFileNode: RelFileNode{SpcNode: spcNode, DBNode: dbNode, RelNode: relNode},
		BlockNo:          blockNo,
	}
}
