package walparser

/* List of postgres resource managers, for clarification you can look at postgres code:
 * src/include/access/rmgrlist.h
 */

const (
	RmXlogID = iota
	RmXactID
	RmSmgrID
	RmClogID
	RmDBaseID
	RmTblSpcID
	RmMultiXactID
	RmRelMapID
	RmStandbyID
	RmHeap2ID
	RmHeapID
	RmBTreeID
	RmHashID
	RmGinID
	RmGistID
	RmSeqID
	RmSPGistID
	RmBrinID
	RmCommitTSID
	RmReplOriginID
	RmGenericID
	RmLogicalMsgID

	RmNextFreeID
)
