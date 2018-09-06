package walparser

type XLogRecordBlock struct {
	Header XLogRecordBlockHeader
	Image  []byte
	Data   []byte
}
