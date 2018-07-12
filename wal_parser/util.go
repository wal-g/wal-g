package wal_parser

type Oid = uint32
type TimeLineID = uint32
type XLogRecordPtr = uint64

func minUint32(a uint32, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func concatByteSlices(a []byte, b []byte) []byte {
	result := make([]byte, len(a) + len(b))
	copy(result, a)
	copy(result[len(a):], b)
	return result
}
