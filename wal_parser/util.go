package wal_parser

type Oid = uint32

func minUint32(a uint32, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
