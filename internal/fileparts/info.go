package fileparts

const (
	OffsetPAX   = "WAL-G.FileParts.Offset"
	FileSizePAX = "WAL-G.FileParts.FileSize"
)

type Info struct {
	PartSize int64            `json:"part_size"`
	Files    map[string]int64 `json:"files"`
}
