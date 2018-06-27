package walg

import "sync"

// TarBundle represents one completed directory.
type TarBundle interface {
	NewTarBall(dedicatedUploader bool)
	GetIncrementBaseLsn() *uint64
	GetIncrementBaseFiles() BackupFileList

	StartQueue()
	Deque() TarBall
	EnqueueBack(tarBall TarBall, parallelOpInProgress *bool)
	CheckSizeAndEnqueueBack(tarBall TarBall) error
	FinishQueue() error
	GetFiles() *sync.Map
}
