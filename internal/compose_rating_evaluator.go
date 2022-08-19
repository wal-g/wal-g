package internal

type ComposeRatingEvaluator interface {
	Evaluate(path string, updatesCount uint64, wasInBase bool) uint64
}

type DefaultComposeRatingEvaluator struct {
	incrementFromFiles BackupFileList
}

func NewDefaultComposeRatingEvaluator(incrementFromFiles BackupFileList) *DefaultComposeRatingEvaluator {
	return &DefaultComposeRatingEvaluator{incrementFromFiles: incrementFromFiles}
}

func (evaluator *DefaultComposeRatingEvaluator) Evaluate(path string, updatesCount uint64, wasInBase bool) uint64 {
	if !wasInBase {
		return updatesCount
	}
	prevUpdateCount := evaluator.incrementFromFiles[path].UpdatesCount
	if prevUpdateCount == 0 {
		return updatesCount
	}
	return (updatesCount * 100) / prevUpdateCount
}
