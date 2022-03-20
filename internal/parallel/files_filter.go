package parallel

type FilesFilter interface {
	ShouldUploadFile(path string) bool
}

type CommonFilesFilter struct{}

func (*CommonFilesFilter) ShouldUploadFile(path string) bool {
	return true
}
