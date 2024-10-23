package internal

type FilesFilter interface {
	ShouldUploadFile(path string) bool
}

type CommonFilesFilter struct{}

func NewCommonFilesFilter() FilesFilter {
	return &CommonFilesFilter{}
}

func (*CommonFilesFilter) ShouldUploadFile(path string) bool {
	return true
}
