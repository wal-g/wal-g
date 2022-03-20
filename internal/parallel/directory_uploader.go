package parallel

type DirectoryUploader interface {
	Upload(path string) error
	Download(path string) error
}

type CommonDirectoryUploader struct {
}

func NewCommonDirectoryUploader() *CommonDirectoryUploader {
	return &CommonDirectoryUploader{}
}

func (uploader *CommonDirectoryUploader) Upload(path string) error {
	return nil
}

func (uploader *CommonDirectoryUploader) Download(path string) error {
	return nil
}
