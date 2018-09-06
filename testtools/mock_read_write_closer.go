package testtools

type MockReadWriteCloser struct{}

func (readWriterCloser *MockReadWriteCloser) Close() error {
	return nil
}

func (readWriterCloser *MockReadWriteCloser) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (readWriterCloser *MockReadWriteCloser) Write(p []byte) (n int, err error) {
	return 0, nil
}
