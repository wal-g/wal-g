package testtools

type MockReadSeekCloser struct {
	Testdata []byte
}

func (readSeekCloser *MockReadSeekCloser) Close() error {
	return nil
}

func (readSeekCloser *MockReadSeekCloser) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = readSeekCloser.Testdata[i]
	}
	return 0, nil
}

func (readSeekCloser *MockReadSeekCloser) Seek(offset int64, whence int) (n int64, err error) {
	readSeekCloser.Testdata = readSeekCloser.Testdata[offset:]
	return 0, nil
}
