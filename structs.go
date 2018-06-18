package walg

// Empty is used for channel signaling.
type Empty struct{}

// NilWriter to /dev/null
type NilWriter struct{}

// Write to /dev/null
func (nw *NilWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

type BackupFileList map[string]BackupFileDescription