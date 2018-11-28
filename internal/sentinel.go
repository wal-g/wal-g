package internal

import "os"

// Sentinel is used to signal completion of a walked
// directory.
type Sentinel struct {
	Info os.FileInfo
	path string
}
