package tools

import (
	"fmt"
	"os"
	"time"
)

// MakeDir creates a new directory with mode 0755.
func MakeDir(name string) {
	dest := name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, 0755); err != nil {
			panic(err)
		}
	}
}

// TimeTrack is used to time how long functions take.
//
// Usage Example:
// defer timeTrack(time.Now(), "EXTRACT ALL")
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}
