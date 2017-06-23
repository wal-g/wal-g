package extract

import (
	"log"
	"os"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func MakeDir(name string) {
	dest := name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.Mkdir(dest, 0700); err != nil {
			panic(err)
		}
	}
}

// func debug(mes string, val uint32) {
// 	log.Printf("%s: %d", mes, val)
// }