package extract

import (
	"time"
	"os"
	"log"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func MakeDir(home, name string) {
	dest := home + "/" + name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.Mkdir(dest, 0700); err != nil {
			panic(err)
		}
	}
}