package tools

import (
	"os"
)

func MakeDir(name string) {
	dest := name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, 0755); err != nil {
			panic(err)
		}
	}
}
