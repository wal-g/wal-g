package main

import (
	"github.com/katie31/wal-g"
	"net/http"
	"os"
)

func main() {
	home := os.Getenv("HOME")
	http.HandleFunc("/", walg.Handler)
	err := http.ListenAndServeTLS(":8080", home+"/server.crt", home+"/server.key", nil)

	if err != nil {
		panic(err)
	}
}
