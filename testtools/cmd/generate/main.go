package main

import (
	"github.com/wal-g/wal-g/testtools"
	"net/http"
	"os"
)

func main() {
	home := os.Getenv("HOME")
	http.HandleFunc("/", testtools.Handler)
	err := http.ListenAndServeTLS(":8080", home+"/server.crt", home+"/server.key", nil)

	if err != nil {
		panic(err)
	}
}
