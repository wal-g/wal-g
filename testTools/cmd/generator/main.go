package main

import (
	"github.com/wal-g/wal-g/testTools"
	"net/http"
	"os"
)

func main() {
	home := os.Getenv("HOME")
	http.HandleFunc("/", tools.Handler)
	err := http.ListenAndServeTLS(":8080", home+"/server.crt", home+"/server.key", nil)

	if err != nil {
		panic(err)
	}
}
