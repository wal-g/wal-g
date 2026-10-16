package main

import (
	"log"
	"net"

	"github.com/wal-g/wal-g/pkg/pgdebug"
)

func main() {
	l, err := net.Listen("tcp", "127.0.0.1:15432")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("debug server listening on", l.Addr())
	log.Fatal(pgdebug.Serve(l, "wal-g debug 0.1"))
}
