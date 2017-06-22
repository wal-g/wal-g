package main

import (
	"os"
	_ "flag"
	"github.com/katie31/extract"
	"fmt"
	"log"
)

func main() {
	all := os.Args
	f := all[1]
	dir := all[2]
	data := all[3:]

	ft := extract.FileTarInterpreter{
		Home:   os.Getenv("HOME"),
		NewDir: dir,
	}

	if f == "-d" || f == "-f" {
		extract.MakeDir(ft.Home, ft.NewDir)
	} else {
		log.Fatalln("Flag Missing")
	}
	
	np := extract.NOPTarInterpreter{}

	fmt.Println("NOP Go Routines: ", extract.ExtractAll(&np, data, f))
	fmt.Println("File Go Routines: ", extract.ExtractAll(&ft, data, f))

}

