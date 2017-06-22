package main

import (
	"os"
	"flag"
	"github.com/katie31/extract"
)

func checkArgs() bool {
	
}

func main() {
	flag.Parse()
	args := flag.Args()
	dir := args[0]
	f := args[1:]



	// all := os.Args
	// flag := all[1]
	// dir := all[2]
	// f := all[3:]

	ft := FileTarInterpreter{
		home:   os.Getenv("HOME"),
		newDir: dir,
	}

	if flag == "-d" || flag == "-f" {
		extract.MakeDir(ft.home, ft.newDir)
	} else {
		log.Fatalln("Flag Missing")
	}
	
	np := NOPTarInterpreter{}

	fmt.Println("NOP Go Routines: ", extract.ExtractAll(&np, f, flag))
	fmt.Println("File Go Routines: ", extract.ExtractAll(&ft, f, flag))

}

