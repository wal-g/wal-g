package main

import (
	"os"
	_ "flag"
	"github.com/katie31/extract"
	"fmt"
	"log"
	_ "net/http/pprof"
	"runtime/pprof"
)

func main() {
	all := os.Args
	c := all[1]
	f := all[2]
	dir := all[3]
	data := all[4:]

	if c == "-c" {
        f, err := os.Create("12.prof")
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }
	// f := all[1]
	// dir := all[2]
	// data := all[3:]

	ft := extract.FileTarInterpreter{
		//Home:   os.Getenv("HOME"),
		NewDir: dir,
	}

	if f == "-d" || f == "-f" {
		extract.MakeDir(ft.NewDir)
	} else {
		log.Fatalln("Flag Missing")
	}
	
	// np := extract.NOPTarInterpreter{}
	// fmt.Println("NOP Go Routines: ", extract.ExtractAll(&np, data, f))

	fmt.Println("File Go Routines: ", extract.ExtractAll(&ft, data, f))
	log.Printf("Uncompressed: %v", extract.Uncompressed)
	log.Printf("Compressed: %v", extract.Compressed)
	log.Printf("Ratio: %.2f%%", (float64(extract.Compressed) / float64(extract.Uncompressed)) * float64(100))

}

