package main

import (
	"flag"
	"fmt"
	"github.com/katie31/extract"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
)

var profile bool
var remote bool
var noOp bool
var data []string

func init() {
	flag.BoolVar(&profile, "p", false, "Profiler (false on default)")
	flag.BoolVar(&remote, "d", false, "File or remote (file on default)")
	flag.BoolVar(&noOp, "n", false, "Write to file (write on default)")

}

func main() {
	flag.Parse()
	all := flag.Args()
	dir := all[0]
	data := all[1:]

	if profile {
		f, err := os.Create("sample.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if !noOp {
		ft := extract.FileTarInterpreter{
			NewDir: dir,
		}

		extract.MakeDir(ft.NewDir)

		fmt.Println("File Go Routines: ", extract.ExtractAll(&ft, data, remote))
		log.Printf("Uncompressed: %v", extract.Uncompressed)
		log.Printf("Compressed: %v", extract.Compressed)
		log.Printf("Ratio: %.2f%%", (float64(extract.Compressed)/float64(extract.Uncompressed))*float64(100))
	} else {
		np := extract.NOPTarInterpreter{}
		fmt.Println("NOP Go Routines: ", extract.ExtractAll(&np, data, remote))
		log.Printf("Uncompressed: %v", extract.Uncompressed)
		log.Printf("Compressed: %v", extract.Compressed)
		log.Printf("Ratio: %.2f%%", (float64(extract.Compressed)/float64(extract.Uncompressed))*float64(100))
	}
}
