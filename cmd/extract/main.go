package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/katie31/extract"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
)

var profile bool
var mem bool
var noOp bool
var data []string

func init() {
	flag.BoolVar(&profile, "p", false, "Profiler (false on default)")
	flag.BoolVar(&mem, "m", false, "Memory profiler (false on default)")
	flag.BoolVar(&noOp, "n", false, "Write to file (write on default)")

}

func main() {
	flag.Parse()
	all := flag.Args()
	dir := all[0]
	data := all[1:]

	out := make([]extract.ReaderMaker, len(data))
	for i, val := range data {
		if strings.HasPrefix(val, "https://") {
			tls := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}

			h := &extract.HttpReaderMaker{
				Client: &http.Client{Transport: tls},
				Path:   val,
			}

			out[i] = h
		} else {
			f := &extract.FileReaderMaker{
				Path: val,
			}
			out[i] = f
		}
	}

	if profile {
		f, err := os.Create("cpu.prof")
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

		fmt.Println("File Go Routines: ", extract.ExtractAll(&ft, out))
		if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		f.Close()
	}
	} else {
		np := extract.NOPTarInterpreter{}
		fmt.Println("NOP Go Routines: ", extract.ExtractAll(&np, out))

	}

	fmt.Printf("Uncompressed: %v\n", extract.Uncompressed)
	fmt.Printf("Compressed: %v\n", extract.Compressed)
	fmt.Printf("Ratio: %.2f%%\n", (float64(extract.Compressed)/float64(extract.Uncompressed))*float64(100))

	
}
