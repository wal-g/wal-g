package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/katie31/wal-g"
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
	flag.BoolVar(&noOp, "n", false, "NOP extractor (write on default)")

}

func main() {
	flag.Parse()
	all := flag.Args()
	dir := all[0]
	data := all[1:]

	out := make([]walg.ReaderMaker, len(data))
	for i, val := range data {
		if strings.HasPrefix(val, "https://") {
			tls := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}

			h := &walg.HttpReaderMaker{
				Client:     &http.Client{Transport: tls},
				Path:       val,
				FileFormat: walg.CheckType(val),
			}

			out[i] = h
		} else {
			f := &walg.FileReaderMaker{
				Path:       val,
				FileFormat: walg.CheckType(val),
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
		ft := walg.FileTarInterpreter{
			NewDir: dir,
		}

		walg.MakeDir(ft.NewDir)

		fmt.Println("File Go Routines: ", walg.ExtractAll(&ft, out))
		if mem {
			f, err := os.Create("mem.prof")
			if err != nil {
				log.Fatal(err)
			}

			pprof.WriteHeapProfile(f)
			f.Close()
		}
	} else {
		np := walg.NOPTarInterpreter{}
		fmt.Println("NOP Go Routines: ", walg.ExtractAll(&np, out))

	}

	fmt.Printf("Uncompressed: %v\n", walg.Uncompressed)
	fmt.Printf("Compressed: %v\n", walg.Compressed)
	fmt.Printf("Ratio: %.2f%%\n", (float64(walg.Compressed)/float64(walg.Uncompressed))*float64(100))

}
