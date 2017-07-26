package walg_test

import (
	"github.com/katie31/wal-g"
	"fmt"
	"github.com/katie31/wal-g/prototype"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func setUp(t *testing.T) string{
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	/*** Create temp directory. ***/
	dir, err := ioutil.TempDir(cwd, "data")
	if err != nil {
		t.Log(err)
	}
	fmt.Println(dir)

	//defer os.RemoveAll(dir)

	sb := prototype.NewStrideByteReader(10)
	lr := &io.LimitedReader{sb, int64(100)}
	

	for i := 1; i < 6; i++ {
		lr = &io.LimitedReader{sb, int64(100)}
		f, err := os.Create(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Log(err)
		}
		io.Copy(f, lr)
		f.Close()
	}

	err = os.MkdirAll(filepath.Join(dir,"global"), 0700)
	if err != nil {
		t.Log(err)
	}
	f, err := os.Create(filepath.Join(dir, "global", "pg_control"))
	if err != nil {
		t.Log(err)
	}
	err = f.Chmod(0600) 
	if err != nil {
		t.Log(err)
	}
	
	f.Close()
	return dir
}

func TestWalk(t *testing.T) {
	dir := setUp(t)
	bundle := &walg.Bundle{
		MinSize: int64(10),
	}
	outDir := filepath.Join(filepath.Dir(dir), "compressed")
	bundle.Tbm = &prototype.FileTarBallMaker{
		BaseDir: filepath.Base(dir),
		Trim: dir,
		Out: outDir,
	}
	err := os.MkdirAll(outDir, 0766)
	if err != nil {
		t.Log(err)
	}

	bundle.NewTarBall()
	fmt.Println("Walking ...")
	err = filepath.Walk(dir, bundle.TarWalker)
	if err != nil {
		panic(err)
	}
	err = bundle.Tb.CloseTar()
	if err != nil {
		panic(err)
	}
	bundle.Tb.Finish()

	sen := bundle.Sen.Info.Name()
	if sen != "pg_control" {
		t.Errorf("walk: Sentinel expected %s but got %s", "pg_control", sen)
	}

	defer os.RemoveAll(dir)
	defer os.RemoveAll(outDir)
}
