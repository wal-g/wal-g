package main

import (
	"fmt"
	"github.com/wal-g/wal-g"
	"os"
	"io"
)

type ZeroWriter struct {

}

func (b *ZeroWriter) Write(p []byte) (n int, err error) {
	return len(p),nil
}


func main() {
	id := walg.GetKeyRingId()
	fmt.Printf("Keyring ID: %v\n", id)
	var armour, err = walg.GetPubRingArmour(id)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("Pubkey armour: %v\n", string(armour))

	armour, err = walg.GetSecretRingArmour(id)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("Secret armour: %v\n", string(armour))

	var c walg.Crypter
	wfile, _ := os.Create("temp.txt")
	var writer io.WriteCloser
	writer,err = c.Encrypt(wfile)
	if err != nil {
		fmt.Println(err.Error())
	}

	writer.Write([]byte{0,1,2,7})

	writer.Close()

	rfile, _ := os.Open("temp.txt")
	var reader io.Reader
	reader,err = c.Decrypt(rfile)
	if err != nil {
		fmt.Println(err.Error())
	}

	bytes:= make([]byte,8)

	n, _ := reader.Read(bytes)

	fmt.Printf("Decrypted %v bytes %v",n, bytes)
}
