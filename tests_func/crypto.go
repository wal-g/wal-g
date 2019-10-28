package main

import (
	//"golang.org/x/crypto/nacl/box"
	//crypto_rand "crypto/rand"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/packet"
	"math/rand"
)

func genGPG(name string, email string) *openpgp.Entity {
	entity, err := openpgp.NewEntity(name, "", email, &packet.Config{})
	if err != nil {
		panic(err)
	}
	return entity
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
