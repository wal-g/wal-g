package main

import (
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/packet"
	"math/rand"
	"time"
)

func genGPG(name string, email string) *openpgp.Entity {
	entity, err := openpgp.NewEntity(name, "", email, &packet.Config{})
	if err != nil {
		panic(err)
	}
	return entity
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandSeq(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
