package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

type Calculator struct {
	AlgorithmName string
	Hash          hash.Hash
}

func CreateCalculator() *Calculator {
	return &Calculator{
		AlgorithmName: "sha256",
		Hash:          sha256.New(),
	}
}

func (calculator *Calculator) AddData(data []byte) {
	_, _ = calculator.Hash.Write(data)
}

func (calculator *Calculator) Algorithm() string {
	return calculator.AlgorithmName
}

func (calculator *Calculator) Checksum() string {
	checksum := calculator.Hash.Sum(nil)
	return hex.EncodeToString(checksum)
}
