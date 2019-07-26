package parsingutil

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFieldToParse_ParseFrom(t *testing.T) {
	var x uint16
	err := NewFieldToParse(&x, "x").ParseFrom(bytes.NewReader([]byte{0xAB, 0xCD}))
	assert.NoError(t, err)
	assert.Equal(t, x, uint16(0xCDAB))
}

func TestFieldToParse_ErrorWhileParsing(t *testing.T) {
	var x uint32
	err := NewFieldToParse(&x, "x").ParseFrom(bytes.NewReader([]byte{1, 2, 3}))
	assert.Error(t, err)
}

func TestFieldToParse_ParseMultipleFieldsFromReader(t *testing.T) {
	var x uint16
	var y uint32
	var z uint16
	data := []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0x00, 0xDE, 0xF0}
	var paddingByte uint8
	PaddingByte := NewFieldToParse(&paddingByte, "padding byte")
	err := ParseMultipleFieldsFromReader([]FieldToParse{
		{&x, "x"},
		{&y, "y"},
		*PaddingByte,
		{&z, "z"},
	}, bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, x, uint16(0x3412))
	assert.Equal(t, y, uint32(0xBC9A7856))
	assert.Equal(t, z, uint16(0xF0DE))
}
