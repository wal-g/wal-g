package wal_parser

import (
	"io"
	"encoding/binary"
	"github.com/pkg/errors"
)

var PaddingByte FieldToParse

func init() {
	var paddingByte uint8
	PaddingByte = FieldToParse{&paddingByte, "padding byte"}
}

type FieldToParse struct {
	field interface{}
	name  string
}

func (fieldToParse *FieldToParse) parseFrom(reader io.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, fieldToParse.field)
	if err != nil {
		return errors.Wrapf(err, "FieldToParse: failed to parse field '%v'", fieldToParse.name)
	}
	return nil
}

func parseMultipleFieldsFromReader(fields []FieldToParse, reader io.Reader) error {
	for _, field := range fields {
		err := field.parseFrom(reader)
		if err != nil {
			return err
		}
	}
	return nil
}
