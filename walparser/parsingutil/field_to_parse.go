package parsingutil

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"io"
)

var PaddingByte FieldToParse

func init() {
	var paddingByte uint8
	PaddingByte = FieldToParse{&paddingByte, "padding byte"}
}

type FieldToParse struct {
	Field interface{}
	Name  string
}

func NewFieldToParse(field interface{}, name string) *FieldToParse {
	return &FieldToParse{field, name}
}

func (fieldToParse *FieldToParse) ParseFrom(reader io.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, fieldToParse.Field)
	return errors.Wrapf(err, "FieldToParse: failed to parse field '%v'", fieldToParse.Name)
}

func ParseMultipleFieldsFromReader(fields []FieldToParse, reader io.Reader) error {
	for _, field := range fields {
		err := field.ParseFrom(reader)
		if err != nil {
			return err
		}
	}
	return nil
}
