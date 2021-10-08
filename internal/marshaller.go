package internal

import (
	"bytes"
	"encoding/json"
	"io"

	einJson "github.com/EinKrebs/json"
)

type DtoMarshallerType int

const (
	RegularJsonMarshaller  DtoMarshallerType = iota + 1
	StreamedJsonMarshaller DtoMarshallerType = iota + 2
)

type DtoUnmarshallerType int

const (
	RegularJsonUnmarshaller DtoUnmarshallerType = iota + 1
	StreamedJsonUnmarshaller DtoUnmarshallerType = iota + 2
)

type DtoMarshaller interface {
	Marshal(dto interface{}) (io.Reader, error)
}

type DtoUnmarshaller interface {
	Unmarshal(reader io.Reader, dto interface{}) error
}

var _ DtoMarshaller = RegularJson{}
var _ DtoUnmarshaller = RegularJson{}

type RegularJson struct {}

func (r RegularJson) Marshal(dto interface{}) (io.Reader, error) {
	data, err := json.Marshal(dto)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func (r RegularJson) Unmarshal(reader io.Reader, dto interface{}) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dto)
}

var _ DtoMarshaller = StreamedJson{}
var _ DtoUnmarshaller = StreamedJson{}

type StreamedJson struct {}

func (s StreamedJson) Marshal(dto interface{}) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		if err := einJson.Marshal(dto, w); err != nil {
			_ = w.CloseWithError(err)
		}
	}()
	return r, nil
}

func (s StreamedJson) Unmarshal(reader io.Reader, dto interface{}) error {
	return einJson.Unmarshal(reader, dto)
}

