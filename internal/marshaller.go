package internal

import (
	"bytes"
	"encoding/json"
	"io"

	einJson "github.com/EinKrebs/json"
)

type DtoMarshallerType int

const (
	RegularJSONMarshaller DtoMarshallerType = iota + 1
	StreamedJSONMarshaller
)

type DtoUnmarshallerType int

const (
	RegularJSONUnmarshaller DtoUnmarshallerType = iota + 1
	StreamedJSONUnmarshaller
)

type DtoMarshaller interface {
	Marshal(dto interface{}) (io.Reader, error)
}

type DtoUnmarshaller interface {
	Unmarshal(reader io.Reader, dto interface{}) error
}

var _ DtoMarshaller = RegularJSON{}
var _ DtoUnmarshaller = RegularJSON{}

type RegularJSON struct{}

func (r RegularJSON) Marshal(dto interface{}) (io.Reader, error) {
	data, err := json.Marshal(dto)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func (r RegularJSON) Unmarshal(reader io.Reader, dto interface{}) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dto)
}

var _ DtoMarshaller = StreamedJSON{}
var _ DtoUnmarshaller = StreamedJSON{}

type StreamedJSON struct{}

func (s StreamedJSON) Marshal(dto interface{}) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		if err := einJson.Marshal(dto, w); err != nil {
			_ = w.CloseWithError(err)
		}
	}()
	return r, nil
}

func (s StreamedJSON) Unmarshal(reader io.Reader, dto interface{}) error {
	return einJson.Unmarshal(reader, dto)
}
