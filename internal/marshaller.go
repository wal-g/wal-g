package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/wal-g/tracelog"
	"io"
	"io/ioutil"

	einJSON "github.com/EinKrebs/json"
	"github.com/spf13/viper"
)

type UnknownSerializerTypeError struct {
	error
}

func NewUnknownSerializerTypeError(serializerType DtoSerializerType) UnknownSerializerTypeError {
	return UnknownSerializerTypeError{fmt.Errorf("undefined dto serializer type: %s", serializerType)}
}

func (err UnknownSerializerTypeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type DtoSerializerType string

const (
	RegularJSONSerializer  DtoSerializerType = "json_default"
	StreamedJSONSerializer DtoSerializerType = "json_streamed"
)

type DtoSerializer interface {
	Marshal(dto interface{}) (io.Reader, error)
	Unmarshal(reader io.Reader, dto interface{}) error
}

func NewDtoSerializer() (DtoSerializer, error) {
	switch t := DtoSerializerType(viper.GetString(SerializerTypeSetting)); t {
	case RegularJSONSerializer:
		return RegularJSON{}, nil
	case StreamedJSONSerializer:
		return StreamedJSON{}, nil
	default:
		return nil, NewUnknownSerializerTypeError(t)
	}
}

var _ DtoSerializer = RegularJSON{}

type RegularJSON struct{}

func (r RegularJSON) Marshal(dto interface{}) (io.Reader, error) {
	data, err := json.Marshal(dto)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func (r RegularJSON) Unmarshal(reader io.Reader, dto interface{}) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dto)
}

var _ DtoSerializer = StreamedJSON{}

type StreamedJSON struct{}

func (s StreamedJSON) Marshal(dto interface{}) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		if err := einJSON.Marshal(dto, w); err != nil {
			_ = w.CloseWithError(err)
		}
	}()
	return r, nil
}

func (s StreamedJSON) Unmarshal(reader io.Reader, dto interface{}) error {
	return einJSON.Unmarshal(reader, dto)
}
