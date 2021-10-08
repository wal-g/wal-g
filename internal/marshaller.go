package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	einJSON "github.com/EinKrebs/json"
	"github.com/spf13/viper"
)

var (
	errUnknownSerializer = fmt.Errorf("undefined dto serializer type")
)

type DtoSerializerType string

const (
	RegularJSONSerializer  DtoSerializerType = "json_default"
	StreamedJSONSerializer DtoSerializerType = "json_streamed"
)

type DtoMarshaller interface {
	Marshal(dto interface{}) (io.Reader, error)
}

type DtoUnmarshaller interface {
	Unmarshal(reader io.Reader, dto interface{}) error
}

type DtoSerializer interface {
	DtoMarshaller
	DtoUnmarshaller
}

func NewDtoSerializer() (DtoSerializer, error) {
	switch DtoSerializerType(viper.GetString(SerializerTypeSetting)) {
	case RegularJSONSerializer:
		return RegularJSON{}, nil
	case StreamedJSONSerializer:
		return StreamedJSON{}, nil
	default:
		return nil, errUnknownSerializer
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
