package binary

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/utility"
	"gopkg.in/yaml.v3"
)

type MongodFileConfig struct {
	path   string
	config map[string]interface{}
}

func CreateMongodConfig(path string) (*MongodFileConfig, error) {
	fileData, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read data from "+path)
	}
	configData := map[string]interface{}{}
	err = yaml.Unmarshal(fileData, &configData)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal json from data of "+path)
	}
	return &MongodFileConfig{
		path:   path,
		config: configData,
	}, nil
}

func (mongodFileConfig *MongodFileConfig) GetDBPath() string {
	return mongodFileConfig.Get("storage.dbPath").(string)
}

func (mongodFileConfig *MongodFileConfig) Get(key string) any {
	var result any
	result = mongodFileConfig.config
	var ok bool

	for _, item := range strings.Split(key, ".") {
		result, ok = result.(map[string]any)[item]
		if !ok {
			break
		}
	}
	return result
}

func (mongodFileConfig *MongodFileConfig) SaveConfigToTempFile(keys ...string) (string, error) {
	config := map[string]interface{}{}
	for _, key := range keys {
		config[key] = mongodFileConfig.Get(key)
	}

	jsonData, err := json.Marshal(config)
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal json")
	}

	tempFile, err := os.CreateTemp("", "mongod-*.conf")
	if err != nil {
		return "", errors.Wrap(err, "unable to create temp file")
	}
	defer utility.LoggedClose(tempFile, "close tmp file")

	writeBytes, err := tempFile.Write(jsonData)
	if err != nil {
		return "", errors.Wrap(err, "unable to save data to temp file")
	}
	if writeBytes != len(jsonData) {
		return "", errors.Errorf("not all data are written (%d != %d)", writeBytes, len(jsonData))
	}
	return tempFile.Name(), nil
}
