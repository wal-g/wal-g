package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"io/ioutil"
	"os"
	"path/filepath"
	"plugin"
)

var Extensions []Extension

type Extension interface {
	TryPrintHelp(command string, args []string) bool
	HasCommand(command string) bool
	Execute(command string, uploader *Uploader, folder storage.Folder, args []string)
	GetAllowedConfigKeys() map[string]*string
	Flush(time BackupTime, folder storage.Folder)
}

func LoadExtensions(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".so" {
			continue
		}
		plug, err := plugin.Open(filepath.Join(path, file.Name()))
		if err != nil {
			return errors.Wrap(err, "can't open plugin")
		}

		symExtension, err := plug.Lookup("Extension")
		if err != nil {
			return errors.Wrap(err, "can't find symbol Extension in plugin")
		}
		var extension Extension
		extension, ok := symExtension.(Extension)
		if !ok {
			return errors.New("unexpected type from module symbol")
		}
		Extensions = append(Extensions, extension)
	}
	return nil
}

func GetExtensionByCommandName(commandName string) (Extension, bool) {
	for _, extension := range Extensions {
		if extension.HasCommand(commandName) {
			return extension, true
		}
	}
	return nil, false
}
