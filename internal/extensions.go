package internal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"plugin"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var Extensions []Extension

type Extension interface {
	RegisterCommands(cmd *cobra.Command)
	GetAllowedConfigKeys() map[string]*string
}

func loadExtensions(path string) error {
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

func registerExtensionCommands(rootCmd *cobra.Command) {
	for _, extension := range Extensions {
		extension.RegisterCommands(rootCmd)
	}
}
