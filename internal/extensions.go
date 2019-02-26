package internal

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"plugin"
)

var Extensions []Extension

type Extension interface {
	TryPrintHelp(command, firstArgument string) bool
	HasCommand(command string) bool
	Execute(command string, uploader *Uploader, folder StorageFolder, args []string)
	HasAllowedConfigKey(key string) bool
	Flush(time BackupTime, folder StorageFolder)
}

func LoadExtensions() {
	files, err := ioutil.ReadDir("extensions")
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".so" {
			continue
		}
		plug, err := plugin.Open("./extensions/" + file.Name())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		symExtension, err := plug.Lookup("Extension")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		var extension Extension
		extension, ok := symExtension.(Extension)
		if !ok {
			fmt.Println("unexpected type from module symbol")
			os.Exit(1)
		}
		Extensions = append(Extensions, extension)
	}
}

func GetExtensionByCommandName(commandName string) (Extension, bool) {
	for _, extension := range Extensions {
		if extension.HasCommand(commandName) {
			return extension, true
		}
	}
	return nil, false
}
