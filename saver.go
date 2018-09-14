package walg

import "io"

type  Saver interface {
	save(writer io.Writer) error
}

func saveToDataFolder(saver Saver, filename string, dataFolder *TemporaryDataFolder) error {
	file, err := dataFolder.openWriteOnlyFile(filename)
	if err != nil {
		return err
	}
	err = saver.save(file)
	if err != nil {
		return err
	}
	return file.Close()
}
