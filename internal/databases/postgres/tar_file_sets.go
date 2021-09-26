package postgres

type TarFileSets interface {
	AddFile(name string, file string)
	AddFiles(name string, files []string)
	GetFiles() map[string][]string
}

type RegularTarFileSets struct {
	files map[string][]string
}

func NewRegularTarFileSets() *RegularTarFileSets {
	return &RegularTarFileSets{
		files: make(map[string][]string),
	}
}

func (tarFileSets *RegularTarFileSets) AddFile(name string, file string) {
	tarFileSets.files[name] = append(tarFileSets.files[name], file)
}

func (tarFileSets *RegularTarFileSets) AddFiles(name string, files []string) {
	tarFileSets.files[name] = append(tarFileSets.files[name], files...)
}

func (tarFileSets *RegularTarFileSets) GetFiles() map[string][]string {
	return tarFileSets.files
}

type NopTarFileSets struct {
}

func NewNopTarFileSets() *NopTarFileSets {
	return &NopTarFileSets{}
}

func (tarFileSets *NopTarFileSets) AddFile(name string, file string) {
}

func (tarFileSets *NopTarFileSets) AddFiles(name string, files []string) {
}

func (tarFileSets *NopTarFileSets) GetFiles() map[string][]string {
	return make(map[string][]string)
}
