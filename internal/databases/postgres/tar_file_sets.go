package postgres

type TarFileSets interface {
	AddFile(name string, file string)
	AddFiles(name string, files []string)
	Get() map[string][]string
}

type RegularTarFileSets map[string][]string

func NewRegularTarFileSets() *RegularTarFileSets {
	return &RegularTarFileSets{}
}

func (tarFileSets *RegularTarFileSets) AddFile(name string, file string) {
	(*tarFileSets)[name] = append((*tarFileSets)[name], file)
}

func (tarFileSets *RegularTarFileSets) AddFiles(name string, files []string) {
	(*tarFileSets)[name] = append((*tarFileSets)[name], files...)
}

func (tarFileSets *RegularTarFileSets) Get() map[string][]string {
	return *tarFileSets
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

func (tarFileSets *NopTarFileSets) Get() map[string][]string {
	return make(map[string][]string)
}
