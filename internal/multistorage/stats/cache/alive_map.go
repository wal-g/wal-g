package cache

// AliveMap shows which storages are dead and which are alive, by their names.
type AliveMap map[string]bool

// FirstAlive traverses storages in order of priority. It returns the first storage from the ordered list, that is alive
// in AliveMap and there is no preceding storage that is NOT found in AliveMap. If there is no such storage, e.g. all
// storages are dead, or any storage preceding the alive one is not presented in AliveMap, nil is returned.
func (am AliveMap) FirstAlive(namesInOrder []string) *string {
	for _, name := range namesInOrder {
		alive, ok := am[name]
		if !ok {
			return nil
		}
		if alive {
			return &name
		}
	}
	return nil
}

func (am AliveMap) AliveNames(namesInOrder []string) []string {
	aliveNames := make([]string, 0, len(am))
	for _, name := range namesInOrder {
		if alive, ok := am[name]; ok && alive {
			aliveNames = append(aliveNames, name)
		}
	}
	return aliveNames
}

func (am AliveMap) Names() []string {
	names := make([]string, 0, len(am))
	for name := range am {
		names = append(names, name)
	}
	return names
}
