package storage

import "errors"

var (
	ErrNoMoreObjects  = errors.New("no more objects")
	ErrProviderClosed = errors.New("provider closed")
)

type ObjectProvider struct {
	ch  chan Object
	ech chan error
	err error
}

func NewObjectProvider() *ObjectProvider {
	s := new(ObjectProvider)

	s.ch = make(chan Object, 80)
	s.ech = make(chan error, 4)

	return s
}

func (p *ObjectProvider) GetObject() (Object, error) {
	select {
	case o, ok := <-p.ch:
		if !ok {
			return nil, ErrNoMoreObjects
		}
		return o, nil
	case p.err = <-p.ech:
		// if chanel is closed, p.err will be nil
		if p.err == nil {
			return p.GetObject()
		}
		return nil, p.err
	}
}

func (p *ObjectProvider) AddObjectToProvider(o Object) error {
	select {
	case p.ch <- o:
		return nil
	case err := <-p.ech:
		return err
	default:
		return ErrProviderClosed
	}
}

func (p *ObjectProvider) AddErrorToProvider(err error) bool {
	if p.err != nil {
		return false
	}
	if err == nil {
		return true
	}
	select {
	case p.ech <- err:
		return true
	default:
		return false
	}
}

func (p *ObjectProvider) ObjectsCount() int {
	return len(p.ch)
}

func (p *ObjectProvider) Close() {
	close(p.ch)
	close(p.ech)
	p.err = ErrProviderClosed
}
