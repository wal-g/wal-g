package storage

import (
	"errors"
)

var (
	ErrNoMoreObjects  = errors.New("no more objects")
	ErrProviderClosed = errors.New("provider closed")
)

type ObjectProvider struct {
	ch  chan Object
	ech chan error
	err error
}

func NewLowMemoryObjectProvider() *ObjectProvider {
	s := new(ObjectProvider)

	s.ch = make(chan Object)
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
		if p.err == nil || p.err == ErrProviderClosed {
			return p.GetObject()
		}
		return nil, p.err
	}
}

func (p *ObjectProvider) AddObject(o Object) error {
	select {
	case p.ch <- o:
		return nil
	case err := <-p.ech:
		return err
	}
}

func (p *ObjectProvider) AddError(err error) bool {
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

func (p *ObjectProvider) HandleError(err error) {
	if err == nil {
		return
	}
	ok := p.AddError(err)
	for !ok {
		ok = p.AddError(err)
	}
}

func (p *ObjectProvider) ObjectsCount() int {
	return len(p.ch)
}

func (p *ObjectProvider) Close() {
	close(p.ch)
	select {
	case p.ech <- ErrProviderClosed:
	default:
	}
	close(p.ech)
}
