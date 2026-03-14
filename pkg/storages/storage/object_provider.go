package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/wal-g/tracelog"
)

var (
	ErrNoMoreObjects  = errors.New("no more objects")
	ErrProviderClosed = errors.New("provider closed")
)

type ObjectProvider struct {
	ch     chan Object
	ech    chan error
	err    error
	closed bool
	mu     sync.Mutex
}

func NewLowMemoryObjectProvider() *ObjectProvider {
	return &ObjectProvider{
		ch:  make(chan Object),
		ech: make(chan error, 1),
	}
}

func (p *ObjectProvider) GetObject(ctx context.Context) (Object, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case o, ok := <-p.ch:
		if !ok {
			return nil, ErrNoMoreObjects
		}
		return o, nil
	case err := <-p.ech:
		return nil, err
	}
}

func (p *ObjectProvider) AddObject(ctx context.Context, o Object) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrProviderClosed
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.ch <- o:
		return nil
	case err := <-p.ech:
		return err
	}
}

func (p *ObjectProvider) HandleError(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	select {
	case p.ech <- err:
	default:
	}
}

func (p *ObjectProvider) AddError(err error) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil || p.closed {
		tracelog.DebugLogger.Printf("ObjectProvider.AddError: Cannot add error (closed=%v, existing_err=%v)", p.closed, p.err)
		return false
	}
	if err == nil {
		return true
	}

	tracelog.DebugLogger.Printf("ObjectProvider.AddError: Adding error: %v", err)
	select {
	case p.ech <- err:
		return true
	default:
		tracelog.WarningLogger.Printf("ObjectProvider.AddError: Error channel full, dropping error: %v", err)
		return false
	}
}

func (p *ObjectProvider) ObjectsCount() int {
	return len(p.ch)
}

func (p *ObjectProvider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.ch)
}
