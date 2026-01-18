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
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func NewLowMemoryObjectProvider() *ObjectProvider {
	ctx, cancel := context.WithCancel(context.Background())
	s := &ObjectProvider{
		ch:     make(chan Object),
		ech:    make(chan error, 4),
		ctx:    ctx,
		cancel: cancel,
	}

	tracelog.DebugLogger.Println("ObjectProvider: Created new provider")
	return s
}

func (p *ObjectProvider) GetObject(ctx context.Context) (Object, error) {
	select {
	case <-ctx.Done():
		tracelog.DebugLogger.Println("ObjectProvider.GetObject: Context cancelled (caller)")
		return nil, ctx.Err()
	case <-p.ctx.Done():
		tracelog.DebugLogger.Println("ObjectProvider.GetObject: Provider closed")
		return nil, ErrProviderClosed
	case o, ok := <-p.ch:
		if !ok {
			tracelog.DebugLogger.Println("ObjectProvider.GetObject: Channel closed, no more objects")
			return nil, ErrNoMoreObjects
		}
		tracelog.DebugLogger.Printf("ObjectProvider.GetObject: Got object %s", o.GetName())
		return o, nil
	case err := <-p.ech:
		tracelog.DebugLogger.Printf("ObjectProvider.GetObject: Received error: %v", err)
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()

		if err == nil || err == ErrProviderClosed {
			return p.GetObject(ctx)
		}
		return nil, err
	}
}

func (p *ObjectProvider) AddObject(ctx context.Context, o Object) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		tracelog.DebugLogger.Println("ObjectProvider.AddObject: Provider already closed")
		return ErrProviderClosed
	}
	p.mu.RUnlock()

	tracelog.DebugLogger.Printf("ObjectProvider.AddObject: Attempting to add %s", o.GetName())

	select {
	case <-ctx.Done():
		tracelog.DebugLogger.Printf("ObjectProvider.AddObject: Context cancelled while adding %s", o.GetName())
		return ctx.Err()
	case <-p.ctx.Done():
		tracelog.DebugLogger.Printf("ObjectProvider.AddObject: Provider closed while adding %s", o.GetName())
		return ErrProviderClosed
	case p.ch <- o:
		tracelog.DebugLogger.Printf("ObjectProvider.AddObject: Successfully added %s", o.GetName())
		return nil
	case err := <-p.ech:
		tracelog.DebugLogger.Printf("ObjectProvider.AddObject: Received error: %v", err)
		return err
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

func (p *ObjectProvider) HandleError(err error) {
	if err == nil {
		return
	}
	_ = p.AddError(err)
}

func (p *ObjectProvider) ObjectsCount() int {
	return len(p.ch)
}

func (p *ObjectProvider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		tracelog.DebugLogger.Println("ObjectProvider.Close: Already closed")
		return
	}

	tracelog.InfoLogger.Printf("ObjectProvider.Close: Closing provider (pending objects: %d)", len(p.ch))
	p.closed = true

	p.cancel()

	close(p.ch)

	select {
	case p.ech <- ErrProviderClosed:
		tracelog.DebugLogger.Println("ObjectProvider.Close: Sent ErrProviderClosed to error channel")
	default:
		tracelog.DebugLogger.Println("ObjectProvider.Close: Error channel full, could not send ErrProviderClosed")
	}
	close(p.ech)

	tracelog.InfoLogger.Println("ObjectProvider.Close: Provider closed successfully")
}

func (p *ObjectProvider) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}
