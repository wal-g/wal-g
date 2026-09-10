package binary

import (
	"context"
	"time"

	"github.com/wal-g/tracelog"
)

type supervisedMongod interface {
	URI() string
	Done() <-chan struct{}
	Wait() error
	Connect(context.Context) error
	Shutdown(context.Context) error
	Stop()
	Close()
}

type managedMongod struct {
	process *MongodProcess
	service *MongodService
	done    chan struct{}
	waitErr error
}

func startManagedMongod(
	ctx context.Context,
	minimalConfigPath string,
	replayArgs ReplyOplogConfig,
) (supervisedMongod, error) {
	process := Mongod(minimalConfigPath)
	if !replayArgs.WithCatchUpReconfig {
		process.WithParams(DisableLogicalSessionCacheRefresh, TakeUnstableCheckpointOnShutdown)
	}
	if replayArgs.Partial {
		process.WithRestore()
	}
	if _, err := process.Start(ctx); err != nil {
		return nil, err
	}

	mongod := &managedMongod{process: process, done: make(chan struct{})}
	go func() {
		mongod.waitErr = process.Wait()
		close(mongod.done)
	}()
	return mongod, nil
}

func (m *managedMongod) URI() string {
	return m.process.GetURI()
}

func (m *managedMongod) Done() <-chan struct{} {
	return m.done
}

func (m *managedMongod) Wait() error {
	<-m.done
	return m.waitErr
}

func (m *managedMongod) Connect(ctx context.Context) error {
	service, err := CreateMongodService(ctx, "wal-g oplog replay", m.URI(), 10*time.Minute)
	if err != nil {
		return err
	}
	m.service = service
	return nil
}

func (m *managedMongod) Shutdown(ctx context.Context) error {
	return m.service.Shutdown(ctx)
}

func (m *managedMongod) Stop() {
	select {
	case <-m.done:
	default:
		m.process.Close()
	}
}

func (m *managedMongod) Close() {
	m.Stop()
	_ = m.Wait()
	if m.service == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), mongoDisconnectTimeout)
	defer cancel()
	if err := m.service.MongoClient.Disconnect(ctx); err != nil {
		tracelog.WarningLogger.Printf("Unable to disconnect supervised mongod client: %v", err)
	}
}
