package binary

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

const mongodExitDetectionTimeout = time.Second

type oplogReplayAttempt struct {
	checkpoint  stages.ReplayCheckpoint
	progress    *stages.ReplayProgress
	resumeAfter bool
}

type replayAttemptKind uint8

const (
	replayAttemptCompleted replayAttemptKind = iota
	replayAttemptMongodExited
	replayAttemptFailed
	replayAttemptCanceled
)

type replayAttemptResult struct {
	kind replayAttemptKind
	err  error
}

type replayAttemptExecutor struct {
	startMongod        func(context.Context, string, ReplyOplogConfig) (supervisedMongod, error)
	replay             func(context.Context, string, ReplyOplogConfig, oplogReplayAttempt) error
	exitDetectionDelay time.Duration
}

func newReplayAttemptExecutor() replayAttemptExecutor {
	return replayAttemptExecutor{
		startMongod:        startManagedMongod,
		replay:             runOplogReplay,
		exitDetectionDelay: mongodExitDetectionTimeout,
	}
}

func runSupervisedOplogReplay(ctx context.Context, replayArgs ReplyOplogConfig) error {
	progress := stages.NewReplayProgress(replayArgs.Since)
	executor := newReplayAttemptExecutor()
	return superviseOplogReplay(ctx, replayArgs, progress, func(attempt oplogReplayAttempt) replayAttemptResult {
		return executor.run(ctx, replayArgs, attempt, replayArgs.MinimalConfigPath)
	})
}

func superviseOplogReplay(
	ctx context.Context,
	replayArgs ReplyOplogConfig,
	progress *stages.ReplayProgress,
	runAttempt func(oplogReplayAttempt) replayAttemptResult,
) error {
	consecutiveRestarts := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		checkpointBefore := progress.Snapshot()
		result := runAttempt(oplogReplayAttempt{
			checkpoint:  checkpointBefore,
			progress:    progress,
			resumeAfter: checkpointBefore.Generation > 0,
		})

		switch result.kind {
		case replayAttemptCompleted:
			return nil
		case replayAttemptCanceled:
			return result.err
		case replayAttemptFailed:
			return result.err
		case replayAttemptMongodExited:
		default:
			return fmt.Errorf("unknown oplog replay attempt result: %d", result.kind)
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		checkpointAfter := progress.Snapshot()
		if checkpointAfter.Generation > checkpointBefore.Generation {
			consecutiveRestarts = 0
		}
		if consecutiveRestarts >= replayArgs.MaxMongodRestarts {
			return errors.Wrapf(result.err,
				"supervised mongod crashed after %d restarts, last durable oplog timestamp is %s",
				consecutiveRestarts, checkpointAfter.OpTime.TS)
		}
		consecutiveRestarts++
		tracelog.WarningLogger.Printf(
			"supervised mongod crashed, restarting oplog replay after %s, attempt %d/%d",
			checkpointAfter.OpTime.TS, consecutiveRestarts, replayArgs.MaxMongodRestarts)
	}
}

func (e replayAttemptExecutor) run(
	ctx context.Context,
	replayArgs ReplyOplogConfig,
	attempt oplogReplayAttempt,
	minimalConfigPath string,
) replayAttemptResult {
	mongod, err := e.startMongod(ctx, minimalConfigPath, replayArgs)
	if err != nil {
		return classifyAttemptError(ctx,
			errors.Wrap(err, "unable to start mongod in special mode"))
	}
	defer mongod.Close()

	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()

	connectC := make(chan error, 1)
	go func() { connectC <- mongod.Connect(attemptCtx) }()
	select {
	case <-mongod.Done():
		cancelAttempt()
		<-connectC
		return processExitResult(ctx, mongod.Wait())
	case connectErr := <-connectC:
		if connectErr != nil {
			if exited, processErr := exitedProcess(mongod); exited {
				return processExitResult(ctx, processErr)
			}
			return classifyAttemptError(ctx,
				errors.Wrap(connectErr, "unable to create mongod service"))
		}
	}

	replayC := make(chan error, 1)
	go func() { replayC <- e.replay(attemptCtx, mongod.URI(), replayArgs, attempt) }()

	select {
	case <-mongod.Done():
		cancelAttempt()
		<-replayC
		return processExitResult(ctx, mongod.Wait())
	case replayErr := <-replayC:
		if replayErr != nil {
			return e.classifyReplayError(ctx, mongod, replayErr)
		}
	}

	if exited, processErr := exitedProcess(mongod); exited {
		return processExitResult(ctx, processErr)
	}
	if err := mongod.Shutdown(ctx); err != nil {
		if exited, processErr := exitedProcess(mongod); exited {
			return processExitResult(ctx, processErr)
		}
		return classifyAttemptError(ctx, err)
	}
	if err := mongod.Wait(); err != nil {
		return classifyAttemptError(ctx, err)
	}
	return replayAttemptResult{kind: replayAttemptCompleted}
}

func (e replayAttemptExecutor) classifyReplayError(
	ctx context.Context,
	mongod supervisedMongod,
	replayErr error,
) replayAttemptResult {
	if result := classifyAttemptError(ctx, replayErr); result.kind == replayAttemptCanceled {
		return result
	}
	if exited, processErr := exitedProcess(mongod); exited {
		return processExitResult(ctx, processErr)
	}

	timer := time.NewTimer(e.exitDetectionDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return replayAttemptResult{kind: replayAttemptCanceled, err: ctx.Err()}
	case <-mongod.Done():
		return processExitResult(ctx, mongod.Wait())
	case <-timer.C:
		return replayAttemptResult{kind: replayAttemptFailed, err: replayErr}
	}
}

func exitedProcess(mongod supervisedMongod) (bool, error) {
	select {
	case <-mongod.Done():
		return true, mongod.Wait()
	default:
		return false, nil
	}
}

func classifyAttemptError(ctx context.Context, err error) replayAttemptResult {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return replayAttemptResult{kind: replayAttemptCanceled, err: ctxErr}
	}
	return replayAttemptResult{kind: replayAttemptFailed, err: err}
}

func processExitResult(ctx context.Context, err error) replayAttemptResult {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return replayAttemptResult{kind: replayAttemptCanceled, err: ctxErr}
	}
	if err == nil {
		err = fmt.Errorf("supervised mongod exited unexpectedly during oplog replay")
	} else {
		err = errors.Wrap(err, "supervised mongod exited during oplog replay")
	}
	return replayAttemptResult{kind: replayAttemptMongodExited, err: err}
}
