package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/OlaecheaMK0/workq/internal/queue"
)

type Handler func(context.Context, queue.Job) (json.RawMessage, error)

type Queue interface {
	Claim(context.Context, time.Duration) (queue.Job, error)
	RecoverExpired(context.Context) (int64, error)
	Heartbeat(context.Context, queue.Job, time.Duration) error
	Complete(context.Context, queue.Job, json.RawMessage) error
	Fail(context.Context, queue.Job, string, time.Duration) error
}

type Worker struct {
	Queue     Queue
	Handle    Handler
	Log       *slog.Logger
	Lease     time.Duration
	Poll      time.Duration
	RetryBase time.Duration
	RetryMax  time.Duration
	Timeout   time.Duration
}

func (w *Worker) Run(ctx context.Context, concurrency int) error {
	if concurrency < 1 || concurrency > 32 {
		return errors.New("concurrency must be between 1 and 32")
	}
	if w.Lease < 300*time.Millisecond || w.Poll <= 0 || w.Timeout <= 0 || w.RetryBase <= 0 || w.RetryMax < w.RetryBase {
		return errors.New("invalid worker timing configuration")
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.loop(ctx) }()
	}
	wg.Wait()
	return nil
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		if recovered, err := w.Queue.RecoverExpired(ctx); err != nil {
			if ctx.Err() == nil {
				w.Log.Error("recover expired leases", "error", err)
			}
		} else if recovered > 0 {
			w.Log.Info("recovered expired leases", "count", recovered)
		}
		job, err := w.Queue.Claim(ctx, w.Lease)
		if err == nil {
			w.execute(ctx, job)
			continue
		}
		if !errors.Is(err, queue.ErrNotFound) && ctx.Err() == nil {
			w.Log.Error("claim job", "error", err)
		}
		timer := time.NewTimer(w.Poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func invoke(ctx context.Context, handle Handler, job queue.Job) (result json.RawMessage, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return handle(ctx, job)
}

func (w *Worker) execute(parent context.Context, job queue.Job) {
	ctx, cancel := context.WithTimeout(parent, w.Timeout)
	heartbeatDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				heartbeatDone <- nil
				return
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(ctx, w.Lease/3)
				err := w.Queue.Heartbeat(hbCtx, job, w.Lease)
				hbCancel()
				if err != nil {
					if ctx.Err() != nil {
						heartbeatDone <- nil
						return
					}
					cancel()
					heartbeatDone <- err
					return
				}
			}
		}
	}()
	result, handlerErr := invoke(ctx, w.Handle, job)
	cancel()
	heartbeatErr := <-heartbeatDone
	if heartbeatErr != nil {
		w.Log.Warn("abandoned job after heartbeat failure", "job_id", job.ID, "error", heartbeatErr)
		return
	}
	// On shutdown, leave the lease for another process to recover. A lost lease
	// can never be completed or failed by this process because writes are fenced.
	if parent.Err() != nil {
		return
	}
	ackCtx, ackCancel := context.WithTimeout(parent, 3*time.Second)
	defer ackCancel()
	var err error
	if handlerErr != nil {
		delay := queue.RetryDelay(job.Attempts, w.RetryBase, w.RetryMax)
		err = w.Queue.Fail(ackCtx, job, handlerErr.Error(), delay)
		w.Log.Info("job failed", "job_id", job.ID, "attempt", job.Attempts, "retry_in", delay, "error", handlerErr)
	} else {
		err = w.Queue.Complete(ackCtx, job, result)
		if err == nil {
			w.Log.Info("job completed", "job_id", job.ID, "attempt", job.Attempts)
		}
	}
	if err != nil {
		w.Log.Warn("job acknowledgment failed", "job_id", job.ID, "error", err)
	}
}
