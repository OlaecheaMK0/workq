package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/OlaecheaMK0/workq/internal/httpapi"
	"github.com/OlaecheaMK0/workq/internal/queue"
	"github.com/OlaecheaMK0/workq/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("workq stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	if len(os.Args) != 2 {
		return errors.New("usage: workq api|worker|migrate")
	}
	mode := os.Args[1]
	if mode != "api" && mode != "worker" && mode != "migrate" {
		return errors.New("usage: workq api|worker|migrate")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	store, err := queue.Open(startCtx, dsn)
	if err != nil {
		cancel()
		return errors.New("cannot connect to database; check configuration and availability")
	}
	defer store.Close()
	err = store.Migrate(startCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("bootstrap schema: %w", err)
	}
	if mode == "migrate" {
		log.Info("schema ready")
		return nil
	}
	if mode == "worker" {
		lease, err := duration("LEASE_DURATION", 5*time.Second)
		if err != nil {
			return err
		}
		poll, err := duration("POLL_INTERVAL", 250*time.Millisecond)
		if err != nil {
			return err
		}
		base, err := duration("RETRY_BASE", 500*time.Millisecond)
		if err != nil {
			return err
		}
		max, err := duration("RETRY_MAX", 30*time.Second)
		if err != nil {
			return err
		}
		timeout, err := duration("JOB_TIMEOUT", 30*time.Second)
		if err != nil {
			return err
		}
		concurrency := 2
		if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
			concurrency, err = strconv.Atoi(raw)
			if err != nil {
				return errors.New("invalid WORKER_CONCURRENCY")
			}
		}
		log.Info("worker starting", "concurrency", concurrency, "lease", lease)
		w := worker.Worker{Queue: store, Handle: worker.ReportHandler(store), Log: log, Lease: lease, Poll: poll, RetryBase: base, RetryMax: max, Timeout: timeout}
		return w.Run(ctx, concurrency)
	}
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	api := httpapi.API{Store: store, Token: os.Getenv("API_TOKEN"), Log: log}
	server := &http.Server{Addr: addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	result := make(chan error, 1)
	go func() { result <- server.ListenAndServe() }()
	log.Info("API starting", "address", addr, "authentication_enabled", api.Token != "")
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func duration(name string, fallback time.Duration) (time.Duration, error) {
	if raw := os.Getenv(name); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("invalid %s", name)
		}
		return d, nil
	}
	return fallback, nil
}
