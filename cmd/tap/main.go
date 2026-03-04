package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/health"
	"github.com/evalops/ensemble-tap/internal/ingress"
	"github.com/evalops/ensemble-tap/internal/publish"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type readiness interface {
	Ready() error
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to Tap config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	metrics := health.NewMetrics()
	publisher, err := publish.NewNATSPublisher(ctx, cfg.NATS, metrics)
	if err != nil {
		logger.Error("initialize nats publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	clickhouseSink, err := publish.NewClickHouseSink(ctx, cfg.ClickHouse, cfg.NATS, publisher.JetStream(), metrics)
	if err != nil {
		logger.Error("initialize clickhouse sink", "error", err)
		os.Exit(1)
	}
	if clickhouseSink != nil {
		if err := clickhouseSink.Start(ctx); err != nil {
			logger.Error("start clickhouse sink", "error", err)
			os.Exit(1)
		}
		defer clickhouseSink.Close()
	}

	ingressServer := ingress.NewServer(cfg, publisher, metrics, logger)
	webhookRoutes := ingressServer.Routes()

	mux := http.NewServeMux()
	mux.Handle("/", webhookRoutes)
	mux.Handle("GET /livez", health.LivenessHandler())
	mux.Handle("GET /readyz", health.ReadinessHandler(func() error {
		if rd, ok := any(publisher).(readiness); ok {
			return rd.Ready()
		}
		return nil
	}))
	mux.Handle("GET /metrics", promhttp.Handler())

	httpServer := ingressServer.HTTPServer(mux)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("ensemble-tap started", "addr", httpServer.Addr, "base_path", cfg.Server.BasePath)
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := ingressServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown http server", "error", err)
	}
	publisher.WaitForClosed(3 * time.Second)

	logger.Info("ensemble-tap stopped")
	fmt.Println("shutdown complete")
}
