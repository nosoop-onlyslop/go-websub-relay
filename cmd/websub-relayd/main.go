// Command websub-relayd runs the WebSub relay daemon.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nosoop-onlyslop/go-websub-relay/config"
	"github.com/nosoop-onlyslop/go-websub-relay/relay"
	"github.com/nosoop-onlyslop/go-websub-relay/server"
)

func main() {
	if err := run(); err != nil {
		slog.Error("websub-relayd failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "relay.toml", "path to relay TOML config")
	logLevel := flag.String("log-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")
	flag.Parse()

	logger := newLogger(*logLevel)
	slog.SetDefault(logger)

	relayConfig, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	service, err := relay.New(relayConfig, logger)
	if err != nil {
		return err
	}
	if err := service.Start(ctx); err != nil {
		service.Stop()
		return err
	}

	listen, err := config.ParseListen(relayConfig.Listen)
	if err != nil {
		service.Stop()
		return err
	}
	network := "tcp"
	if listen.Unix {
		network = "unix"
		clearStaleSocket(listen.Path, logger)
	}
	listener, err := net.Listen(network, listen.Address())
	if err != nil {
		service.Stop()
		return err
	}

	httpServer := &http.Server{Handler: server.New(service).Handler()}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()
	logger.Info("listening", "address", relayConfig.Listen)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			service.Stop()
			return err
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	service.Stop()
	return nil
}

func newLogger(level string) *slog.Logger {
	var slogLevel slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		slogLevel = slog.LevelDebug
	case "INFO":
		slogLevel = slog.LevelInfo
	case "WARN", "WARNING":
		slogLevel = slog.LevelWarn
	case "ERROR":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slogLevel}))
}

// clearStaleSocket removes a leftover Unix socket so net.Listen can bind again.
func clearStaleSocket(path string, logger *slog.Logger) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if info.Mode()&os.ModeSocket != 0 {
		logger.Info("removing stale socket", "path", path)
		_ = os.Remove(path)
	}
}
