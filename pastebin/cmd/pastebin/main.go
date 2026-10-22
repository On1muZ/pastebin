package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"pastebin/pastebin/internal/blobs"
	"pastebin/pastebin/internal/cache"
	"pastebin/pastebin/internal/httpapi"
	"pastebin/pastebin/internal/logger"
	"pastebin/pastebin/internal/metadata"
	"pastebin/pastebin/internal/paste"
	"pastebin/pastebin/internal/views"
	"pastebin/pastebin/internal/workers"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	dbPingTimeout     = 5 * time.Second
	redisPingTimeout  = 5 * time.Second
	flushOnceTimeout  = 5 * time.Second
	finalFlushTimeout = 5 * time.Second
	deleteOnceTimeout = 30 * time.Second
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 10 * time.Second
	idleTimeout       = 60 * time.Second
)

func main() {
	cfg, err := loadConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		os.Stderr.WriteString("config error: " + err.Error() + "\n")
		os.Exit(1)
	}
	log := logger.New(cfg.Env)
	log.Info("logger and config was successfully initialized")

	if err := run(cfg, log); err != nil {
		log.Error("pastebin stopped with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("pastebin stopped")
}

func run(cfg Config, log *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log.Info("parent context was successfully initialized")

	dbPool, err := newDBPool(ctx, cfg)
	if err != nil {
		return err
	}
	defer dbPool.Close()
	log.Info("postgres pool was successfully initialized")

	applied, err := metadata.Migrate(ctx, dbPool)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	log.Info("migrations were applied", slog.Int("count", len(applied)))

	redisClient, err := newRedisClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer redisClient.Close()
	log.Info("redis client was successfully initialized")

	conn, err := grpc.NewClient(cfg.GatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("gateway client: %w", err)
	}
	defer conn.Close()
	log.Info("gateway client was successfully initialized", slog.String("addr", cfg.GatewayAddr))

	store := metadata.NewPostgres(dbPool)
	blobStore := blobs.NewGateway(storagenodev1.NewStorageNodeClient(conn))
	pasteCache := cache.NewRedis(redisClient)
	counter := views.NewRedis(redisClient)
	pastes := paste.New(store, pasteCache, counter, blobStore, log, cfg.PasteCacheTTL, cfg.MaxPasteSize)
	handler := httpapi.NewHandler(pastes, log, cfg.MaxPasteSize)

	var wg sync.WaitGroup
	flushWorker := workers.NewFlushWorker(store, counter, log, flushOnceTimeout, finalFlushTimeout)
	wg.Go(func() { flushWorker.Run(ctx, cfg.FlushInterval) })
	log.Info("flush worker was run", slog.Duration("interval", cfg.FlushInterval))

	deleteWorker := workers.NewDeleteWorker(store, blobStore, deleteOnceTimeout, log)
	wg.Go(func() { deleteWorker.Run(ctx, cfg.DeleteInterval) })
	log.Info("delete worker was run", slog.Duration("interval", cfg.DeleteInterval))

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		cancel()
		wg.Wait()
		return fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}
	httpServer := &http.Server{
		Handler:           handler.Routes(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		log.Info("shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Warn("graceful shutdown timed out, forcing close", slog.String("error", err.Error()))
			httpServer.Close()
		}
	}()

	log.Info("pastebin started", slog.String("addr", cfg.Addr), slog.String("env", cfg.Env))
	serveErr := httpServer.Serve(lis)
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	cancel()
	<-stopped
	wg.Wait()
	return serveErr
}

func newDBPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pcfg.MaxConns = cfg.DBMaxConns
	dbPool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, dbPingTimeout)
	defer cancel()
	if err := dbPool.Ping(pingCtx); err != nil {
		dbPool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return dbPool, nil
}

func newRedisClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	pingCtx, cancel := context.WithTimeout(ctx, redisPingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}
