package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"pastebin/gateway/internal/balancer"
	"pastebin/gateway/internal/checker"
	"pastebin/gateway/internal/logger"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/server"
	"pastebin/gateway/internal/transport"
	"pastebin/gateway/internal/workers"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const dbPingTimeout = 5 * time.Second

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
		log.Error("gateway stopped with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("gateway stopped")
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

	store := metadata.NewPostgres(dbPool)
	nodes := make([]*nodepool.Node, 0, len(cfg.Nodes))
	nodeInfos := make([]*metadata.NodeInfo, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		nodes = append(nodes, nodepool.NewNode(n.ID, n.Addr))
		nodeInfos = append(nodeInfos, &metadata.NodeInfo{ID: n.ID, Addr: n.Addr})
	}
	if err := store.SyncNodes(ctx, nodeInfos); err != nil {
		return fmt.Errorf("sync nodes: %w", err)
	}
	log.Info("nodes were synced to metadata", slog.Int("count", len(nodeInfos)))

	pool, err := nodepool.NewPool(nodes)
	if err != nil {
		return fmt.Errorf("could not initialize pool: %w", err)
	}
	log.Info("node pool was successfully initialized")

	var algo balancer.Algorithm
	switch cfg.Balancer {
	case balancerWeightedRandom:
		algo = balancer.NewWeightedRandom(pool)
	default:
		algo = balancer.NewLeastConnsByFreeSpace(pool)
	}
	log.Info("balancer algorithm way initialized", slog.String("algorithm", cfg.Balancer))

	bal := balancer.NewBalancer(algo)
	log.Info("balancer was successfully initialized")

	tr := transport.New(transport.WithLogger(log))
	defer tr.Close()
	log.Info("transport was successfully initialized")

	chk := checker.New(pool, tr, log, cfg.CheckTimeout)
	log.Info("checker was successfully initialized")

	chk.CheckOnce(ctx)
	log.Info("first check finished", slog.Int("alive", len(pool.GetAllAliveNodes())), slog.Int("total", pool.GetTotalNodesCount()))

	var wg sync.WaitGroup
	wg.Go(func() { chk.Run(ctx, cfg.CheckInterval) })
	log.Info("checker was run")

	deleteWorker := workers.NewDeleteWorker(cfg.DeleteInterval, log, store, tr, pool)
	wg.Go(func() { deleteWorker.Run(ctx) })
	log.Info("delete worker was run", slog.Duration("interval", cfg.DeleteInterval))

	replicateWorker := workers.NewReplicateWorker(pool, tr, cfg.ReplicateInterval, log, store, bal)
	wg.Go(func() { replicateWorker.Run(ctx) })
	log.Info("replicate worker was run", slog.Duration("interval", cfg.ReplicateInterval))

	janitorWorker := workers.NewJanitorWorker(cfg.JanitorInterval, cfg.PendingStale, log, store, tr, pool)
	wg.Go(func() { janitorWorker.Run(ctx) })
	log.Info("janitor worker was run", slog.Duration("interval", cfg.JanitorInterval), slog.Duration("pending_stale", cfg.PendingStale))

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		cancel()
		wg.Wait()
		return fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}
	grpcServer := grpc.NewServer()
	storagenodev1.RegisterStorageNodeServer(grpcServer, server.New(log, bal, pool, tr, store, server.Config{
		MaxBlobSize:  cfg.MaxBlobSize,
		WriteTimeout: cfg.WriteTimeout,
	}))
	reflection.Register(grpcServer)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		log.Info("shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(cfg.ShutdownTimeout):
			log.Warn("graceful stop timed out, forcing stop")
			grpcServer.Stop()
		}
	}()

	log.Info("gateway started", slog.String("addr", cfg.Addr), slog.String("env", cfg.Env))
	serveErr := grpcServer.Serve(lis)
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
