package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"pastebin/gateway/internal/workers"
	"strings"
	"time"
)

const (
	envDev  = "dev"
	envProd = "prod"

	balancerLeastConns     = "least-conns"
	balancerWeightedRandom = "weighted-random"
)

type NodeConfig struct {
	ID   string
	Addr string
}

type Config struct {
	Addr              string
	Nodes             []NodeConfig
	DSN               string
	Env               string
	Balancer          string
	CheckInterval     time.Duration
	DeleteInterval    time.Duration
	ReplicateInterval time.Duration
	JanitorInterval   time.Duration
	PendingStale      time.Duration
	CheckTimeout      time.Duration
	WriteTimeout      time.Duration
	MaxBlobSize       int64
	ShutdownTimeout   time.Duration
	DBMaxConns        int32
}

func loadConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	addr := fs.String("addr", ":8090", "адрес для gRPC")
	nodes := fs.String("nodes", "", "ноды в формате id=addr,id=addr")
	env := fs.String("env", envDev, "окружение: dev, prod")
	balancer := fs.String("balancer", balancerLeastConns, "алгоритм балансировки: least-conns, weighted-random")
	checkInterval := fs.Duration("check-interval", 5*time.Second, "интервал проверки нод")
	deleteInterval := fs.Duration("delete-interval", 10*time.Second, "интервал delete worker")
	replicateInterval := fs.Duration("replicate-interval", 30*time.Second, "интервал replicate worker")
	janitorInterval := fs.Duration("janitor-interval", time.Minute, "интервал janitor worker")
	pendingStale := fs.Duration("pending-stale", 5*time.Minute, "через сколько pending blob или реплика считается брошенной")
	checkTimeout := fs.Duration("check-timeout", 5*time.Second, "таймаут проверки одной ноды")
	writeTimeout := fs.Duration("write-timeout", 4*time.Second, "таймаут записи blob на ноды")
	maxBlobSize := fs.Int64("max-blob-size", 512<<10, "максимальный размер blob в байтах")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "сколько ждать завершения запросов при остановке")
	dbMaxConns := fs.Int("db-max-conns", 50, "максимум соединений с postgres")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Addr:              *addr,
		DSN:               os.Getenv("GATEWAY_DSN"),
		Env:               *env,
		Balancer:          *balancer,
		CheckInterval:     *checkInterval,
		DeleteInterval:    *deleteInterval,
		ReplicateInterval: *replicateInterval,
		JanitorInterval:   *janitorInterval,
		PendingStale:      *pendingStale,
		CheckTimeout:      *checkTimeout,
		WriteTimeout:      *writeTimeout,
		MaxBlobSize:       *maxBlobSize,
		ShutdownTimeout:   *shutdownTimeout,
		DBMaxConns:        int32(*dbMaxConns),
	}

	var err error
	if cfg.Nodes, err = parseNodes(*nodes); err != nil {
		return Config{}, err
	}
	if cfg.Env != envDev && cfg.Env != envProd {
		return Config{}, fmt.Errorf("env: unknown value %q, want dev or prod", cfg.Env)
	}
	if cfg.Balancer != balancerLeastConns && cfg.Balancer != balancerWeightedRandom {
		return Config{}, fmt.Errorf("balancer: unknown value %q, want least-conns or weighted-random", cfg.Balancer)
	}
	if cfg.DSN == "" {
		return Config{}, errors.New("GATEWAY_DSN is not set")
	}
	if cfg.CheckInterval <= 0 || cfg.DeleteInterval <= 0 || cfg.ReplicateInterval <= 0 || cfg.JanitorInterval <= 0 {
		return Config{}, errors.New("intervals must be positive")
	}
	if cfg.CheckTimeout <= 0 || cfg.WriteTimeout <= 0 {
		return Config{}, errors.New("timeouts must be positive")
	}
	if cfg.PendingStale <= cfg.WriteTimeout || cfg.PendingStale <= workers.TimeoutToReplicateBlob {
		return Config{}, fmt.Errorf("pending-stale must be greater than write-timeout and %s replicate timeout", workers.TimeoutToReplicateBlob)
	}
	if cfg.MaxBlobSize <= 0 {
		return Config{}, errors.New("max-blob-size must be positive")
	}
	if cfg.ShutdownTimeout <= 0 {
		return Config{}, errors.New("shutdown-timeout must be positive")
	}
	if *dbMaxConns <= 0 || *dbMaxConns > 1000 {
		return Config{}, errors.New("db-max-conns must be in 1..1000")
	}
	return cfg, nil
}

func parseNodes(s string) ([]NodeConfig, error) {
	if strings.TrimSpace(s) == "" {
		return nil, errors.New("nodes is empty")
	}
	seen := make(map[string]struct{})
	var nodes []NodeConfig
	for item := range strings.SplitSeq(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(item), "=")
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("nodes: bad item %q, want id=addr", item)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("nodes: duplicated id %q", id)
		}
		seen[id] = struct{}{}
		nodes = append(nodes, NodeConfig{ID: id, Addr: addr})
	}
	return nodes, nil
}
