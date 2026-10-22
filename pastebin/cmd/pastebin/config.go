package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	envDev  = "dev"
	envProd = "prod"
)

type Config struct {
	Addr            string
	GatewayAddr     string
	DSN             string
	RedisAddr       string
	Env             string
	FlushInterval   time.Duration
	DeleteInterval  time.Duration
	ShutdownTimeout time.Duration
	PasteCacheTTL   time.Duration
	MaxPasteSize    int64
	DBMaxConns      int32
}

func loadConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("pastebin", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "адрес для http")
	gatewayAddr := fs.String("gateway-addr", "gateway:8090", "адрес gateway")
	env := fs.String("env", envDev, "окружение: dev, prod")
	flushInterval := fs.Duration("flush-interval", 5*time.Second, "интервал flush worker")
	deleteInterval := fs.Duration("delete-interval", 10*time.Second, "интервал delete worker")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "сколько ждать завершения запросов при остановке")
	pasteCacheTTL := fs.Duration("paste-cache-ttl", time.Hour, "время жизни пасты в кэше")
	maxPasteSize := fs.Int64("max-paste-size", 512<<10, "максимальный размер пасты")
	dbMaxConns := fs.Int("db-max-conns", 50, "максимум соединений с postgres")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Addr:            *addr,
		GatewayAddr:     *gatewayAddr,
		DSN:             os.Getenv("PASTEBIN_POSTGRES_DSN"),
		RedisAddr:       os.Getenv("PASTEBIN_REDIS_ADDR"),
		Env:             *env,
		FlushInterval:   *flushInterval,
		DeleteInterval:  *deleteInterval,
		ShutdownTimeout: *shutdownTimeout,
		PasteCacheTTL:   *pasteCacheTTL,
		MaxPasteSize:    *maxPasteSize,
		DBMaxConns:      int32(*dbMaxConns),
	}

	if cfg.Env != envDev && cfg.Env != envProd {
		return Config{}, fmt.Errorf("env: unknown value %q, want dev or prod", cfg.Env)
	}
	if cfg.DSN == "" {
		return Config{}, errors.New("PASTEBIN_POSTGRES_DSN is not set")
	}
	if cfg.RedisAddr == "" {
		return Config{}, errors.New("PASTEBIN_REDIS_ADDR is not set")
	}
	if cfg.GatewayAddr == "" {
		return Config{}, errors.New("gateway-addr is empty")
	}
	if cfg.FlushInterval <= 0 || cfg.DeleteInterval <= 0 {
		return Config{}, errors.New("intervals must be positive")
	}
	if cfg.ShutdownTimeout <= 0 {
		return Config{}, errors.New("shutdown-timeout must be positive")
	}
	if cfg.PasteCacheTTL <= 0 {
		return Config{}, errors.New("paste-cache-ttl must be positive")
	}
	if cfg.MaxPasteSize <= 0 {
		return Config{}, errors.New("max-paste-size must be positive")
	}
	if *dbMaxConns <= 0 || *dbMaxConns > 1000 {
		return Config{}, errors.New("db-max-conns must be in 1..1000")
	}
	return cfg, nil
}
