package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"pastebin/storagenode/internal/blob"
	"pastebin/storagenode/internal/server"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	addr := flag.String("addr", ":9001", "адрес для gRPC")
	dataDir := flag.String("data", "./data", "каталог")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*addr, *dataDir, log); err != nil {
		log.Error("storagenode stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, dataDir string, log *slog.Logger) error {
	store, err := blob.NewStore(dataDir)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	storagenodev1.RegisterStorageNodeServer(grpcServer, server.New(store, log))
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		grpcServer.GracefulStop()
	}()

	log.Info("storagenode started", "addr", addr, "data", dataDir)
	return grpcServer.Serve(lis)
}
