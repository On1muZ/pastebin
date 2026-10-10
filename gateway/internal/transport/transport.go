package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"pastebin/gateway/internal/nodepool"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrTransportClosed error = errors.New("the transport is closed")
var ErrCouldNotOpenConn error = errors.New("could not open grpc conn")

type nodeConn struct {
	conn   *grpc.ClientConn
	client storagenodev1.StorageNodeClient
}

type Transport struct {
	mu       sync.RWMutex
	conns    map[string]*nodeConn
	closed   bool
	dialOpts []grpc.DialOption
	log      *slog.Logger
}

type Option func(*Transport)

func WithLogger(log *slog.Logger) Option {
	return func(t *Transport) {
		t.log = log
	}
}

func WithDialOpts(dialOpts ...grpc.DialOption) Option {
	return func(t *Transport) {
		for _, d := range dialOpts {
			t.dialOpts = append(t.dialOpts, d)
		}
	}
}

func New(opts ...Option) *Transport {
	t := &Transport{
		conns:    make(map[string]*nodeConn),
		log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		dialOpts: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Transport) Client(node *nodepool.Node) (storagenodev1.StorageNodeClient, error) {
	if node == nil {
		return nil, fmt.Errorf("node is nil pointer")
	}
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, ErrTransportClosed
	}
	var client storagenodev1.StorageNodeClient
	nc := t.conns[node.ID]

	if nc != nil {
		t.mu.RUnlock()
		return nc.client, nil
	}
	t.mu.RUnlock()
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrTransportClosed
	}
	if nc, ok := t.conns[node.ID]; ok {
		t.mu.Unlock()
		return nc.client, nil
	}
	defer t.mu.Unlock()
	conn, err := grpc.NewClient(node.Addr, t.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("could not open new connection to this client: %w: %w", ErrCouldNotOpenConn, err)
	}
	client = storagenodev1.NewStorageNodeClient(conn)
	t.conns[node.ID] = &nodeConn{
		client: client,
		conn:   conn,
	}
	t.log.Info("new conn has opened", slog.String("node_id", node.ID), slog.String("addr", node.Addr))
	return client, nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.log.Info("closing transport")
	var errs []error
	t.closed = true
	for _, nc := range t.conns {
		err := nc.conn.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
