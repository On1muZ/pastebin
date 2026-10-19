package transport

import (
	"context"
	"pastebin/gateway/internal/nodepool"
	storagenodev1 "pastebin/gen/go/storagenode/v1"

	"google.golang.org/grpc/stats"
)

type countedKey struct{}

type connCounter struct {
	node *nodepool.Node
}

func (c *connCounter) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if info.FullMethodName == storagenodev1.StorageNode_GetStatus_FullMethodName {
		return ctx
	}
	return context.WithValue(ctx, countedKey{}, true)
}

func (c *connCounter) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if ctx.Value(countedKey{}) == nil {
		return
	}
	switch s.(type) {
	case *stats.Begin:
		c.node.IncAliveConn()
	case *stats.End:
		c.node.DecAliveConn()
	}
}

func (c *connCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (c *connCounter) HandleConn(context.Context, stats.ConnStats) {}
