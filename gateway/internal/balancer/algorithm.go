package balancer

import "pastebin/gateway/internal/nodepool"

type Algorithm interface {
	Next(size int64, except []*nodepool.Node) (*nodepool.Node, error)
}
