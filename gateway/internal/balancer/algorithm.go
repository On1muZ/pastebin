package balancer

import (
	"errors"
	"pastebin/gateway/internal/nodepool"
)

var ErrNoNodes error = errors.New("no nodes found")
var ErrNoNodesCanWriteThisFile error = errors.New("file too big to be written on any node")

const ReserveSpace = 1024 * 1024 * 10

type Algorithm interface {
	Next(size int64, except []*nodepool.Node) (*nodepool.Node, error)
}
