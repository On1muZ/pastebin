package balancer

import (
	"errors"
	"pastebin/gateway/internal/nodepool"
	"sync"
	"testing"
)

// algorithms lists every implementation of Algorithm. Tests in this file are
// the contract that all of them must satisfy.
var algorithms = map[string]func(*nodepool.Pool) Algorithm{
	"LeastConnsByFreeSpace": NewLeastConnsByFreeSpace,
	"WeightedRandom":        NewWeightedRandom,
}

func TestAlgorithmsErrors(t *testing.T) {
	tests := []struct {
		name    string
		size    int64
		build   func() (nodes, except []*nodepool.Node)
		wantErr error
	}{
		{
			name: "empty pool",
			size: gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				return nil, nil
			},
			wantErr: ErrNoNodes,
		},
		{
			name: "all nodes dead",
			size: gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				return []*nodepool.Node{
					newTestNode(100*gib, 100*gib, 0, false),
					newTestNode(100*gib, 100*gib, 0, false),
				}, nil
			},
			wantErr: ErrNoNodes,
		},
		{
			name: "only alive node is excluded",
			size: gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				a := newTestNode(100*gib, 100*gib, 0, true)
				return []*nodepool.Node{a}, []*nodepool.Node{a}
			},
			wantErr: ErrNoNodes,
		},
		{
			name: "alive node excluded and the rest are dead",
			size: gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				a := newTestNode(100*gib, 100*gib, 0, true)
				b := newTestNode(100*gib, 100*gib, 0, false)
				return []*nodepool.Node{a, b}, []*nodepool.Node{a}
			},
			wantErr: ErrNoNodes,
		},
		{
			name: "no alive node has enough space",
			size: 2 * gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				return []*nodepool.Node{
					newTestNode(gib, 100*gib, 0, true),
					newTestNode(gib, 100*gib, 0, true),
				}, nil
			},
			wantErr: ErrNoNodesCanWriteThisFile,
		},
		{
			name: "free space does not cover size plus reserve",
			size: 5 * gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				return []*nodepool.Node{
					newTestNode(5*gib+ReserveSpace-1, 100*gib, 0, true),
				}, nil
			},
			wantErr: ErrNoNodesCanWriteThisFile,
		},
		{
			name: "excluded node has space but the remaining one does not",
			size: 10 * gib,
			build: func() ([]*nodepool.Node, []*nodepool.Node) {
				a := newTestNode(100*gib, 100*gib, 0, true)
				b := newTestNode(gib, 100*gib, 0, true)
				return []*nodepool.Node{a, b}, []*nodepool.Node{a}
			},
			wantErr: ErrNoNodesCanWriteThisFile,
		},
	}
	for algoName, newAlgo := range algorithms {
		for _, tc := range tests {
			t.Run(algoName+"/"+tc.name, func(t *testing.T) {
				nodes, except := tc.build()
				node, err := newAlgo(nodepool.NewPool(nodes)).Next(tc.size, except)
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("incorrect error: got %v, want %v", err, tc.wantErr)
				}
				if node != nil {
					t.Fatalf("node must be nil on error, got %p", node)
				}
			})
		}
	}
}

func TestAlgorithmsAcceptExactlyReservedSpace(t *testing.T) {
	for name, newAlgo := range algorithms {
		t.Run(name, func(t *testing.T) {
			size := 5 * gib
			n := newTestNode(size+ReserveSpace, 100*gib, 0, true)
			got := mustNext(t, newAlgo(nodepool.NewPool([]*nodepool.Node{n})), size, nil)
			if got != n {
				t.Fatalf("node with exactly size+reserve free space must be accepted")
			}
		})
	}
}

func TestAlgorithmsNeverPickExcludedDeadOrFullNodes(t *testing.T) {
	for name, newAlgo := range algorithms {
		t.Run(name, func(t *testing.T) {
			good1 := newTestNode(50*gib, 100*gib, 0, true)
			good2 := newTestNode(80*gib, 100*gib, 2, true)
			dead := newTestNode(100*gib, 100*gib, 0, false)
			excluded := newTestNode(100*gib, 100*gib, 0, true)
			full := newTestNode(gib, 100*gib, 0, true)
			algo := newAlgo(nodepool.NewPool([]*nodepool.Node{good1, good2, dead, excluded, full}))
			except := []*nodepool.Node{excluded}
			for range 1000 {
				got := mustNext(t, algo, 10*gib, except)
				if got != good1 && got != good2 {
					t.Fatalf("picked node that is dead, excluded or without enough space")
				}
			}
		})
	}
}

// Next is called from many request goroutines while the checker keeps
// replacing node statuses. Run with -race.
func TestAlgorithmsConcurrentUse(t *testing.T) {
	for name, newAlgo := range algorithms {
		t.Run(name, func(t *testing.T) {
			nodes := []*nodepool.Node{
				newTestNode(60*gib, 100*gib, 0, true),
				newTestNode(30*gib, 100*gib, 0, true),
				newTestNode(10*gib, 100*gib, 0, true),
			}
			algo := newAlgo(nodepool.NewPool(nodes))

			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 200 {
						node, err := algo.Next(gib, nil)
						if err != nil || node == nil {
							t.Errorf("Next must find a node: got node=%v err=%v", node, err)
							return
						}
						node.IncAliveConn()
						node.DecAliveConn()
					}
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 200 {
					free := int64(10+i%50) * gib
					nodes[0].ChangeStatus(nodepool.Status{FreeSpace: free, TotalSpace: 100 * gib})
				}
			}()
			wg.Wait()
		})
	}
}
