package balancer

import (
	"math"
	"pastebin/gateway/internal/nodepool"
	"testing"
)

// draws is large enough that the tolerances below are many standard
// deviations wide, so these tests are not flaky.
const draws = 20000

func countPicks(t *testing.T, algo Algorithm, size int64, except []*nodepool.Node) map[*nodepool.Node]int {
	t.Helper()
	counts := make(map[*nodepool.Node]int)
	for range draws {
		counts[mustNext(t, algo, size, except)]++
	}
	return counts
}

func assertShare(t *testing.T, counts map[*nodepool.Node]int, node *nodepool.Node, want, tolerance float64) {
	t.Helper()
	got := float64(counts[node]) / draws
	if math.Abs(got-want) > tolerance {
		t.Fatalf("pick share is %.3f, want %.3f (+-%.2f)", got, want, tolerance)
	}
}

func TestWeightedRandomSingleCandidateIsAlwaysPicked(t *testing.T) {
	only := newTestNode(30*gib, 100*gib, 0, true)
	dead := newTestNode(100*gib, 100*gib, 0, false)
	algo := NewWeightedRandom(nodepool.NewPool([]*nodepool.Node{dead, only}))

	for range 200 {
		if got := mustNext(t, algo, gib, nil); got != only {
			t.Fatalf("the only candidate must always be picked")
		}
	}
}

func TestWeightedRandomFollowsFreeShare(t *testing.T) {
	a := newTestNode(60*gib, 100*gib, 0, true)
	b := newTestNode(30*gib, 100*gib, 0, true)
	c := newTestNode(10*gib, 100*gib, 0, true)
	algo := NewWeightedRandom(nodepool.NewPool([]*nodepool.Node{a, b, c}))

	counts := countPicks(t, algo, gib, nil)

	// weights 0.6 : 0.3 : 0.1 out of a total of 1.0
	assertShare(t, counts, a, 0.6, 0.03)
	assertShare(t, counts, b, 0.3, 0.03)
	assertShare(t, counts, c, 0.1, 0.03)
}

func TestWeightedRandomRenormalizesAfterExclusion(t *testing.T) {
	a := newTestNode(60*gib, 100*gib, 0, true)
	b := newTestNode(30*gib, 100*gib, 0, true)
	c := newTestNode(10*gib, 100*gib, 0, true)
	algo := NewWeightedRandom(nodepool.NewPool([]*nodepool.Node{a, b, c}))

	counts := countPicks(t, algo, gib, []*nodepool.Node{a})

	if counts[a] != 0 {
		t.Fatalf("excluded node was picked %d times", counts[a])
	}
	// remaining weights 0.3 : 0.1 -> 75% / 25%
	assertShare(t, counts, b, 0.75, 0.03)
	assertShare(t, counts, c, 0.25, 0.03)
}

func TestWeightedRandomIgnoresActiveConns(t *testing.T) {
	// Unlike LeastConnsByFreeSpace the weight is only free/total, so load
	// does not influence the choice. This documents that difference.
	idle := newTestNode(50*gib, 100*gib, 0, true)
	busy := newTestNode(50*gib, 100*gib, 100, true)
	algo := NewWeightedRandom(nodepool.NewPool([]*nodepool.Node{idle, busy}))

	counts := countPicks(t, algo, gib, nil)

	assertShare(t, counts, idle, 0.5, 0.05)
	assertShare(t, counts, busy, 0.5, 0.05)
}

func TestWeightedRandomReactsToStatusChanges(t *testing.T) {
	a := newTestNode(50*gib, 100*gib, 0, true)
	b := newTestNode(50*gib, 100*gib, 0, true)
	algo := NewWeightedRandom(nodepool.NewPool([]*nodepool.Node{a, b}))

	a.ChangeStatus(nodepool.Status{FreeSpace: 10 * gib, TotalSpace: 100 * gib})
	counts := countPicks(t, algo, gib, nil)

	// weights 0.1 : 0.5 -> a gets 1/6
	assertShare(t, counts, a, 1.0/6.0, 0.03)
	assertShare(t, counts, b, 5.0/6.0, 0.03)
}
