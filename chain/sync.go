package chain

// ★ Student file (Part 1, task 7): block propagation and synchronisation.
//
// Two mechanisms keep nodes in agreement:
//
//   - Push: a new block is broadcast to all peers, and every node that accepts it relays it
//     to its own peers, so it crosses the network even if not everyone is connected to
//     everyone. If a pushed block's parent is unknown (we missed some blocks), fetch the
//     missing ancestors from the peer that sent it.
//   - Pull: pushes get lost (a peer was down, partitioned, a message was dropped). So every
//     node also asks its peers for their head every SyncInterval, and fetches the chain of
//     any peer whose head has a higher total difficulty than its own. That is what makes
//     nodes converge after a partition heals even if nobody mines a new block.
//
// Tests: go test ./chain -run TestSync -v, then go test ./tests -v (several real nodes).

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// SyncInterval is how often the pull loop runs. Tests rely on nodes converging within a
// few intervals after a partition heals, so keep it at most 2 s.
const SyncInterval = time.Second

// maxSyncDepth bounds how far back fetchChain walks, so a lying peer cannot make us
// download forever.
const maxSyncDepth = 100_000

// Handler connects the block chain to the network.
type Handler struct {
	bc   *BlockChain
	net  PeerNet
	log  *log.Logger
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewHandler creates the sync handler.
func NewHandler(bc *BlockChain, net PeerNet, logger *log.Logger) *Handler {
	return &Handler{bc: bc, net: net, log: logger, stop: make(chan struct{}), done: make(chan struct{})}
}

// Start runs the pull loop (SyncOnce every SyncInterval) in the background until Stop.
func (h *Handler) Start() {
	go func() {
		defer close(h.done)
		t := time.NewTicker(SyncInterval)
		defer t.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-t.C:
				h.SyncOnce()
			}
		}
	}()
}

// Stop ends the pull loop. Harmless if Start was never called.
func (h *Handler) Stop() { h.once.Do(func() { close(h.stop) }) }

// HandleBlock is called for every block a peer pushes to us, one at a time, in arrival
// order. from is the sending peer's id.
//
//   - Already known: ignore it (this is what ends the relay).
//   - Parent unknown: fetch the missing ancestors from `from` (fetchChain).
//   - Valid and new (canonical or not): relay it to every peer except `from`.
//   - Invalid: drop it (log it), do not relay.
func (h *Handler) HandleBlock(b *types.Block, from string) {
	if b == nil {
		return
	}
	if h.bc.HasBlock(b.Hash()) {
		return
	}
	status, err := h.bc.InsertBlock(b)
	if err != nil {
		if errors.Is(err, ErrUnknownParent) {
			if err := h.fetchChain(from, b.Hash()); err != nil {
				h.log.Printf("block %d %s from %s: fetching ancestors: %v",
					b.NumberU64(), b.Hash().TerminalString(), from, err)
			}
			return
		}
		h.log.Printf("rejecting block %d %s from %s: %v", b.NumberU64(), b.Hash().TerminalString(), from, err)
		return
	}
	if status == StatusKnown {
		return
	}
	// Valid and new: relay it to everyone but the sender.
	h.net.BroadcastBlock(b, from)
}

// fetchChain downloads the block `tip` and every ancestor of it that we do not have from
// peer (RequestAncestors, newest first, as many requests as needed), inserts them oldest
// first, and relays tip if it was inserted.
func (h *Handler) fetchChain(peer string, tip common.Hash) error {
	if h.bc.HasBlock(tip) {
		return nil
	}
	var collected []*types.Block // newest first
	var tipBlock *types.Block
	want := tip
	for total := 0; ; {
		blocks, err := h.net.RequestAncestors(peer, want, MaxAncestors)
		if err != nil {
			return err
		}
		if len(blocks) == 0 {
			return fmt.Errorf("peer %s returned no blocks for %s", peer, want.TerminalString())
		}
		// Do not trust the peer: the first block must be the one we asked for, and each
		// following block must be the parent of the previous one.
		if blocks[0].Hash() != want {
			return fmt.Errorf("peer %s returned %s, asked for %s",
				peer, blocks[0].Hash().TerminalString(), want.TerminalString())
		}
		for i, b := range blocks {
			if i > 0 {
				prev := blocks[i-1]
				if b.Hash() != prev.ParentHash() || b.NumberU64()+1 != prev.NumberU64() {
					return fmt.Errorf("peer %s sent a non-contiguous chain at block %d %s",
						peer, b.NumberU64(), b.Hash().TerminalString())
				}
			}
			if h.bc.HasBlock(b.Hash()) {
				// A race with another insertion, or the peer sent too much: stop here.
				blocks = blocks[:i]
				break
			}
		}
		if len(blocks) == 0 {
			break
		}
		if tipBlock == nil {
			tipBlock = blocks[0]
		}
		collected = append(collected, blocks...)
		total += len(blocks)
		if total > maxSyncDepth {
			return fmt.Errorf("peer %s: chain from %s is deeper than %d blocks",
				peer, tip.TerminalString(), maxSyncDepth)
		}
		oldest := blocks[len(blocks)-1]
		if oldest.NumberU64() == 1 || h.bc.HasBlock(oldest.ParentHash()) {
			break // it connects to the genesis block or to a block we already have
		}
		want = oldest.ParentHash()
	}

	for i := len(collected) - 1; i >= 0; i-- {
		if _, err := h.bc.InsertBlock(collected[i]); err != nil {
			return fmt.Errorf("inserting block %d %s: %w", collected[i].NumberU64(), collected[i].Hash().TerminalString(), err)
		}
	}
	if tipBlock != nil {
		h.net.BroadcastBlock(tipBlock, peer)
	}
	return nil
}

// SyncOnce asks every peer for its head and, if the best one has a higher total
// difficulty than ours, fetches its chain. Peers that do not answer are skipped.
func (h *Handler) SyncOnce() {
	peers := h.net.Peers()
	if len(peers) == 0 {
		return
	}
	type answer struct {
		peer string
		head PeerHead
		err  error
	}
	results := make(chan answer, len(peers))
	for _, peer := range peers {
		go func(peer string) {
			head, err := h.net.RequestHead(peer)
			results <- answer{peer, head, err}
		}(peer)
	}

	heads := make([]answer, 0, len(peers))
	for range peers {
		if a := <-results; a.err == nil && a.head.TD != nil {
			heads = append(heads, a)
		}
	}
	// Heaviest first. A peer's claimed TD is only a hint: InsertBlock computes the real one.
	sort.Slice(heads, func(i, j int) bool { return heads[i].head.TD.Cmp(heads[j].head.TD) > 0 })

	local := h.bc.GetTd(h.bc.CurrentBlock().Hash())
	for _, a := range heads {
		if local != nil && a.head.TD.Cmp(local) <= 0 {
			break // sorted: the rest are no heavier either
		}
		if h.bc.HasBlock(a.head.Hash) {
			continue
		}
		if err := h.fetchChain(a.peer, a.head.Hash); err != nil {
			h.log.Printf("sync from %s (head %d %s): %v", a.peer, a.head.Number, a.head.Hash.TerminalString(), err)
			continue // try the next heaviest peer
		}
		return
	}
}
