package chain

// ★ Student file (Part 1, task 6): the miner.
//
// Mining is a loop: take the current head, build a block on top of it from the pending
// transactions, search for a seal, insert the sealed block and tell the peers. Whenever
// the head changes (someone else found a block first), the work on the old head is
// wasted: stop at once and start again on the new head. PoW is memoryless, so restarting
// loses nothing but the block you were about to build on a stale head.
// Tests: go test ./chain -run TestMiner -v

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Broadcaster is the part of the network the miner needs.
type Broadcaster interface {
	BroadcastBlock(b *types.Block, except string)
}

// recommit is how often the miner rebuilds its block to pick up new transactions, even if
// the head did not change.
const recommit = 2 * time.Second

// Miner produces blocks.
type Miner struct {
	bc       *BlockChain
	pool     *TxPool
	net      Broadcaster
	coinbase common.Address
	throttle *Throttle
	log      *log.Logger

	mineMu sync.Mutex // one MineBlock at a time

	mu     sync.Mutex // guards the fields below
	cancel context.CancelFunc
	done   chan struct{}
}

// NewMiner creates a miner that pays itself (coinbase) and announces blocks on net.
func NewMiner(bc *BlockChain, pool *TxPool, net Broadcaster, coinbase common.Address, throttle *Throttle, logger *log.Logger) *Miner {
	return &Miner{bc: bc, pool: pool, net: net, coinbase: coinbase, throttle: throttle, log: logger}
}

// Mining reports whether the auto-mining loop is running.
func (m *Miner) Mining() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cancel != nil
}

// Start runs the mining loop in the background until Stop. Calling it twice is harmless.
func (m *Miner) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel, m.done = cancel, make(chan struct{})
	go m.loop(ctx, m.done)
	m.log.Printf("miner started")
}

// Stop ends the mining loop and waits until it has exited. Harmless if not running.
func (m *Miner) Stop() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel, m.done = nil, nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
		m.log.Printf("miner stopped")
	}
}

// loop mines until ctx is cancelled, then closes done. Each attempt is cancelled when the
// head changes (so we never keep mining on a stale head) or after `recommit` (to include
// new transactions).
func (m *Miner) loop(ctx context.Context, done chan struct{}) {
	defer close(done)
	heads, unsubscribe := m.bc.SubscribeHead()
	defer unsubscribe()

	for ctx.Err() == nil {
		parent := m.bc.CurrentBlock()
		attempt, cancel := context.WithTimeout(ctx, recommit)

		// Watch for a new head: if it is not the block we are mining on, abandon the
		// attempt at once. Join this goroutine before the next attempt, or two of them
		// would race for the next head event.
		watcherDone := make(chan struct{})
		go func(parent *types.Block) {
			defer close(watcherDone)
			for {
				select {
				case <-attempt.Done():
					return
				case head := <-heads:
					// A head event that is not our parent means our work is stale.
					if head.Hash() != parent.Hash() {
						cancel()
						return
					}
				}
			}
		}(parent)

		m.mineMu.Lock()
		_, err := m.mine(attempt, parent)
		m.mineMu.Unlock()
		cancel()
		<-watcherDone

		if err != nil && ctx.Err() == nil &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			m.log.Printf("mining: %v", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

// MineBlock mines exactly one block on the current head, inserts it and broadcasts it.
// The admin API (/admin/mine) and the tests call it while the loop is stopped.
func (m *Miner) MineBlock(ctx context.Context) (*types.Block, error) {
	m.mineMu.Lock()
	defer m.mineMu.Unlock()
	return m.mine(ctx, m.bc.CurrentBlock())
}

// mine builds a block on parent, seals it, inserts it and broadcasts it:
//
//   - Time: the local clock, but at least parent.Time+1. Never more than MaxFutureTime
//     ahead of the clock: your own node would reject the block (ErrFutureBlock), so wait.
//   - Difficulty: CalcDifficulty(cfg, time, parent).
//   - Coinbase: m.coinbase. Extra: anything up to 32 bytes, or nothing.
//   - Transactions: m.pool.Pending(); BuildBlock drops the ones that do not fit.
//
// Then Seal (with m.throttle), InsertBlock, and BroadcastBlock(sealed, "").
func (m *Miner) mine(ctx context.Context, parent *types.Block) (*types.Block, error) {
	if parent == nil {
		return nil, fmt.Errorf("mine: no parent block")
	}
	cfg := m.bc.Config()

	// Wait until a timestamp that is both after the parent and not too far ahead of the
	// local clock is possible.
	var ts uint64
	for {
		now := m.bc.Now().Unix()
		var nowu uint64
		if now > 0 {
			nowu = uint64(now)
		}
		ts = nowu
		if ts <= parent.Time() {
			ts = parent.Time() + 1
		}
		limit := nowu + cfg.MaxFutureTime
		if ts <= limit {
			break
		}
		wait := time.Duration(ts-limit) * time.Second
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}

	h := &types.Header{
		Coinbase:   m.coinbase,
		Time:       ts,
		Difficulty: CalcDifficulty(cfg, ts, parent.Header()),
		Extra:      []byte("defichain"),
	}
	var txs []*types.Transaction
	if m.pool != nil {
		txs = m.pool.Pending()
	}
	b, err := m.bc.BuildBlock(parent, h, txs)
	if err != nil {
		return nil, fmt.Errorf("build block %d: %w", parent.NumberU64()+1, err)
	}
	sealed, err := Seal(ctx, b, m.throttle)
	if err != nil {
		return nil, err
	}
	status, err := m.bc.InsertBlock(sealed)
	if err != nil {
		return nil, fmt.Errorf("insert mined block %d: %w", sealed.NumberU64(), err)
	}
	if m.net != nil {
		m.net.BroadcastBlock(sealed, "")
	}
	if m.log != nil {
		m.log.Printf("mined block %d %s (%s, %d txs, gas %d)", sealed.NumberU64(), sealed.Hash().TerminalString(),
			status, len(sealed.Transactions()), sealed.GasUsed())
	}
	return sealed, nil
}
