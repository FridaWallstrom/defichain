package chain

// ★ Student file (Part 1, task 4): the transaction pool (mempool).
//
// The pool holds valid transactions that are not on the canonical chain yet. Every node
// has one; transactions are gossiped to all of them, so whoever mines next can include
// them. A transaction leaves the pool when the canonical chain uses up its nonce, and
// comes back if a reorg takes it off the canonical chain again (setHead calls reset).
//
// Per sender, transactions are ordered by nonce, and only the run of consecutive nonces
// starting at the sender's nonce on the head state can be executed now ("pending"); the
// others wait for the missing nonce ("queued"). Callers: the RPC server (Add, Get,
// PendingNonce, Content), the P2P layer (Add, Has), the miner (Pending) and setHead
// (reset), all from different goroutines.
//
// Helpers: p.bc.exec.ValidateTx (executor.go), p.bc.HeadState (chain.go).
// Tests: go test ./chain -run TestTxPool -v

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

// TxPool is the pool. The fields are guarded by mu; no method may call back into the
// pool while holding it.
type TxPool struct {
	mu sync.Mutex
	bc *BlockChain

	all      map[common.Hash]*types.Transaction            // every pooled transaction
	bySender map[common.Address]map[uint64]*types.Transaction // sender -> nonce -> transaction
	arrival  map[common.Hash]uint64                        // arrival order, for tie-breaking
	seq      uint64
}

// NewTxPool creates an empty pool; NewBlockChain attaches it to the chain.
func NewTxPool() *TxPool {
	return &TxPool{
		all:      map[common.Hash]*types.Transaction{},
		bySender: map[common.Address]map[uint64]*types.Transaction{},
		arrival:  map[common.Hash]uint64{},
	}
}

// init is called once by NewBlockChain, before the pool is used.
func (p *TxPool) init(bc *BlockChain) { p.bc = bc }

// Add validates tx and adds it. It returns (true, nil) if tx was added, (false, nil) if
// the pool already had it, and an error if tx is rejected, in this order:
//
//   - the stateless rules (ValidateTx): its error;
//   - already in the pool (same hash): false, nil;
//   - nonce below the sender's nonce on the head state: core.ErrNonceTooLow;
//   - balance on the head state below tx.Cost() (gas * fee cap + value): core.ErrInsufficientFunds;
//   - same sender and nonce as a pooled transaction: tx replaces it if its tip
//     (GasTipCap) is strictly higher, otherwise ErrReplaceUnderpriced;
//   - the pool already holds MaxPoolTxs transactions (and tx replaces none): ErrPoolFull.
//
// A nonce gap is fine: tx waits in the queue until the missing nonces arrive.
func (p *TxPool) Add(tx *types.Transaction) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("nil transaction")
	}
	if p.bc == nil {
		return false, fmt.Errorf("txpool is not attached to a chain")
	}
	// State-independent checks outside the lock: they are the expensive part.
	from, err := p.bc.exec.ValidateTx(tx)
	if err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st, _, err := p.bc.HeadState()
	if err != nil {
		return false, err
	}
	return p.addLocked(from, tx, st)
}

// addLocked performs the admission checks that need the pool (and a state) and inserts
// tx. The stateless rules must already have passed and from must be tx's sender.
func (p *TxPool) addLocked(from common.Address, tx *types.Transaction, st *state.StateDB) (bool, error) {
	hash := tx.Hash()
	if _, ok := p.all[hash]; ok {
		return false, nil
	}

	next := st.GetNonce(from)
	if tx.Nonce() < next {
		return false, fmt.Errorf("%w: transaction nonce %d, account nonce %d", core.ErrNonceTooLow, tx.Nonce(), next)
	}
	if bal := st.GetBalance(from).ToBig(); bal.Cmp(tx.Cost()) < 0 {
		return false, fmt.Errorf("%w: balance %s, need %s", core.ErrInsufficientFunds, bal, tx.Cost())
	}

	senderTxs := p.bySender[from]
	old, replacing := senderTxs[tx.Nonce()]
	if replacing {
		if tx.GasTipCap().Cmp(old.GasTipCap()) <= 0 {
			return false, fmt.Errorf("%w: tip %s, the pooled transaction has %s",
				ErrReplaceUnderpriced, tx.GasTipCap(), old.GasTipCap())
		}
	} else if len(p.all) >= MaxPoolTxs {
		return false, ErrPoolFull
	}

	if replacing {
		p.removeLocked(from, old)
	}
	if p.bySender[from] == nil {
		p.bySender[from] = map[uint64]*types.Transaction{}
	}
	p.all[hash] = tx
	p.bySender[from][tx.Nonce()] = tx
	p.seq++
	p.arrival[hash] = p.seq
	return true, nil
}

// removeLocked deletes a transaction that is known to belong to from.
func (p *TxPool) removeLocked(from common.Address, tx *types.Transaction) {
	hash := tx.Hash()
	delete(p.all, hash)
	delete(p.arrival, hash)
	if txs := p.bySender[from]; txs != nil {
		delete(txs, tx.Nonce())
		if len(txs) == 0 {
			delete(p.bySender, from)
		}
	}
}

// Has reports whether a transaction is in the pool.
func (p *TxPool) Has(hash common.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.all[hash]
	return ok
}

// Get returns a pooled transaction, or nil.
func (p *TxPool) Get(hash common.Hash) *types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.all[hash]
}

// Len is the number of pooled transactions, pending and queued.
func (p *TxPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.all)
}

// txCursor is one sender's pending run and how far the miner has got through it.
type txCursor struct {
	txs []*types.Transaction
	idx int
	seq uint64 // arrival order of txs[idx], for ties
}

type txHeap []*txCursor

func (h txHeap) Len() int { return len(h) }
func (h txHeap) Less(i, j int) bool {
	ti, tj := h[i].txs[h[i].idx], h[j].txs[h[j].idx]
	if c := ti.GasTipCap().Cmp(tj.GasTipCap()); c != 0 {
		return c > 0 // highest tip first
	}
	return h[i].seq < h[j].seq // then first come, first served
}
func (h txHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *txHeap) Push(x any)   { *h = append(*h, x.(*txCursor)) }
func (h *txHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// Pending returns the transactions that can go into the next block, in the order a miner
// should try them: per sender, its pending run in nonce order; across senders, always
// the highest tip (GasTipCap) among the senders' next transactions, and on equal tips
// the one that arrived first.
func (p *TxPool) Pending() []*types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bc == nil {
		return nil
	}
	st, _, err := p.bc.HeadState()
	if err != nil {
		return nil
	}
	h := make(txHeap, 0, len(p.bySender))
	for addr, nonces := range p.bySender {
		next := st.GetNonce(addr)
		var run []*types.Transaction
		for {
			tx, ok := nonces[next]
			if !ok {
				break
			}
			run = append(run, tx)
			next++
		}
		if len(run) > 0 {
			h = append(h, &txCursor{txs: run, seq: p.arrival[run[0].Hash()]})
		}
	}
	heap.Init(&h)
	out := make([]*types.Transaction, 0, len(p.all))
	for h.Len() > 0 {
		c := h[0]
		out = append(out, c.txs[c.idx])
		c.idx++
		if c.idx < len(c.txs) {
			c.seq = p.arrival[c.txs[c.idx].Hash()]
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	return out
}

// PendingNonce is the nonce the sender's next transaction should use: its nonce on the
// head state plus the length of its pending run (eth_getTransactionCount "pending").
func (p *TxPool) PendingNonce(addr common.Address) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bc == nil {
		return 0
	}
	st, _, err := p.bc.HeadState()
	if err != nil {
		return 0
	}
	next := st.GetNonce(addr)
	nonces := p.bySender[addr]
	for {
		if _, ok := nonces[next]; !ok {
			return next
		}
		next++
	}
}

// Content splits the pool into pending and queued transactions, per sender, each list
// sorted by nonce (txpool_content). A sender with no transaction of a kind has no entry
// in that map.
func (p *TxPool) Content() (pending, queued map[common.Address][]*types.Transaction) {
	pending = map[common.Address][]*types.Transaction{}
	queued = map[common.Address][]*types.Transaction{}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bc == nil {
		return pending, queued
	}
	st, _, err := p.bc.HeadState()
	if err != nil {
		return pending, queued
	}
	for addr, nonces := range p.bySender {
		next := st.GetNonce(addr)
		var run, rest []*types.Transaction
		for {
			tx, ok := nonces[next]
			if !ok {
				break
			}
			run = append(run, tx)
			next++
		}
		for n, tx := range nonces {
			if n >= next {
				rest = append(rest, tx)
			}
		}
		if len(run) > 0 {
			pending[addr] = run
		}
		if len(rest) > 0 {
			sort.Slice(rest, func(i, j int) bool { return rest[i].Nonce() < rest[j].Nonce() })
			queued[addr] = rest
		}
	}
	return pending, queued
}

// reset adapts the pool to a new head whose state is st. setHead calls it after every
// head change, with dropped = the transactions of the blocks that left the canonical
// chain (none unless there was a reorg), oldest first. Each dropped transaction goes
// through the same checks as Add, but against st; those that pass are added back. Then
// every pooled transaction whose nonce is below its sender's nonce on st is removed. It
// returns the dropped transactions that were added back, in order.
func (p *TxPool) reset(st *state.StateDB, dropped []*types.Transaction) []*types.Transaction {
	if p.bc == nil || st == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	var readded []*types.Transaction
	for _, tx := range dropped {
		if tx == nil {
			continue
		}
		from, err := p.bc.exec.ValidateTx(tx)
		if err != nil {
			continue
		}
		if ok, err := p.addLocked(from, tx, st); err == nil && ok {
			readded = append(readded, tx)
		}
	}

	// Remove what the new chain has already used.
	for addr := range p.bySender {
		base := st.GetNonce(addr)
		for nonce, tx := range p.bySender[addr] {
			if nonce < base {
				p.removeLocked(addr, tx)
			}
		}
	}
	return readded
}
