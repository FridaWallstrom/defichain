package chain

// ★ Student file (Part 1, task 3): executing and validating blocks.
//
// A block header commits to the result of running its transactions on the parent's
// state: Root (the world state after the block), GasUsed, ReceiptHash and Bloom (the
// receipts and their logs), and TxHash (the transactions themselves). A miner computes
// these fields when it builds a block (build); every node runs the block again when it
// receives it, and rejects it unless it gets exactly the same values (process). That is
// why every node can trust the state without trusting the miner.
//
// Helpers (executor.go): openState, newGasPool, applyTx, payReward, stateRoot,
// commitState, txsRoot, receiptsRoot, logsBloom, assemble.
// Tests: go test ./chain -run TestExec -v

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

// execResult is what running a list of transactions produced.
type execResult struct {
	txs      []*types.Transaction // the transactions that were executed
	receipts []*types.Receipt
	reverts  []string // per transaction: its revert reason, or ""
	gasUsed  uint64
}

// execute is the loop shared by build and process. It opens the parent's state, applies
// txs in order, pays the block reward and returns the result with the new state.
//
// With strict set, an invalid transaction rejects the whole block (process); without it,
// the transaction is skipped in place and the later ones are still tried (build).
func (x *Executor) execute(parent *types.Header, h *types.Header, blockHash common.Hash,
	txs []*types.Transaction, getHash vm.GetHashFunc, strict bool) (*execResult, *state.StateDB, error) {

	st, err := x.openState(parent.Root)
	if err != nil {
		return nil, nil, err
	}
	gp := newGasPool(h)
	res := &execResult{}
	for _, tx := range txs {
		// The index is the number of transactions actually included so far, not the
		// position in the candidate list.
		receipt, reason, err := x.applyTx(h, blockHash, getHash, st, gp, len(res.txs), tx)
		if err != nil {
			if strict {
				return nil, nil, fmt.Errorf("%w: transaction %d %s is invalid: %v",
					ErrInvalidBlock, len(res.txs), tx.Hash().TerminalString(), err)
			}
			continue // invalid here: leave it out, try the next one
		}
		res.txs = append(res.txs, tx)
		res.receipts = append(res.receipts, receipt)
		res.reverts = append(res.reverts, reason)
	}
	// The reward comes after all transactions: a coinbase cannot spend the reward of the
	// block it is mining inside that block.
	x.payReward(h, st)
	res.gasUsed = gp.CumulativeUsed()
	return res, st, nil
}

// build executes candidate transactions on top of parent and returns the unsealed block.
// h has every field set except those that depend on execution: build fills in Root,
// GasUsed, TxHash, ReceiptHash and Bloom (UncleHash is the empty hash). Invalid
// transactions are left out, in place. Nothing is written to the state database: process
// runs the block again when it is inserted (receipts here have a zero block hash).
func (x *Executor) build(parent *types.Header, h *types.Header, txs []*types.Transaction, getHash vm.GetHashFunc) (*types.Block, error) {
	if parent == nil || h == nil {
		return nil, fmt.Errorf("%w: nil parent or header", ErrInvalidBlock)
	}
	h = types.CopyHeader(h)
	res, st, err := x.execute(parent, h, common.Hash{}, txs, getHash, false)
	if err != nil {
		return nil, err
	}
	h.Root = x.stateRoot(h, st)
	h.GasUsed = res.gasUsed
	h.UncleHash = types.EmptyUncleHash
	return assemble(h, res.txs, res.receipts), nil
}

// process executes block b on top of parent and checks every commitment in b's header.
// Only if they all hold is the new state committed and the result returned. Every reason
// to reject the block is an error wrapping ErrInvalidBlock.
func (x *Executor) process(parent *types.Header, b *types.Block, getHash vm.GetHashFunc) (*BlockResult, error) {
	if parent == nil || b == nil {
		return nil, fmt.Errorf("%w: nil block or parent", ErrInvalidBlock)
	}
	// 1. The body must match the header: no uncles, no withdrawals.
	if len(b.Uncles()) != 0 {
		return nil, fmt.Errorf("%w: body has %d uncles", ErrInvalidBlock, len(b.Uncles()))
	}
	if b.Withdrawals() != nil {
		return nil, fmt.Errorf("%w: body has withdrawals", ErrInvalidBlock)
	}
	txs := b.Transactions()
	if got := txsRoot(txs); got != b.TxHash() {
		return nil, fmt.Errorf("%w: transactions root is %s, header says %s",
			ErrInvalidBlock, got.Hex(), b.TxHash().Hex())
	}

	// 2. Execute strictly: one invalid transaction makes the block invalid.
	h := b.Header()
	res, st, err := x.execute(parent, h, b.Hash(), txs, getHash, true)
	if err != nil {
		return nil, err
	}

	// 3. Every commitment must match the execution result.
	if res.gasUsed != h.GasUsed {
		return nil, fmt.Errorf("%w: gas used is %d, header says %d", ErrInvalidBlock, res.gasUsed, h.GasUsed)
	}
	if bloom := logsBloom(res.receipts); bloom != h.Bloom {
		return nil, fmt.Errorf("%w: logs bloom does not match the receipts", ErrInvalidBlock)
	}
	if root := receiptsRoot(res.receipts); root != h.ReceiptHash {
		return nil, fmt.Errorf("%w: receipts root is %s, header says %s", ErrInvalidBlock, root.Hex(), h.ReceiptHash.Hex())
	}
	if root := x.stateRoot(h, st); root != h.Root {
		return nil, fmt.Errorf("%w: state root is %s, header says %s", ErrInvalidBlock, root.Hex(), h.Root.Hex())
	}

	// 4. Only now keep the state; a rejected block leaves nothing behind.
	if err := x.commitState(h, st); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidBlock, err)
	}
	return &BlockResult{Receipts: res.receipts, Reverts: res.reverts}, nil
}
