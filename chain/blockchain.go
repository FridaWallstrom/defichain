package chain

// ★ Student file (Part 1, task 5): inserting blocks, fork choice and reorgs.
//
// The node keeps every valid block it has seen, as a tree rooted at genesis. Fork choice
// picks one leaf as the head; the canonical chain is the path from genesis to it. Our
// rule is Nakamoto's: the head is the block with the highest total difficulty (TD, the
// sum of the difficulties of all blocks from genesis to it: "the most work"), not the
// longest chain. On a tie, keep the current head (first seen wins).
//
// Helpers you will use, all in chain.go: processBlock, writeBlock, writeCanonical,
// deleteCanonical, setHead, GetBlock, GetTd, CurrentBlock, CanonicalHash.
// Tests: go test ./chain -run TestChain -v

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// chainExt is room for your own state in BlockChain (bc.ext). Its zero value must be
// ready to use. The reference solution needs none.
type chainExt struct{}

// insertBlock validates b, stores it, and makes it the head if it has the highest total
// difficulty. It runs with bc.insertMu held (see InsertBlock), so no other insertion runs
// at the same time. In order:
//
//  1. Already stored?                       -> StatusKnown, nil
//  2. Parent not stored?                    -> ErrUnknownParent (the caller fetches it)
//  3. VerifyHeader(header, parent, now)     -> its error (now = bc.Now().Unix())
//  4. processBlock(b)                       -> its error (wraps ErrInvalidBlock)
//  5. writeBlock(b, td, result) with td = TD(parent) + b.Difficulty
//  6. Fork choice: if td > TD(current head), reorg to b -> StatusCanon; else StatusSide.
//
// An invalid block must not be stored, and must not change the head.
func (bc *BlockChain) insertBlock(b *types.Block) (InsertStatus, error) {
	if b == nil {
		return StatusKnown, fmt.Errorf("%w: nil block", ErrInvalidBlock)
	}
	if bc.HasBlock(b.Hash()) {
		return StatusKnown, nil
	}
	parent := bc.GetBlock(b.ParentHash())
	if parent == nil {
		return StatusKnown, fmt.Errorf("%w: %s", ErrUnknownParent, b.ParentHash().TerminalString())
	}

	now := bc.Now().Unix()
	var nowu uint64
	if now > 0 {
		nowu = uint64(now)
	}
	if err := VerifyHeader(bc.cfg, b.Header(), parent.Header(), nowu); err != nil {
		return StatusKnown, err
	}
	res, err := bc.processBlock(b)
	if err != nil {
		return StatusKnown, err
	}

	td := new(big.Int).Add(bc.GetTd(parent.Hash()), b.Difficulty())
	bc.writeBlock(b, td, res)

	head := bc.CurrentBlock()
	if td.Cmp(bc.GetTd(head.Hash())) > 0 {
		if err := bc.reorg(head, b); err != nil {
			return StatusSide, err
		}
		return StatusCanon, nil
	}
	return StatusSide, nil
}

// reorg makes newHead the head of the canonical chain, in place of oldHead. Usually
// newHead is a child of oldHead and nothing is taken away; but it may also be on another
// branch of the tree, and then:
//
//   - find the common ancestor of the two heads (walk both back by parent hash; walk the
//     higher one down first, until both are at the same height);
//   - every block from the ancestor (exclusive) to newHead becomes canonical (writeCanonical);
//   - heights above newHead that were canonical on the old chain stop being canonical
//     (deleteCanonical), or GetBlockByNumber would still return old blocks there;
//   - transactions of the old branch that are not in the new one go back to the txpool:
//     pass them to setHead, oldest block first, in block order.
//
// Finally setHead(newHead, dropped).
func (bc *BlockChain) reorg(oldHead, newHead *types.Block) error {
	if oldHead == nil || newHead == nil {
		return fmt.Errorf("reorg: nil head")
	}
	old, next := oldHead, newHead
	var added, removed []*types.Block // newest first while walking

	// Bring both branches to the same height.
	for old.NumberU64() > next.NumberU64() {
		removed = append(removed, old)
		old = bc.GetBlock(old.ParentHash())
		if old == nil {
			return fmt.Errorf("reorg: missing ancestor of the old head")
		}
	}
	for next.NumberU64() > old.NumberU64() {
		added = append(added, next)
		next = bc.GetBlock(next.ParentHash())
		if next == nil {
			return fmt.Errorf("reorg: missing ancestor of the new head")
		}
	}
	// Walk both back until they meet: the common ancestor.
	for old.Hash() != next.Hash() {
		removed = append(removed, old)
		added = append(added, next)
		old = bc.GetBlock(old.ParentHash())
		next = bc.GetBlock(next.ParentHash())
		if old == nil || next == nil {
			return fmt.Errorf("reorg: the two heads are not in the same tree")
		}
	}

	// The new branch, oldest first, becomes canonical.
	for i := len(added) - 1; i >= 0; i-- {
		bc.writeCanonical(added[i])
	}
	// Heights above the new head must not stay canonical (the new chain may be shorter).
	if newHead.NumberU64() < oldHead.NumberU64() {
		for n := newHead.NumberU64() + 1; n <= oldHead.NumberU64(); n++ {
			bc.deleteCanonical(n)
		}
	}

	// Transactions of the old branch that are not in the new one go back to the pool.
	onNewBranch := make(map[common.Hash]bool)
	for _, b := range added {
		for _, tx := range b.Transactions() {
			onNewBranch[tx.Hash()] = true
		}
	}
	var dropped []*types.Transaction
	for i := len(removed) - 1; i >= 0; i-- { // oldest block first
		for _, tx := range removed[i].Transactions() {
			if !onNewBranch[tx.Hash()] {
				dropped = append(dropped, tx)
			}
		}
	}

	bc.setHead(newHead, dropped)
	return nil
}
