package chain

// ★ Student file (Part 1, task 2): difficulty adjustment and header validation.
// Tests: go test ./chain -run TestConsensus -v

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"defichain/spec"
)

// CalcDifficulty is the difficulty a child of parent with timestamp `time` must have.
// It is Ethereum's Homestead rule (EIP-2) with our parameters:
//
//	x    = max(1 - (time - parent.Time) / TargetBlockTime, -99)    (integer division)
//	diff = parent.Difficulty + parent.Difficulty / DifficultyBoundDivisor * x
//	diff = max(diff, MinimumDifficulty)
//
// A block found quickly (in less than TargetBlockTime) raises the difficulty by
// parent/Div; a slow one lowers it, by more the slower it was. Only the timestamps say
// how fast blocks were found, which is why miners must not lie about time too much (see
// MaxFutureTime). VerifyHeader rejects time <= parent.Time before calling this; treat
// that case as time - parent.Time = 0.
func CalcDifficulty(cfg spec.ChainConfig, time uint64, parent *types.Header) *big.Int {
	minimum := new(big.Int).SetUint64(cfg.MinimumDifficulty)
	if parent == nil || parent.Difficulty == nil {
		return minimum
	}
	// Work on copies: never modify parent.Difficulty.
	base := new(big.Int).Set(parent.Difficulty)

	var delta uint64
	if time > parent.Time {
		delta = time - parent.Time
	}
	target := cfg.TargetBlockTime
	if target == 0 {
		target = 1 // the config forbids this; do not panic on a broken config
	}
	// x = max(1 - delta/target, -99). Clamp the quotient before converting it to a
	// signed integer: delta can be huge.
	q := delta / target
	x := int64(1)
	if q >= 100 {
		x = -99
	} else {
		x = 1 - int64(q)
	}

	divisor := cfg.DifficultyBoundDivisor
	if divisor == 0 {
		divisor = 1
	}
	step := new(big.Int).Div(base, new(big.Int).SetUint64(divisor))
	step.Mul(step, big.NewInt(x))

	diff := new(big.Int).Add(base, step)
	if diff.Cmp(minimum) < 0 {
		diff.Set(minimum)
	}
	return diff
}

// VerifyHeader checks every rule a header must follow, given its parent and the local
// clock `now` (Unix seconds). The caller has already found parent by h.ParentHash.
// Returns nil, or an error wrapping the sentinel of the first rule broken:
//
//	1. h.Number == parent.Number + 1                           ErrInvalidNumber
//	2. h.Time > parent.Time                                    ErrInvalidTimestamp
//	3. h.Time <= now + MaxFutureTime                           ErrFutureBlock
//	4. h.Difficulty == CalcDifficulty(cfg, h.Time, parent)      ErrInvalidDifficulty
//	5. h.GasLimit == cfg.GasLimit                              ErrInvalidGasLimit
//	6. h.GasUsed <= h.GasLimit                                 ErrInvalidGasUsed
//	7. len(h.Extra) <= 32                                      ErrExtraTooLong
//	8. h.UncleHash == types.EmptyUncleHash                     ErrInvalidUncles
//	9. h.BaseFee != nil && h.BaseFee == 0                      ErrInvalidBaseFee
//	10. VerifySeal(h) == nil                                   (VerifySeal's error)
//
// The cheap checks come first, the seal last: a node should not spend effort on a block
// that is wrong anyway.
func VerifyHeader(cfg spec.ChainConfig, h, parent *types.Header, now uint64) error {
	if h == nil {
		return fmt.Errorf("%w: nil header", ErrInvalidNumber)
	}
	if parent == nil {
		return fmt.Errorf("%w: nil parent header", ErrInvalidNumber)
	}

	// 1. Height.
	if h.Number == nil || parent.Number == nil {
		return fmt.Errorf("%w: nil block number", ErrInvalidNumber)
	}
	wantNumber := new(big.Int).Add(parent.Number, common.Big1)
	if h.Number.Cmp(wantNumber) != 0 {
		return fmt.Errorf("%w: got %v, want %v", ErrInvalidNumber, h.Number, wantNumber)
	}

	// 2. Time strictly after the parent.
	if h.Time <= parent.Time {
		return fmt.Errorf("%w: %d is not after the parent's %d", ErrInvalidTimestamp, h.Time, parent.Time)
	}

	// 3. Not too far ahead of the local clock. Compare the gap to avoid overflow.
	if h.Time > now && h.Time-now > cfg.MaxFutureTime {
		return fmt.Errorf("%w: %d is more than %d s ahead of %d", ErrFutureBlock, h.Time, cfg.MaxFutureTime, now)
	}

	// 4. Difficulty.
	wantDiff := CalcDifficulty(cfg, h.Time, parent)
	if h.Difficulty == nil || h.Difficulty.Cmp(wantDiff) != 0 {
		return fmt.Errorf("%w: got %v, want %v", ErrInvalidDifficulty, h.Difficulty, wantDiff)
	}

	// 5. Gas limit.
	if h.GasLimit != cfg.GasLimit {
		return fmt.Errorf("%w: got %d, want %d", ErrInvalidGasLimit, h.GasLimit, cfg.GasLimit)
	}

	// 6. Gas used.
	if h.GasUsed > h.GasLimit {
		return fmt.Errorf("%w: %d > %d", ErrInvalidGasUsed, h.GasUsed, h.GasLimit)
	}

	// 7. Extra data.
	if len(h.Extra) > 32 {
		return fmt.Errorf("%w: %d bytes", ErrExtraTooLong, len(h.Extra))
	}

	// 8. No uncles.
	if h.UncleHash != types.EmptyUncleHash {
		return fmt.Errorf("%w: uncle hash %s, want %s", ErrInvalidUncles, h.UncleHash.Hex(), types.EmptyUncleHash.Hex())
	}

	// 9. Base fee: present and zero.
	if h.BaseFee == nil || h.BaseFee.Sign() != 0 {
		return fmt.Errorf("%w: got %v, want 0", ErrInvalidBaseFee, h.BaseFee)
	}

	// 10. The seal, last: it is the most expensive check.
	return VerifySeal(h)
}
