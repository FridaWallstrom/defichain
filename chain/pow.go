package chain

// ★ Student file (Part 1, task 1): the proof-of-work seal.
//
// A block is sealed by finding a nonce such that a hash of the header with that nonce is
// small enough. Our PoW function is a simplified Hashcash, not Ethereum's Ethash (Ethash
// adds a large memory-hard dataset so that mining needs RAM, not just CPU; the consensus
// logic is the same):
//
//	sealHash = Keccak256(RLP(header with Nonce and MixDigest set to zero))
//	powHash  = Keccak256(sealHash || nonce)          (nonce: 8 bytes, big-endian)
//	valid   <=> header.MixDigest == powHash  and  powHash <= 2^256 / difficulty
//
// MixDigest stores the PoW hash in the header, so a verifier can reject a block with a
// wrong MixDigest by comparing 32 bytes before doing any other work.
//
// The exact byte layout, and test vectors, are in docs/SPEC.md. Tests: go test ./chain -run TestPow -v

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// two256 is 2^256.
var two256 = new(big.Int).Lsh(common.Big1, 256)

// SealHash is the hash a miner works on: the header without its seal (Nonce, MixDigest).
// Every other field, including the difficulty and the timestamp, is covered, so changing
// any of them invalidates the seal.
func SealHash(h *types.Header) common.Hash {
	if h == nil {
		return common.Hash{}
	}
	c := types.CopyHeader(h)
	c.Nonce = types.BlockNonce{}
	c.MixDigest = common.Hash{}
	enc, err := rlp.EncodeToBytes(c)
	if err != nil {
		// Headers are always encodable unless a big.Int field is negative. Treat that as
		// an unusable header rather than panicking; VerifyHeader rejects it earlier.
		return common.Hash{}
	}
	return crypto.Keccak256Hash(enc)
}

// PowHash is Keccak256(sealHash || nonce), the nonce as 8 big-endian bytes.
func PowHash(sealHash common.Hash, nonce uint64) common.Hash {
	var buf [40]byte
	copy(buf[:32], sealHash[:])
	binary.BigEndian.PutUint64(buf[32:], nonce)
	return crypto.Keccak256Hash(buf[:])
}

// Target is the largest PoW hash a block of this difficulty may have: 2^256 / difficulty
// (integer division). Doubling the difficulty halves the target, so it takes twice as many
// attempts on average to find a nonce.
//
// A missing or non-positive difficulty has no target; nil is returned (the callers reject
// such headers with ErrInvalidDifficulty first).
func Target(difficulty *big.Int) *big.Int {
	if difficulty == nil || difficulty.Sign() <= 0 {
		return nil
	}
	return new(big.Int).Div(two256, difficulty)
}

// VerifySeal checks h's proof of work: MixDigest must equal PowHash(SealHash(h), nonce),
// and that hash, read as a big-endian number, must be <= Target(h.Difficulty). Errors
// wrap ErrInvalidDifficulty (difficulty missing or <= 0), ErrInvalidMixDigest or
// ErrInvalidPoW.
func VerifySeal(h *types.Header) error {
	if h == nil || h.Difficulty == nil || h.Difficulty.Sign() <= 0 {
		return fmt.Errorf("%w: header has difficulty %v", ErrInvalidDifficulty, difficultyString(h))
	}
	target := Target(h.Difficulty)
	if target == nil {
		return fmt.Errorf("%w: header has difficulty %v", ErrInvalidDifficulty, h.Difficulty)
	}
	pow := PowHash(SealHash(h), h.Nonce.Uint64())
	if pow != h.MixDigest {
		return fmt.Errorf("%w: mix digest %s, PoW hash is %s", ErrInvalidMixDigest, h.MixDigest.Hex(), pow.Hex())
	}
	if new(big.Int).SetBytes(pow[:]).Cmp(target) > 0 {
		return fmt.Errorf("%w: PoW hash %s is above the target %s", ErrInvalidPoW, pow.Hex(), target.String())
	}
	return nil
}

func difficultyString(h *types.Header) any {
	if h == nil || h.Difficulty == nil {
		return nil
	}
	return h.Difficulty
}

// sealBatch is how many nonces Seal tries between calls to throttle.Wait.
const sealBatch = 1024

// Seal searches for a nonce that makes b's header valid and returns the sealed block
// (b.WithSeal(header with Nonce and MixDigest set)). It tries nonces in batches of
// sealBatch and calls throttle.Wait(ctx, sealBatch) after each batch, which both caps the
// hashrate (a nil throttle means no cap) and returns an error once ctx is cancelled: then
// Seal returns that error. It starts from a random nonce so that two miners working on the
// same header do not try the same nonces.
func Seal(ctx context.Context, b *types.Block, throttle *Throttle) (*types.Block, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil block", ErrInvalidDifficulty)
	}
	h := b.Header()
	if h.Difficulty == nil || h.Difficulty.Sign() <= 0 {
		return nil, fmt.Errorf("%w: header has difficulty %v", ErrInvalidDifficulty, difficultyString(h))
	}
	target := Target(h.Difficulty)
	if target == nil {
		return nil, fmt.Errorf("%w: header has difficulty %v", ErrInvalidDifficulty, h.Difficulty)
	}
	seal := SealHash(h)
	nonce := rand.Uint64()
	var num big.Int
	for {
		for i := 0; i < sealBatch; i++ {
			pow := PowHash(seal, nonce)
			num.SetBytes(pow[:])
			if num.Cmp(target) <= 0 {
				h.Nonce = types.EncodeNonce(nonce)
				h.MixDigest = pow
				return b.WithSeal(h), nil
			}
			nonce++
		}
		if err := throttle.Wait(ctx, sealBatch); err != nil {
			return nil, err
		}
	}
}
