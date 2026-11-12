package core

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestDeterministicDvPAndConsensusStateRoot(t *testing.T) {
	// Generamos dos participantes: Trader A (comprador de BTC) y Market Maker B (vendedor de BTC)
	pubA, privA, _ := ed25519.GenerateKey(rand.Reader)
	pubB, privB, _ := ed25519.GenerateKey(rand.Reader)

	// 1. Depósito inicial (Mint): Trader A deposita 100,000 USD; Market Maker B deposita 5 BTC
	txMintA := &Transaction{
		Type:     TxMint,
		Sender:   pubA,
		AssetIn:  "USD",
		AmountIn: 100_000 * ScaleFactor,
		Nonce:    1,
	}
	txMintA.SignSender(privA)

	txMintB := &Transaction{
		Type:     TxMint,
		Sender:   pubB,
		AssetIn:  "BTC",
		AmountIn: 5 * ScaleFactor,
		Nonce:    1,
	}
	txMintB.SignSender(privB)

	// 2. Liquidación Atómica DvP: Trader A entrega 60,000 USD a cambio de 1 BTC de Market Maker B
	txSwap := &Transaction{
		Type:         TxAtomicSwap,
		Sender:       pubA,
		Counterparty: pubB,
		AssetIn:      "USD",
		AmountIn:     60_000 * ScaleFactor,
		AssetOut:     "BTC",
		AmountOut:    1 * ScaleFactor,
		Nonce:        2,
	}
	txSwap.SignSender(privA)
	txSwap.SignCounterparty(privB)

	txs := []*Transaction{txMintA, txMintB, txSwap}
	txRoot := ComputeTransactionsRoot(txs)

	block1 := &Block{
		Header: BlockHeader{
			Height:       1,
			TxMerkleRoot: txRoot,
			TimestampMs:  1700000000000,
		},
		Transactions: txs,
	}

	// Simulamos dos nodos validadores independientes ejecutando el mismo bloque
	node1 := NewStateMachine()
	node2 := NewStateMachine()

	root1, err := node1.ApplyBlock(block1)
	if err != nil {
		t.Fatalf("node1 failed to apply block: %v", err)
	}

	root2, err := node2.ApplyBlock(block1)
	if err != nil {
		t.Fatalf("node2 failed to apply block: %v", err)
	}

	// Verificación 1: Determinismo absoluto entre nodos
	if root1 != root2 {
		t.Fatalf("consensus broken: node1 StateRoot (%x) != node2 StateRoot (%x)", root1, root2)
	}

	// Verificación 2: Exactitud de saldos en punto fijo tras el DvP
	accA := node1.GetAccount(pubA)
	accB := node1.GetAccount(pubB)

	if accA.Balances["USD"] != 40_000*ScaleFactor || accA.Balances["BTC"] != 1*ScaleFactor {
		t.Fatalf("unexpected balances for Trader A: %+v", accA.Balances)
	}
	if accB.Balances["USD"] != 60_000*ScaleFactor || accB.Balances["BTC"] != 4*ScaleFactor {
		t.Fatalf("unexpected balances for Market Maker B: %+v", accB.Balances)
	}
}

func TestReplayProtectionAndAtomicRollback(t *testing.T) {
	pubA, privA, _ := ed25519.GenerateKey(rand.Reader)
	pubB, privB, _ := ed25519.GenerateKey(rand.Reader)

	sm := NewStateMachine()

	// Bloque 1: Mint de 1,000 USD para A
	txMint := &Transaction{
		Type:     TxMint,
		Sender:   pubA,
		AssetIn:  "USD",
		AmountIn: 1_000 * ScaleFactor,
		Nonce:    1,
	}
	txMint.SignSender(privA)

	b1 := &Block{
		Header:       BlockHeader{Height: 1, TxMerkleRoot: ComputeTransactionsRoot([]*Transaction{txMint})},
		Transactions: []*Transaction{txMint},
	}
	initialRoot, err := sm.ApplyBlock(b1)
	if err != nil {
		t.Fatalf("unexpected error on block 1: %v", err)
	}

	// Intento de Replay Attack en Bloque 2 (misma transacción con Nonce 1 ya usado)
	b2Replay := &Block{
		Header:       BlockHeader{Height: 2, PrevBlock: b1.Header.Hash(), TxMerkleRoot: ComputeTransactionsRoot([]*Transaction{txMint})},
		Transactions: []*Transaction{txMint},
	}
	if _, err := sm.ApplyBlock(b2Replay); err == nil {
		t.Fatal("expected replay attack with duplicate nonce to fail, but it succeeded")
	}

	// Intento de DvP donde la contraparte B no tiene BTC (debe revertir todo el bloque sin tocar los USD de A)
	txFailedDvP := &Transaction{
		Type:         TxAtomicSwap,
		Sender:       pubA,
		Counterparty: pubB,
		AssetIn:      "USD",
		AmountIn:     500 * ScaleFactor,
		AssetOut:     "BTC",
		AmountOut:    1 * ScaleFactor,
		Nonce:        2,
	}
	txFailedDvP.SignSender(privA)
	txFailedDvP.SignCounterparty(privB)

	b2FailedDvP := &Block{
		Header:       BlockHeader{Height: 2, PrevBlock: b1.Header.Hash(), TxMerkleRoot: ComputeTransactionsRoot([]*Transaction{txFailedDvP})},
		Transactions: []*Transaction{txFailedDvP},
	}
	if _, err := sm.ApplyBlock(b2FailedDvP); err == nil {
		t.Fatal("expected DvP with unfunded counterparty to fail, but it succeeded")
	}

	// El StateRoot debe permanecer intacto tras los bloques rechazados
	if sm.StateRoot() != initialRoot {
		t.Fatal("state root mutated after rejected blocks; atomic rollback failed")
	}
}
