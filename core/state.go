package core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// StateMachine gestiona el libro contable en memoria de forma determinista y thread-safe.
type StateMachine struct {
	mu            sync.RWMutex
	accounts      map[string]*Account
	height        uint64
	lastBlockHash [32]byte
	stateRoot     [32]byte
}

func NewStateMachine() *StateMachine {
	sm := &StateMachine{
		accounts: make(map[string]*Account),
		height:   0,
	}
	sm.stateRoot = ComputeStateRoot(sm.accounts)
	return sm
}

func (sm *StateMachine) StateRoot() [32]byte {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.stateRoot
}

func (sm *StateMachine) Height() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.height
}

func (sm *StateMachine) GetAccount(pubKey []byte) *Account {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	addr := hex.EncodeToString(pubKey)
	if acc, exists := sm.accounts[addr]; exists {
		return acc.Clone()
	}
	return NewAccount(addr)
}

// SortTransactionsDeterministic ordena un lote de transacciones de forma canónica
// (primero por Hash de transacción y respetando secuencia de Nonce por emisor)
// para evitar que el líder manipule el orden arbitrariamente (mitigación de Front-Running).
func SortTransactionsDeterministic(txs []*Transaction) {
	sort.Slice(txs, func(i, j int) bool {
		if bytes.Equal(txs[i].Sender, txs[j].Sender) {
			return txs[i].Nonce < txs[j].Nonce
		}
		h1 := txs[i].Hash()
		h2 := txs[j].Hash()
		return bytes.Compare(h1[:], h2[:]) < 0
	})
}

// ApplyBlock ejecuta un bloque entero de forma atómica: o todas las transacciones son válidas
// y el StateRoot coincide con el propuesto, o el estado entero se revierte sin cambios.
func (sm *StateMachine) ApplyBlock(block *Block) ([32]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if block.Header.Height != sm.height+1 {
		return [32]byte{}, fmt.Errorf("invalid block height: expected %d, got %d", sm.height+1, block.Header.Height)
	}
	if sm.height > 0 && block.Header.PrevBlock != sm.lastBlockHash {
		return [32]byte{}, errors.New("prev_block_hash mismatch")
	}

	expectedTxRoot := ComputeTransactionsRoot(block.Transactions)
	if block.Header.TxMerkleRoot != expectedTxRoot {
		return [32]byte{}, errors.New("tx_merkle_root mismatch")
	}

	// Snapshot en memoria por si alguna transacción falla a mitad del bloque (atomicidad de bloque)
	stagingAccounts := make(map[string]*Account, len(sm.accounts))
	for k, v := range sm.accounts {
		stagingAccounts[k] = v.Clone()
	}

	for idx, tx := range block.Transactions {
		if err := applyTransaction(stagingAccounts, tx); err != nil {
			return [32]byte{}, fmt.Errorf("tx %d (%s) failed: %w", idx, tx.HashHex()[:8], err)
		}
	}

	newStateRoot := ComputeStateRoot(stagingAccounts)
	if block.Header.StateRoot != ([32]byte{}) && block.Header.StateRoot != newStateRoot {
		return [32]byte{}, errors.New("deterministic state_root mismatch across nodes")
	}

	// Commit definitivo del estado
	sm.accounts = stagingAccounts
	sm.height = block.Header.Height
	sm.stateRoot = newStateRoot
	sm.lastBlockHash = block.Header.Hash()

	return newStateRoot, nil
}

// applyTransaction aplica la lógica financiera en punto fijo sobre el mapa de cuentas.
func applyTransaction(accounts map[string]*Account, tx *Transaction) error {
	if err := tx.VerifySignatures(); err != nil {
		return err
	}
	if tx.AmountIn <= 0 {
		return errors.New("amount_in must be strictly positive")
	}
	if tx.AssetIn == "" {
		return errors.New("asset_in cannot be empty")
	}

	senderAddr := hex.EncodeToString(tx.Sender)
	senderAcc, exists := accounts[senderAddr]
	if !exists {
		senderAcc = NewAccount(senderAddr)
		accounts[senderAddr] = senderAcc
	}

	// Verificación estricta de Nonce secuencial (evita Replay Attacks)
	if tx.Nonce != senderAcc.Nonce+1 {
		return fmt.Errorf("invalid nonce for %s: expected %d, got %d", senderAddr[:8], senderAcc.Nonce+1, tx.Nonce)
	}

	switch tx.Type {
	case TxMint:
		// Emisión / Depósito de liquidez inicial en la cuenta del emisor
		senderAcc.Balances[tx.AssetIn] += tx.AmountIn
		senderAcc.Nonce++

	case TxTransfer:
		if len(tx.Counterparty) == 0 {
			return errors.New("transfer requires a recipient counterparty")
		}
		if senderAcc.Balances[tx.AssetIn] < tx.AmountIn {
			return fmt.Errorf("insufficient %s balance for transfer", tx.AssetIn)
		}

		recipientAddr := hex.EncodeToString(tx.Counterparty)
		recipientAcc, ok := accounts[recipientAddr]
		if !ok {
			recipientAcc = NewAccount(recipientAddr)
			accounts[recipientAddr] = recipientAcc
		}

		senderAcc.Balances[tx.AssetIn] -= tx.AmountIn
		recipientAcc.Balances[tx.AssetIn] += tx.AmountIn
		senderAcc.Nonce++

	case TxAtomicSwap:
		// Liquidación Atómica Delivery-versus-Payment (DvP):
		// Sender entrega (AmountIn de AssetIn) y recibe (AmountOut de AssetOut)
		// Counterparty entrega (AmountOut de AssetOut) y recibe (AmountIn de AssetIn)
		if tx.AmountOut <= 0 || tx.AssetOut == "" {
			return errors.New("atomic swap requires positive amount_out and valid asset_out")
		}
		if bytes.Equal(tx.Sender, tx.Counterparty) {
			return errors.New("self-trading is not allowed in atomic swap")
		}

		cpAddr := hex.EncodeToString(tx.Counterparty)
		cpAcc, ok := accounts[cpAddr]
		if !ok {
			return errors.New("counterparty account does not exist")
		}

		// Comprobación simultánea de fondos en ambas patas antes de mover un solo céntimo
		if senderAcc.Balances[tx.AssetIn] < tx.AmountIn {
			return fmt.Errorf("sender insufficient %s balance for DvP", tx.AssetIn)
		}
		if cpAcc.Balances[tx.AssetOut] < tx.AmountOut {
			return fmt.Errorf("counterparty insufficient %s balance for DvP", tx.AssetOut)
		}

		// Ejecución atómica de las 4 actualizaciones contables
		senderAcc.Balances[tx.AssetIn] -= tx.AmountIn
		cpAcc.Balances[tx.AssetIn] += tx.AmountIn

		cpAcc.Balances[tx.AssetOut] -= tx.AmountOut
		senderAcc.Balances[tx.AssetOut] += tx.AmountOut

		senderAcc.Nonce++

	default:
		return errors.New("unsupported transaction type")
	}

	return nil
}

func (sm *StateMachine) LastBlockHash() [32]byte {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastBlockHash
}

// PreviewBlock simula la ejecución de un bloque sin mutar el estado oficial para calcular su StateRoot.
func (sm *StateMachine) PreviewBlock(block *Block) ([32]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if block.Header.Height != sm.height+1 {
		return [32]byte{}, fmt.Errorf("invalid preview height: expected %d, got %d", sm.height+1, block.Header.Height)
	}

	stagingAccounts := make(map[string]*Account, len(sm.accounts))
	for k, v := range sm.accounts {
		stagingAccounts[k] = v.Clone()
	}

	for _, tx := range block.Transactions {
		if err := applyTransaction(stagingAccounts, tx); err != nil {
			return [32]byte{}, err
		}
	}
	return ComputeStateRoot(stagingAccounts), nil
}
