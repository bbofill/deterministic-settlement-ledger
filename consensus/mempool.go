package consensus

import (
	"errors"
	"sync"

	"github.com/bbofill/deterministic-settlement-ledger/core"
)

// Mempool almacena transacciones pendientes de liquidación en memoria RAM de forma concurrente.
type Mempool struct {
	mu      sync.Mutex
	pending map[string]*core.Transaction
	order   []*core.Transaction
	maxSize int
}

func NewMempool(maxSize int) *Mempool {
	return &Mempool{
		pending: make(map[string]*core.Transaction, maxSize),
		order:   make([]*core.Transaction, 0, maxSize),
		maxSize: maxSize,
	}
}

// Add valida la firma criptográfica e inserta la transacción si no está duplicada.
func (mp *Mempool) Add(tx *core.Transaction) error {
	if err := tx.VerifySignatures(); err != nil {
		return err
	}

	txHash := tx.HashHex()

	mp.mu.Lock()
	defer mp.mu.Unlock()

	if _, exists := mp.pending[txHash]; exists {
		return nil // Idempotente: ya la tenemos en la bandeja de entrada
	}
	if len(mp.pending) >= mp.maxSize {
		return errors.New("mempool capacity exceeded")
	}

	mp.pending[txHash] = tx
	mp.order = append(mp.order, tx)
	return nil
}

// ReapBatch extrae hasta maxCount transacciones y las ordena con la regla determinista anti-MEV.
func (mp *Mempool) ReapBatch(maxCount int) []*core.Transaction {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if len(mp.order) == 0 {
		return nil
	}

	count := len(mp.order)
	if count > maxCount {
		count = maxCount
	}

	batch := make([]*core.Transaction, count)
	copy(batch, mp.order[:count])

	// Aplicamos la ordenación canónica determinista (Frequent Batch Auction ordering)
	core.SortTransactionsDeterministic(batch)
	return batch
}

// RemoveCommitted elimina de la mempool las transacciones que ya han sido selladas en un bloque.
func (mp *Mempool) RemoveCommitted(txs []*core.Transaction) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	for _, tx := range txs {
		delete(mp.pending, tx.HashHex())
	}

	remaining := make([]*core.Transaction, 0, len(mp.pending))
	for _, tx := range mp.order {
		if _, stillPending := mp.pending[tx.HashHex()]; stillPending {
			remaining = append(remaining, tx)
		}
	}
	mp.order = remaining
}

func (mp *Mempool) Size() int {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return len(mp.pending)
}
