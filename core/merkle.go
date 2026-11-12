package core

import (
	"crypto/sha256"
	"sort"
)

// ComputeMerkleRoot construye un Árbol de Merkle binario a partir de una lista de hashes.
func ComputeMerkleRoot(hashes [][32]byte) [32]byte {
	if len(hashes) == 0 {
		return sha256.Sum256(nil)
	}

	currentLayer := make([][32]byte, len(hashes))
	copy(currentLayer, hashes)

	for len(currentLayer) > 1 {
		// Si el número de nodos es impar, duplicamos el último para emparejarlo
		if len(currentLayer)%2 != 0 {
			currentLayer = append(currentLayer, currentLayer[len(currentLayer)-1])
		}

		var nextLayer [][32]byte
		for i := 0; i < len(currentLayer); i += 2 {
			var combined [64]byte
			copy(combined[:32], currentLayer[i][:])
			copy(combined[32:], currentLayer[i+1][:])
			parentHash := sha256.Sum256(combined[:])
			nextLayer = append(nextLayer, parentHash)
		}
		currentLayer = nextLayer
	}

	return currentLayer[0]
}

// ComputeTransactionsRoot calcula el Merkle Root de todas las transacciones de un bloque.
func ComputeTransactionsRoot(txs []*Transaction) [32]byte {
	hashes := make([][32]byte, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash()
	}
	return ComputeMerkleRoot(hashes)
}

// ComputeStateRoot ordena todas las cuentas por dirección y calcula el Merkle Root global.
// Como los mapas en Go tienen orden aleatorio, ordenar las claves primero es crítico para el determinismo.
func ComputeStateRoot(accounts map[string]*Account) [32]byte {
	if len(accounts) == 0 {
		return sha256.Sum256(nil)
	}

	addresses := make([]string, 0, len(accounts))
	for addr := range accounts {
		addresses = append(addresses, addr)
	}
	sort.Strings(addresses)

	leafHashes := make([][32]byte, 0, len(addresses))
	for _, addr := range addresses {
		leafHashes = append(leafHashes, accounts[addr].Hash())
	}

	return ComputeMerkleRoot(leafHashes)
}
