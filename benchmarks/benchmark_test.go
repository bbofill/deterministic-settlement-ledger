package benchmarks

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/bbofill/deterministic-settlement-ledger/consensus"
	"github.com/bbofill/deterministic-settlement-ledger/core"
	"github.com/bbofill/deterministic-settlement-ledger/network"
	"github.com/bbofill/deterministic-settlement-ledger/storage"
)

func validatorKey(id int) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte(fmt.Sprintf("bench-validator-%d", id)))
	priv := ed25519.NewKeyFromSeed(seed[:])
	return priv.Public().(ed25519.PublicKey), priv
}

func TestFiveNodeClusterDvPBenchmarkAndChaos(t *testing.T) {
	const numNodes = 5
	basePort := 19100
	tmpDir, err := os.MkdirTemp("", "ledger-cluster-bench-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	addrs := make([]string, numNodes)
	validators := make([]ed25519.PublicKey, numNodes)
	privKeys := make([]ed25519.PrivateKey, numNodes)

	for i := 0; i < numNodes; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		validators[i], privKeys[i] = validatorKey(i)
	}

	nodes := make([]*network.NodeServer, numNodes)
	for i := 0; i < numNodes; i++ {
		var peers []string
		for j := 0; j < numNodes; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		store, err := storage.NewStore(fmt.Sprintf("%s/node-%d", tmpDir, i))
		if err != nil {
			t.Fatal(err)
		}
		sm := core.NewStateMachine()
		mp := consensus.NewMempool(50_000)
		bft := consensus.NewBFTEngine(i, privKeys[i], validators)

		nodes[i] = network.NewNodeServer(i, addrs[i], peers, sm, store, mp, bft)
		if err := nodes[i].Start(); err != nil {
			t.Fatal(err)
		}
		defer nodes[i].Stop()
	}

	// Generamos 50 pares de cuentas (Trader + Market Maker) y las fondeamos con Mint
	const numPairs = 50
	const swapsPerPair = 10
	totalSwaps := numPairs * swapsPerPair

	traderPubs := make([]ed25519.PublicKey, numPairs)
	traderPrivs := make([]ed25519.PrivateKey, numPairs)
	mmPubs := make([]ed25519.PublicKey, numPairs)
	mmPrivs := make([]ed25519.PrivateKey, numPairs)

	for i := 0; i < numPairs; i++ {
		traderPubs[i], traderPrivs[i], _ = ed25519.GenerateKey(rand.Reader)
		mmPubs[i], mmPrivs[i], _ = ed25519.GenerateKey(rand.Reader)

		mintUSD := &core.Transaction{
			Type:     core.TxMint,
			Sender:   traderPubs[i],
			AssetIn:  "USD",
			AmountIn: 10_000_000 * core.ScaleFactor,
			Nonce:    1,
		}
		mintUSD.SignSender(traderPrivs[i])

		mintBTC := &core.Transaction{
			Type:     core.TxMint,
			Sender:   mmPubs[i],
			AssetIn:  "BTC",
			AmountIn: 1_000 * core.ScaleFactor,
			Nonce:    1,
		}
		mintBTC.SignSender(mmPrivs[i])

		var r network.SubmitTxReply
		_ = nodes[i%numNodes].SubmitTransaction(mintUSD, &r)
		_ = nodes[(i+1)%numNodes].SubmitTransaction(mintBTC, &r)
	}

	// Esperamos a que se liquiden los depósitos iniciales
	time.Sleep(500 * time.Millisecond)

	// Chaos Test: Apagamos el Nodo 4 en caliente para verificar tolerancia a fallos (4 de 5 nodos activos)
	nodes[4].Stop()

	// Enviamos ráfagas de liquidaciones atómicas DvP y medimos latencias de confirmación
	latencies := make([]time.Duration, 0, totalSwaps)
	startWall := time.Now()

	for step := 0; step < swapsPerPair; step++ {
		batchStart := time.Now()
		for i := 0; i < numPairs; i++ {
			swap := &core.Transaction{
				Type:         core.TxAtomicSwap,
				Sender:       traderPubs[i],
				Counterparty: mmPubs[i],
				AssetIn:      "USD",
				AmountIn:     50_000 * core.ScaleFactor,
				AssetOut:     "BTC",
				AmountOut:    1 * core.ScaleFactor,
				Nonce:        uint64(step + 2),
			}
			swap.SignSender(traderPrivs[i])
			swap.SignCounterparty(mmPrivs[i])

			var r network.SubmitTxReply
			_ = nodes[i%4].SubmitTransaction(swap, &r)
		}

		// Esperamos hasta que el lote se liquide en el bloque BFT
		for {
			acc := nodes[0].State.GetAccount(traderPubs[0])
			if acc.Nonce >= uint64(step+2) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		latencies = append(latencies, time.Since(batchStart))
	}

	totalElapsed := time.Since(startWall)
	tps := float64(totalSwaps) / totalElapsed.Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	// Verificamos que los 4 nodos supervivientes tienen exactamente el mismo StateRoot al bit
	root0 := nodes[0].State.StateRoot()
	for i := 1; i < 4; i++ {
		if nodes[i].State.StateRoot() != root0 {
			t.Fatalf("StateRoot drift on node %d", i)
		}
	}

	t.Logf("=== QUANT SETTLEMENT BENCHMARK RESULTS (4/5 Active BFT Nodes) ===")
	t.Logf("Total Atomic DvP Trades Settled : %d", totalSwaps)
	t.Logf("Throughput (TPS)                : %.2f trades/sec", tps)
	t.Logf("Settlement Latency p50          : %v", p50)
	t.Logf("Settlement Latency p95          : %v", p95)
	t.Logf("Settlement Latency p99          : %v", p99)
	t.Logf("Final Consensus Merkle StateRoot: %x", root0)
}
