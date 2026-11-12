package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bbofill/deterministic-settlement-ledger/consensus"
	"github.com/bbofill/deterministic-settlement-ledger/core"
	"github.com/bbofill/deterministic-settlement-ledger/network"
	"github.com/bbofill/deterministic-settlement-ledger/storage"
)

// DeterministicKeyPair genera claves Ed25519 reproducibles para los 5 validadores del clúster.
func DeterministicKeyPair(nodeID int) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte(fmt.Sprintf("quant-validator-node-seed-%d", nodeID)))
	priv := ed25519.NewKeyFromSeed(seed[:])
	return priv.Public().(ed25519.PublicKey), priv
}

func main() {
	nodeID := flag.Int("id", 0, "Validator node ID (0..N-1)")
	listen := flag.String("listen", "127.0.0.1:9000", "TCP RPC listen address")
	peersFlag := flag.String("peers", "", "Comma-separated peer addresses")
	totalNodes := flag.Int("total", 5, "Total number of BFT validators")
	dataDir := flag.String("data", "./data/node0", "WAL storage directory")
	flag.Parse()

	validators := make([]ed25519.PublicKey, *totalNodes)
	var myPriv ed25519.PrivateKey
	for i := 0; i < *totalNodes; i++ {
		pub, priv := DeterministicKeyPair(i)
		validators[i] = pub
		if i == *nodeID {
			myPriv = priv
		}
	}

	sm := core.NewStateMachine()
	store, err := storage.NewStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize WAL storage: %v", err)
	}
	defer store.Close()

	if err := store.RecoverState(sm); err != nil {
		log.Fatalf("Failed to recover state from WAL: %v", err)
	}

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	mp := consensus.NewMempool(50_000)
	bft := consensus.NewBFTEngine(*nodeID, myPriv, validators)
	server := network.NewNodeServer(*nodeID, *listen, peers, sm, store, mp, bft)

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start node server: %v", err)
	}

	log.Printf("[Node %d] Online at %s | Height: %d | StateRoot: %x",
		*nodeID, *listen, sm.Height(), sm.StateRoot())

	// Monitor de estado cada 2 segundos
	go func() {
		for range time.Tick(2 * time.Second) {
			log.Printf("[Node %d] Height=%d | Mempool=%d | StateRoot=%x",
				*nodeID, sm.Height(), mp.Size(), sm.StateRoot()[:6])
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("[Node %d] Shutting down gracefully...", *nodeID)
	server.Stop()
}
