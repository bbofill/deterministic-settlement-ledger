package network

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/bbofill/deterministic-settlement-ledger/consensus"
	"github.com/bbofill/deterministic-settlement-ledger/core"
	"github.com/bbofill/deterministic-settlement-ledger/storage"
)

type SubmitTxReply struct {
	Accepted bool
	TxHash   string
	ErrorMsg string
}

type CommitArgs struct {
	Block *core.Block
	Votes []consensus.Vote
}

type CommitReply struct {
	Success   bool
	StateRoot [32]byte
	ErrorMsg  string
}

// NodeServer une la máquina de estados, el almacenamiento WAL, la mempool y el motor BFT.
type NodeServer struct {
	mu         sync.Mutex
	NodeID     int
	ListenAddr string
	Peers      []string
	State      *core.StateMachine
	Store      *storage.Store
	Mempool    *consensus.Mempool
	BFT        *consensus.BFTEngine
	listener   net.Listener
	stopCh     chan struct{}
}

func NewNodeServer(
	nodeID int,
	listenAddr string,
	peers []string,
	sm *core.StateMachine,
	store *storage.Store,
	mp *consensus.Mempool,
	bft *consensus.BFTEngine,
) *NodeServer {
	return &NodeServer{
		NodeID:     nodeID,
		ListenAddr: listenAddr,
		Peers:      peers,
		State:      sm,
		Store:      store,
		Mempool:    mp,
		BFT:        bft,
		stopCh:     make(chan struct{}),
	}
}

// Start levanta el servidor TCP binario y el bucle de consenso cada 200ms.
func (ns *NodeServer) Start() error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("SettlementNode", ns); err != nil {
		return err
	}

	l, err := net.Listen("tcp", ns.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", ns.ListenAddr, err)
	}
	ns.listener = l

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-ns.stopCh:
					return
				default:
					continue
				}
			}
			go srv.ServeConn(conn)
		}
	}()

	go ns.consensusLoop()
	return nil
}

func (ns *NodeServer) Stop() {
	close(ns.stopCh)
	if ns.listener != nil {
		_ = ns.listener.Close()
	}
}

// SubmitTransaction recibe una orden de un cliente o par y la propaga a los demás nodos.
func (ns *NodeServer) SubmitTransaction(tx *core.Transaction, reply *SubmitTxReply) error {
	if err := ns.Mempool.Add(tx); err != nil {
		reply.Accepted = false
		reply.ErrorMsg = err.Error()
		return nil
	}
	reply.Accepted = true
	reply.TxHash = tx.HashHex()

	// Difusión asíncrona (Gossip) al resto de nodos del clúster
	go ns.gossipTransaction(tx)
	return nil
}

// ProposeBlock es llamado por el líder del turno para que este validador verifique y vote el bloque.
func (ns *NodeServer) ProposeBlock(block *core.Block, voteReply *consensus.Vote) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	// Clonamos el estado actual en seco para comprobar si el bloque es 100% legal y coincide el StateRoot
	expectedRoot, err := ns.State.PreviewBlock(block)
	if err != nil {
		v, _ := ns.BFT.CreateVote(block, [32]byte{}, false)
		*voteReply = v
		return nil
	}

	v, err := ns.BFT.CreateVote(block, expectedRoot, true)
	if err != nil {
		return err
	}
	*voteReply = v
	return nil
}

// CommitBlock aplica definitivamente el bloque cuando ya cuenta con el quórum BFT de >= 2/3 votos.
func (ns *NodeServer) CommitBlock(args *CommitArgs, reply *CommitReply) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if args.Block.Header.Height <= ns.State.Height() {
		reply.Success = true
		reply.StateRoot = ns.State.StateRoot()
		return nil
	}

	if err := ns.BFT.VerifyQuorum(args.Block, args.Votes); err != nil {
		reply.Success = false
		reply.ErrorMsg = err.Error()
		return nil
	}

	newRoot, err := ns.State.ApplyBlock(args.Block)
	if err != nil {
		reply.Success = false
		reply.ErrorMsg = err.Error()
		return nil
	}

	if err := ns.Store.AppendBlock(args.Block); err != nil {
		log.Printf("[Node %d] WAL append error: %v", ns.NodeID, err)
	}

	ns.Mempool.RemoveCommitted(args.Block.Transactions)
	reply.Success = true
	reply.StateRoot = newRoot
	return nil
}

// consensusLoop ejecuta ventanas discretas de liquidación cada 200ms.
func (ns *NodeServer) consensusLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ns.stopCh:
			return
		case <-ticker.C:
			nextHeight := ns.State.Height() + 1
			if !ns.BFT.IsProposer(nextHeight) {
				continue
			}
			ns.proposeAndCommitRound(nextHeight)
		}
	}
}

func (ns *NodeServer) proposeAndCommitRound(height uint64) {
	batch := ns.Mempool.ReapBatch(500)
	if len(batch) == 0 {
		return
	}

	prevHash := ns.State.LastBlockHash()
	tempBlock := &core.Block{
		Header: core.BlockHeader{
			Height:       height,
			PrevBlock:    prevHash,
			TxMerkleRoot: core.ComputeTransactionsRoot(batch),
		},
		Transactions: batch,
	}

	previewRoot, err := ns.State.PreviewBlock(tempBlock)
	if err != nil {
		// Si alguna transacción del lote es inválida (ej. sin saldo), limpiamos el lote
		ns.Mempool.RemoveCommitted(batch)
		return
	}

	proposal := ns.BFT.BuildProposal(height, prevHash, previewRoot, batch)

	// Recogemos el voto propio y pedimos votos a todos los pares en paralelo
	var votesMu sync.Mutex
	votes := make([]consensus.Vote, 0, len(ns.Peers)+1)

	selfVote, err := ns.BFT.CreateVote(proposal, previewRoot, true)
	if err == nil {
		votes = append(votes, selfVote)
	}

	var wg sync.WaitGroup
	for _, peerAddr := range ns.Peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var vote consensus.Vote
			if err := callRPC(addr, "SettlementNode.ProposeBlock", proposal, &vote); err == nil && vote.Approve {
				votesMu.Lock()
				votes = append(votes, vote)
				votesMu.Unlock()
			}
		}(peerAddr)
	}
	wg.Wait()

	if err := ns.BFT.VerifyQuorum(proposal, votes); err != nil {
		return
	}

	// Quórum alcanzado: comprometemos en local y ordenamos Commit a todos los nodos
	commitArgs := &CommitArgs{Block: proposal, Votes: votes}
	var localReply CommitReply
	_ = ns.CommitBlock(commitArgs, &localReply)

	for _, peerAddr := range ns.Peers {
		go func(addr string) {
			var r CommitReply
			_ = callRPC(addr, "SettlementNode.CommitBlock", commitArgs, &r)
		}(peerAddr)
	}
}

func (ns *NodeServer) gossipTransaction(tx *core.Transaction) {
	for _, peerAddr := range ns.Peers {
		go func(addr string) {
			var r SubmitTxReply
			_ = callRPC(addr, "SettlementNode.SubmitTransaction", tx, &r)
		}(peerAddr)
	}
}

func callRPC(addr string, method string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(300 * time.Millisecond))

	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}
