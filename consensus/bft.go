package consensus

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bbofill/deterministic-settlement-ledger/core"
)

// Vote representa el voto firmado con Ed25519 por un nodo validador sobre un bloque propuesto.
type Vote struct {
	Height       uint64
	BlockHash    [32]byte
	StateRoot    [32]byte
	ValidatorPub ed25519.PublicKey
	Signature    []byte
	Approve      bool
}

func (v *Vote) SigningPayload() []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, v.Height)
	buf.Write(v.BlockHash[:])
	buf.Write(v.StateRoot[:])
	if v.Approve {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	return buf.Bytes()
}

func (v *Vote) Sign(priv ed25519.PrivateKey) {
	v.Signature = ed25519.Sign(priv, v.SigningPayload())
}

func (v *Vote) Verify() bool {
	if len(v.ValidatorPub) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(v.ValidatorPub, v.SigningPayload(), v.Signature)
}

// BFTEngine coordina la selección de líder y verifica el quórum bizantino (> 2/3 de los nodos).
type BFTEngine struct {
	NodeID     int
	PrivKey    ed25519.PrivateKey
	PubKey     ed25519.PublicKey
	Validators map[string]ed25519.PublicKey // Hex PubKey -> PublicKey
	TotalNodes int
}

func NewBFTEngine(nodeID int, priv ed25519.PrivateKey, validators []ed25519.PublicKey) *BFTEngine {
	valMap := make(map[string]ed25519.PublicKey, len(validators))
	for _, pub := range validators {
		valMap[hex.EncodeToString(pub)] = pub
	}
	return &BFTEngine{
		NodeID:     nodeID,
		PrivKey:    priv,
		PubKey:     priv.Public().(ed25519.PublicKey),
		Validators: valMap,
		TotalNodes: len(validators),
	}
}

// QuorumSize devuelve el mínimo de votos necesarios: floor(2*N/3) + 1 (ej. 4 de 5 nodos).
func (bft *BFTEngine) QuorumSize() int {
	return (2*bft.TotalNodes)/3 + 1
}

// IsProposer determina por turno rotatorio si este nodo es el líder para la altura dada.
func (bft *BFTEngine) IsProposer(height uint64) bool {
	return int(height%uint64(bft.TotalNodes)) == bft.NodeID
}

// BuildProposal construye y firma un nuevo bloque candidato listo para someterse a votación BFT.
func (bft *BFTEngine) BuildProposal(height uint64, prevHash [32]byte, stateRoot [32]byte, txs []*core.Transaction) *core.Block {
	header := core.BlockHeader{
		Height:       height,
		PrevBlock:    prevHash,
		StateRoot:    stateRoot,
		TxMerkleRoot: core.ComputeTransactionsRoot(txs),
		TimestampMs:  time.Now().UnixMilli(),
		ProposerPub:  bft.PubKey,
	}
	header.ProposerSig = ed25519.Sign(bft.PrivKey, header.SigningPayload())

	return &core.Block{
		Header:       header,
		Transactions: txs,
	}
}

// VerifyQuorum comprueba que un bloque tenga al menos QuorumSize votos válidos de validadores distintos.
func (bft *BFTEngine) VerifyQuorum(block *core.Block, votes []Vote) error {
	blockHash := block.Header.Hash()
	seenValidators := make(map[string]bool)
	validApprovals := 0

	for _, vote := range votes {
		if !vote.Approve || vote.Height != block.Header.Height || vote.BlockHash != blockHash {
			continue
		}
		if vote.StateRoot != block.Header.StateRoot {
			continue
		}
		valHex := hex.EncodeToString(vote.ValidatorPub)
		if _, authorized := bft.Validators[valHex]; !authorized {
			continue
		}
		if seenValidators[valHex] {
			continue // Evita que un mismo nodo vote dos veces
		}
		if !vote.Verify() {
			continue
		}

		seenValidators[valHex] = true
		validApprovals++
	}

	if validApprovals < bft.QuorumSize() {
		return fmt.Errorf("insufficient BFT quorum: got %d approvals, need %d", validApprovals, bft.QuorumSize())
	}
	return nil
}

// CreateVote genera el voto firmado de este nodo para un bloque propuesto.
func (bft *BFTEngine) CreateVote(block *core.Block, computedStateRoot [32]byte, approve bool) (Vote, error) {
	if len(block.Header.ProposerPub) != ed25519.PublicKeySize {
		return Vote{}, errors.New("invalid proposer public key")
	}
	if !ed25519.Verify(block.Header.ProposerPub, block.Header.SigningPayload(), block.Header.ProposerSig) {
		return Vote{}, errors.New("invalid proposer block signature")
	}

	v := Vote{
		Height:       block.Header.Height,
		BlockHash:    block.Header.Hash(),
		StateRoot:    computedStateRoot,
		ValidatorPub: bft.PubKey,
		Approve:      approve,
	}
	v.Sign(bft.PrivKey)
	return v, nil
}
