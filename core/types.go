package core

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"
)

// ScaleFactor define 8 decimales en punto fijo (1 unidad = 100_000_000)
const ScaleFactor int64 = 100_000_000

type TxType uint8

const (
	TxMint       TxType = 0
	TxTransfer   TxType = 1
	TxAtomicSwap TxType = 2 // Liquidación atómica Delivery-versus-Payment (DvP)
)

// Account representa el estado contable de un participante en el ledger.
type Account struct {
	Address  string           // Hex de la clave pública Ed25519
	Balances map[string]int64 // Activo -> Saldo en punto fijo (int64)
	Nonce    uint64           // Contador estricto anti-replay
}

func NewAccount(address string) *Account {
	return &Account{
		Address:  address,
		Balances: make(map[string]int64),
		Nonce:    0,
	}
}

// Clone crea una copia profunda de la cuenta para poder revertir si un bloque falla.
func (a *Account) Clone() *Account {
	cloned := &Account{
		Address:  a.Address,
		Balances: make(map[string]int64, len(a.Balances)),
		Nonce:    a.Nonce,
	}
	for k, v := range a.Balances {
		cloned.Balances[k] = v
	}
	return cloned
}

// Hash calcula la huella SHA-256 determinista de la cuenta ordenando las divisas alfabéticamente.
func (a *Account) Hash() [32]byte {
	buf := new(bytes.Buffer)
	buf.WriteString(a.Address)
	_ = binary.Write(buf, binary.BigEndian, a.Nonce)

	assets := make([]string, 0, len(a.Balances))
	for asset := range a.Balances {
		assets = append(assets, asset)
	}
	sort.Strings(assets)

	for _, asset := range assets {
		buf.WriteString(asset)
		_ = binary.Write(buf, binary.BigEndian, a.Balances[asset])
	}
	return sha256.Sum256(buf.Bytes())
}

// Transaction representa una instrucción financiera firmada.
type Transaction struct {
	Type            TxType
	Sender          ed25519.PublicKey
	Counterparty    ed25519.PublicKey
	AssetIn         string
	AmountIn        int64
	AssetOut        string
	AmountOut       int64
	Nonce           uint64
	SenderSig       []byte
	CounterpartySig []byte
}

// SigningPayload genera los bytes exactos en formato binario que deben firmarse (sin las firmas).
func (tx *Transaction) SigningPayload() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(tx.Type))
	buf.Write(tx.Sender)
	buf.Write(tx.Counterparty)
	buf.WriteString(tx.AssetIn)
	_ = binary.Write(buf, binary.BigEndian, tx.AmountIn)
	buf.WriteString(tx.AssetOut)
	_ = binary.Write(buf, binary.BigEndian, tx.AmountOut)
	_ = binary.Write(buf, binary.BigEndian, tx.Nonce)
	return buf.Bytes()
}

// Hash devuelve el identificador único SHA-256 de la transacción incluyendo sus firmas.
func (tx *Transaction) Hash() [32]byte {
	buf := bytes.NewBuffer(tx.SigningPayload())
	buf.Write(tx.SenderSig)
	buf.Write(tx.CounterpartySig)
	return sha256.Sum256(buf.Bytes())
}

func (tx *Transaction) HashHex() string {
	h := tx.Hash()
	return hex.EncodeToString(h[:])
}

// SignSender firma la transacción con la clave privada del emisor.
func (tx *Transaction) SignSender(priv ed25519.PrivateKey) {
	tx.SenderSig = ed25519.Sign(priv, tx.SigningPayload())
}

// SignCounterparty firma la transacción con la clave privada de la contraparte (para DvP).
func (tx *Transaction) SignCounterparty(priv ed25519.PrivateKey) {
	tx.CounterpartySig = ed25519.Sign(priv, tx.SigningPayload())
}

// VerifySignatures comprueba matemáticamente las firmas Ed25519.
func (tx *Transaction) VerifySignatures() error {
	if len(tx.Sender) != ed25519.PublicKeySize {
		return errors.New("invalid sender public key size")
	}
	payload := tx.SigningPayload()
	if !ed25519.Verify(tx.Sender, payload, tx.SenderSig) {
		return errors.New("invalid sender Ed25519 signature")
	}

	// En un Atomic Swap (DvP), ambas partes deben haber firmado el acuerdo.
	if tx.Type == TxAtomicSwap {
		if len(tx.Counterparty) != ed25519.PublicKeySize {
			return errors.New("invalid counterparty public key size for DvP")
		}
		if !ed25519.Verify(tx.Counterparty, payload, tx.CounterpartySig) {
			return errors.New("invalid counterparty Ed25519 signature for DvP")
		}
	}
	return nil
}

// BlockHeader contiene los metadatos criptográficos que encadenan cada bloque.
type BlockHeader struct {
	Height       uint64
	PrevBlock    [32]byte
	StateRoot    [32]byte
	TxMerkleRoot [32]byte
	TimestampMs  int64
	ProposerPub  ed25519.PublicKey
	ProposerSig  []byte
}

func (bh *BlockHeader) SigningPayload() []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, bh.Height)
	buf.Write(bh.PrevBlock[:])
	buf.Write(bh.StateRoot[:])
	buf.Write(bh.TxMerkleRoot[:])
	_ = binary.Write(buf, binary.BigEndian, bh.TimestampMs)
	buf.Write(bh.ProposerPub)
	return buf.Bytes()
}

func (bh *BlockHeader) Hash() [32]byte {
	return sha256.Sum256(bh.SigningPayload())
}

// Block representa una página sellada del libro mayor.
type Block struct {
	Header       BlockHeader
	Transactions []*Transaction
}
