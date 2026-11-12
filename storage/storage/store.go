package storage

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bbofill/deterministic-settlement-ledger/core"
)

// Store gestiona la persistencia duradera de bloques en disco mediante un log binario Append-Only.
type Store struct {
	mu       sync.RWMutex
	filePath string
	file     *os.File
	blocks   map[uint64]*core.Block
}

// NewStore abre o crea el archivo WAL en disco y carga los bloques previos en memoria.
func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "ledger.wal")
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	s := &Store{
		filePath: walPath,
		file:     f,
		blocks:   make(map[uint64]*core.Block),
	}

	if err := s.loadFromDisk(); err != nil {
		_ = f.Close()
		return nil, err
	}

	return s, nil
}

// AppendBlock escribe un bloque en formato binario Gob precedido por su tamaño (4 bytes) y fuerza fsync.
func (s *Store) AppendBlock(block *core.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.blocks[block.Header.Height]; exists {
		return fmt.Errorf("block height %d already persisted", block.Header.Height)
	}

	// Serializamos el bloque en un buffer temporal para conocer su longitud exacta
	pr, pw := io.Pipe()
	var buf []byte
	done := make(chan error, 1)

	go func() {
		var err error
		buf, err = io.ReadAll(pr)
		done <- err
	}()

	enc := gob.NewEncoder(pw)
	if err := enc.Encode(block); err != nil {
		_ = pw.Close()
		return fmt.Errorf("gob encode block failed: %w", err)
	}
	_ = pw.Close()

	if err := <-done; err != nil {
		return err
	}

	// Escribimos [4 bytes longitud BigEndian] + [payload binario]
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(buf)))

	if _, err := s.file.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("failed writing record length: %w", err)
	}
	if _, err := s.file.Write(buf); err != nil {
		return fmt.Errorf("failed writing block payload: %w", err)
	}

	// fsync garantiza que el bloque está físicamente grabado en el disco antes de confirmar
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}

	s.blocks[block.Header.Height] = block
	return nil
}

// GetBlock recupera un bloque por su altura.
func (s *Store) GetBlock(height uint64) (*core.Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[height]
	return b, ok
}

// RecoverState reconstruye toda la StateMachine desde el bloque 1 tras un reinicio o caída del nodo.
func (s *Store) RecoverState(sm *core.StateMachine) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for h := uint64(1); h <= uint64(len(s.blocks)); h++ {
		block, ok := s.blocks[h]
		if !ok {
			return fmt.Errorf("corrupted WAL: missing block at height %d", h)
		}
		if _, err := sm.ApplyBlock(block); err != nil {
			return fmt.Errorf("state recovery failed at height %d: %w", h, err)
		}
	}
	return nil
}

func (s *Store) loadFromDisk() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(s.file, lenBuf[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading WAL header: %w", err)
		}

		recordLen := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, recordLen)
		if _, err := io.ReadFull(s.file, payload); err != nil {
			return fmt.Errorf("truncated WAL block payload: %w", err)
		}

		var block core.Block
		dec := gob.NewDecoder(bytesReader(payload))
		if err := dec.Decode(&block); err != nil {
			return fmt.Errorf("corrupted gob block in WAL: %w", err)
		}

		s.blocks[block.Header.Height] = &block
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

type byteSliceReader struct {
	data []byte
	pos  int
}

func bytesReader(b []byte) io.Reader {
	return &byteSliceReader{data: b}
}

func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
