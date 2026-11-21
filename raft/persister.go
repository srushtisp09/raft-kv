package raft

import (
	"errors"
	"os"
	"sync"
)

// Persister defines how Raft state is saved and loaded.
type Persister interface {
	Save(data []byte) error
	Load() ([]byte, error)
}

// MemPersister stores data in memory.
type MemPersister struct {
	mu   sync.Mutex
	data []byte
}

// Save stores a copy of the data.
func (p *MemPersister) Save(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data = append([]byte(nil), data...)
	return nil
}

// Load returns a copy of the stored data.
func (p *MemPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.data == nil {
		return nil, nil
	}

	return append([]byte(nil), p.data...), nil
}

// FilePersister stores data in a file.
type FilePersister struct {
	mu   sync.Mutex
	path string
}

// NewFilePersister creates a file-based persister.
func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path}
}

// Save writes data safely using a temporary file and atomic rename.
func (p *FilePersister) Save(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tempPath := p.path + ".tmp"

	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	// Remove the temporary file if saving fails.
	defer os.Remove(tempPath)

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	// Force file contents to disk.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// Replace the old file with the complete new file.
	if err := os.Rename(tempPath, p.path); err != nil {
		return err
	}

	// Stretch goal: syncing the parent directory after rename
	// can make the directory entry update durable across crashes.
	// This is platform/filesystem dependent and is skipped here.

	return nil
}

// Load reads the saved data.
func (p *FilePersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)

	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return data, nil
}

// Ensure both implementations satisfy the interface.
var _ Persister = (*MemPersister)(nil)
var _ Persister = (*FilePersister)(nil)

// Keep filepath imported only if you later choose to create parent directories.
// For now, the caller must ensure the parent directory exists.
