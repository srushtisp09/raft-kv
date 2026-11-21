package raft

import (
	"bytes"
	"testing"
)

func TestMemPersisterSaveAndLoad(t *testing.T) {
	p := &MemPersister{}

	original := []byte("raft-state")

	if err := p.Save(original); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(loaded, original) {
		t.Fatalf("expected %q, got %q", original, loaded)
	}
}

func TestMemPersisterLoadBeforeSave(t *testing.T) {
	p := &MemPersister{}

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if loaded != nil {
		t.Fatalf("expected nil, got %v", loaded)
	}
}

func TestMemPersisterSaveTwice(t *testing.T) {
	p := &MemPersister{}

	if err := p.Save([]byte("first")); err != nil {
		t.Fatal(err)
	}

	if err := p.Save([]byte("second")); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(loaded, []byte("second")) {
		t.Fatalf("expected second, got %q", loaded)
	}
}

func TestMemPersisterCopiesData(t *testing.T) {
	p := &MemPersister{}

	original := []byte("original")
	if err := p.Save(original); err != nil {
		t.Fatal(err)
	}

	// Change the original after saving.
	original[0] = 'X'

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(loaded, []byte("original")) {
		t.Fatalf("saved data changed unexpectedly: %q", loaded)
	}

	// Change the loaded copy.
	loaded[0] = 'Y'

	again, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(again, []byte("original")) {
		t.Fatalf("stored data changed after modifying loaded copy: %q", again)
	}
}

func TestFilePersisterSaveAndLoad(t *testing.T) {
	path := t.TempDir() + "/raft-state"
	p := NewFilePersister(path)

	original := []byte("raft-file-state")

	if err := p.Save(original); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(loaded, original) {
		t.Fatalf("expected %q, got %q", original, loaded)
	}
}

func TestFilePersisterLoadBeforeSave(t *testing.T) {
	path := t.TempDir() + "/raft-state"
	p := NewFilePersister(path)

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if loaded != nil {
		t.Fatalf("expected nil, got %v", loaded)
	}
}

func TestFilePersisterSaveTwice(t *testing.T) {
	path := t.TempDir() + "/raft-state"
	p := NewFilePersister(path)

	if err := p.Save([]byte("first")); err != nil {
		t.Fatal(err)
	}

	if err := p.Save([]byte("second")); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(loaded, []byte("second")) {
		t.Fatalf("expected second, got %q", loaded)
	}
}
