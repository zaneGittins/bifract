package backup

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return key
}

func TestRoundTrip_Empty(t *testing.T) {
	key := testKey()
	var buf bytes.Buffer

	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewDecryptingReader(&buf, key)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty output, got %d bytes", len(out))
	}
}

func TestRoundTrip_SmallData(t *testing.T) {
	key := testKey()
	data := []byte("hello world")
	var buf bytes.Buffer

	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewDecryptingReader(&buf, key)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("data mismatch: got %q, want %q", out, data)
	}
}

func TestRoundTrip_ExactChunkSize(t *testing.T) {
	key := testKey()
	data := make([]byte, chunkSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewDecryptingReader(&buf, key)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("data mismatch for exact chunk size")
	}
}

func TestRoundTrip_MultiChunk(t *testing.T) {
	key := testKey()
	// 3.5 chunks worth of data
	data := make([]byte, chunkSize*3+chunkSize/2)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewDecryptingReader(&buf, key)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("data mismatch for multi-chunk")
	}
}

func TestRoundTrip_IncrementalWrites(t *testing.T) {
	key := testKey()
	var buf bytes.Buffer

	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}

	// Write many small pieces that span multiple chunks
	var expected bytes.Buffer
	for i := 0; i < 5000; i++ {
		line := []byte("log entry number something something\n")
		expected.Write(line)
		if _, err := w.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewDecryptingReader(&buf, key)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, expected.Bytes()) {
		t.Fatalf("data mismatch: got %d bytes, want %d bytes", len(out), expected.Len())
	}
}

func TestWrongKey(t *testing.T) {
	key1 := testKey()
	key2 := make([]byte, 32)
	copy(key2, key1)
	key2[0] ^= 0xFF // flip one byte

	data := []byte("secret data")
	var buf bytes.Buffer

	w, err := NewEncryptingWriter(&buf, key1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewDecryptingReader(&buf, key2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("expected error decrypting with wrong key")
	}
}

func TestTruncatedStream(t *testing.T) {
	key := testKey()
	data := make([]byte, chunkSize*2)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncate: remove sentinel and part of last chunk
	truncated := buf.Bytes()[:buf.Len()-20]

	r, err := NewDecryptingReader(bytes.NewReader(truncated), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("expected error for truncated stream")
	}
}

func TestCorruptedChunk(t *testing.T) {
	key := testKey()
	data := []byte("test data for corruption check")
	var buf bytes.Buffer

	w, err := NewEncryptingWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	corrupted := buf.Bytes()
	// Corrupt a byte in the ciphertext area (after base key + length + nonce)
	corruptIdx := 32 + lenFieldSize + nonceSize + 5
	if corruptIdx < len(corrupted) {
		corrupted[corruptIdx] ^= 0xFF
	}

	r, err := NewDecryptingReader(bytes.NewReader(corrupted), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("expected error for corrupted data")
	}
}

func TestInvalidKeyLength(t *testing.T) {
	shortKey := make([]byte, 16)
	var buf bytes.Buffer

	_, err := NewEncryptingWriter(&buf, shortKey)
	if err == nil {
		t.Fatal("expected error for short key")
	}

	_, err = NewDecryptingReader(&buf, shortKey)
	if err == nil {
		t.Fatal("expected error for short key")
	}
}
