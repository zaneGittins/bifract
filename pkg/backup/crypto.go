package backup

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	chunkSize    = 64 * 1024 // 64 KiB plaintext per chunk
	nonceSize    = 12        // AES-GCM standard nonce size
	gcmTagSize   = 16        // AES-GCM authentication tag
	lenFieldSize = 4         // uint32 big-endian chunk length prefix
)

var (
	backupKey     []byte
	backupKeyOnce sync.Once
	ErrNoKey      = errors.New("no backup encryption key configured")
)

func loadBackupKey() {
	raw := strings.TrimSpace(os.Getenv("BIFRACT_BACKUP_ENCRYPTION_KEY"))
	if raw == "" {
		return
	}
	if key, err := hex.DecodeString(raw); err == nil && len(key) == 32 {
		backupKey = key
	}
}

// LoadBackupKey returns the backup encryption key from the environment.
func LoadBackupKey() ([]byte, error) {
	backupKeyOnce.Do(loadBackupKey)
	if len(backupKey) == 0 {
		return nil, ErrNoKey
	}
	dst := make([]byte, 32)
	copy(dst, backupKey)
	return dst, nil
}

// encryptingWriter wraps a writer with chunked AES-256-GCM encryption.
// Each chunk is independently encrypted and authenticated.
//
// Wire format per chunk:
//
//	[4 bytes big-endian ciphertext length][12 bytes nonce][ciphertext + 16 byte GCM tag]
//
// A zero-length sentinel (4 zero bytes) marks end of stream.
type encryptingWriter struct {
	w       io.Writer
	gcm     cipher.AEAD
	buf     []byte
	counter uint64
	baseKey []byte // used with counter to derive per-chunk nonce
	closed  bool
}

// NewEncryptingWriter returns a writer that encrypts data in chunks using AES-256-GCM.
// The caller must call Close() to write the end-of-stream sentinel.
func NewEncryptingWriter(w io.Writer, key []byte) (io.WriteCloser, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	// Generate a random base key for nonce derivation
	baseKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, baseKey); err != nil {
		return nil, fmt.Errorf("generate base key: %w", err)
	}

	// Write the base key as the first 32 bytes of the stream (unencrypted).
	// This is safe because the base key is only used for nonce derivation,
	// not for encryption. It ensures the receiver can reconstruct nonces.
	if _, err := w.Write(baseKey); err != nil {
		return nil, fmt.Errorf("write base key: %w", err)
	}

	return &encryptingWriter{
		w:       w,
		gcm:     gcm,
		buf:     make([]byte, 0, chunkSize),
		baseKey: baseKey,
	}, nil
}

func (ew *encryptingWriter) deriveNonce() []byte {
	// HMAC-SHA256(baseKey || counter) truncated to 12 bytes.
	// This gives unique nonces without random collision risk.
	var counterBuf [8]byte
	binary.BigEndian.PutUint64(counterBuf[:], ew.counter)

	h := sha256.New()
	h.Write(ew.baseKey)
	h.Write(counterBuf[:])
	sum := h.Sum(nil)

	nonce := make([]byte, nonceSize)
	copy(nonce, sum[:nonceSize])

	ew.counter++
	return nonce
}

func (ew *encryptingWriter) Write(p []byte) (int, error) {
	if ew.closed {
		return 0, errors.New("write to closed encrypting writer")
	}

	total := 0
	for len(p) > 0 {
		space := chunkSize - len(ew.buf)
		if space <= 0 {
			if err := ew.flushChunk(); err != nil {
				return total, err
			}
			space = chunkSize
		}

		n := len(p)
		if n > space {
			n = space
		}
		ew.buf = append(ew.buf, p[:n]...)
		p = p[n:]
		total += n
	}
	return total, nil
}

func (ew *encryptingWriter) flushChunk() error {
	if len(ew.buf) == 0 {
		return nil
	}

	nonce := ew.deriveNonce()
	ciphertext := ew.gcm.Seal(nil, nonce, ew.buf, nil)

	// Write length prefix
	var lenBuf [lenFieldSize]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(ciphertext)))
	if _, err := ew.w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write chunk length: %w", err)
	}

	// Write nonce
	if _, err := ew.w.Write(nonce); err != nil {
		return fmt.Errorf("write nonce: %w", err)
	}

	// Write ciphertext (includes GCM tag)
	if _, err := ew.w.Write(ciphertext); err != nil {
		return fmt.Errorf("write ciphertext: %w", err)
	}

	ew.buf = ew.buf[:0]
	return nil
}

func (ew *encryptingWriter) Close() error {
	if ew.closed {
		return nil
	}
	ew.closed = true

	// Flush any remaining data
	if err := ew.flushChunk(); err != nil {
		return err
	}

	// Write sentinel (zero-length chunk)
	var sentinel [lenFieldSize]byte
	_, err := ew.w.Write(sentinel[:])
	return err
}

// decryptingReader wraps a reader with chunked AES-256-GCM decryption.
type decryptingReader struct {
	r       io.Reader
	gcm     cipher.AEAD
	buf     []byte
	pos     int
	counter uint64
	baseKey []byte
	done    bool
}

// NewDecryptingReader returns a reader that decrypts chunked AES-256-GCM data.
func NewDecryptingReader(r io.Reader, key []byte) (io.Reader, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	// Read the 32-byte base key
	baseKey := make([]byte, 32)
	if _, err := io.ReadFull(r, baseKey); err != nil {
		return nil, fmt.Errorf("read base key: %w", err)
	}

	return &decryptingReader{
		r:       r,
		gcm:     gcm,
		baseKey: baseKey,
	}, nil
}

func (dr *decryptingReader) deriveNonce() []byte {
	var counterBuf [8]byte
	binary.BigEndian.PutUint64(counterBuf[:], dr.counter)

	h := sha256.New()
	h.Write(dr.baseKey)
	h.Write(counterBuf[:])
	sum := h.Sum(nil)

	nonce := make([]byte, nonceSize)
	copy(nonce, sum[:nonceSize])

	dr.counter++
	return nonce
}

func (dr *decryptingReader) Read(p []byte) (int, error) {
	// Serve from buffer first
	if dr.pos < len(dr.buf) {
		n := copy(p, dr.buf[dr.pos:])
		dr.pos += n
		return n, nil
	}

	if dr.done {
		return 0, io.EOF
	}

	// Read next chunk
	var lenBuf [lenFieldSize]byte
	if _, err := io.ReadFull(dr.r, lenBuf[:]); err != nil {
		return 0, fmt.Errorf("read chunk length: %w", err)
	}

	chunkLen := binary.BigEndian.Uint32(lenBuf[:])
	if chunkLen == 0 {
		// Sentinel: end of stream
		dr.done = true
		return 0, io.EOF
	}

	// Sanity check: max chunk = 64KB plaintext + 16 byte tag + some margin
	const maxChunkLen = chunkSize + gcmTagSize + 1024
	if chunkLen > maxChunkLen {
		return 0, fmt.Errorf("chunk too large: %d bytes (max %d)", chunkLen, maxChunkLen)
	}

	// Read nonce
	nonceBuf := make([]byte, nonceSize)
	if _, err := io.ReadFull(dr.r, nonceBuf); err != nil {
		return 0, fmt.Errorf("read nonce: %w", err)
	}

	// Verify nonce matches expected counter-derived nonce
	expectedNonce := dr.deriveNonce()
	if !nonceEqual(nonceBuf, expectedNonce) {
		return 0, errors.New("nonce mismatch: possible data corruption or tampering")
	}

	// Read ciphertext
	ciphertext := make([]byte, chunkLen)
	if _, err := io.ReadFull(dr.r, ciphertext); err != nil {
		return 0, fmt.Errorf("read ciphertext: %w", err)
	}

	// Decrypt
	plaintext, err := dr.gcm.Open(nil, nonceBuf, ciphertext, nil)
	if err != nil {
		return 0, fmt.Errorf("decrypt chunk: %w", err)
	}

	dr.buf = plaintext
	dr.pos = 0

	n := copy(p, dr.buf[dr.pos:])
	dr.pos += n
	return n, nil
}

func nonceEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var result byte
	for i := range a {
		result |= a[i] ^ b[i]
	}
	return result == 0
}
