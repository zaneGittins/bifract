package spool

import (
	"encoding/binary"
	"fmt"
	"time"

	"bifract/pkg/storage"
)

// The batch payload format (little-endian):
//
//	uint8   codec version (1)
//	uint32  record count
//	repeated record:
//	    int64   Timestamp       (UnixNano)
//	    int64   IngestTimestamp (UnixNano)
//	    string  LogID
//	    string  FractalID
//	    string  RawLog
//	    string  Normalizer  (codec version >= 2 only)
//	    uint32  field count
//	    repeated field: string key, string value
//
// string = uint32 length prefix + raw bytes.

// codecVersion 2 added the Normalizer stamp. Decode still accepts version 1 frames
// (pre-upgrade spool payloads) by leaving Normalizer empty.
const codecVersion = 2

// encodeBatch serializes a batch of log entries into a single payload buffer.
func encodeBatch(logs []storage.LogEntry) []byte {
	// Pre-size the buffer to avoid repeated growth on large batches.
	size := 1 + 4
	for i := range logs {
		size += 8 + 8 + 4 + len(logs[i].LogID) + 4 + len(logs[i].FractalID) + 4 + len(logs[i].RawLog) + 4 + len(logs[i].Normalizer) + 4
		for k, v := range logs[i].Fields {
			size += 4 + len(k) + 4 + len(v)
		}
	}
	buf := make([]byte, 0, size)

	buf = append(buf, codecVersion)
	buf = appendUint32(buf, uint32(len(logs)))
	for i := range logs {
		buf = appendInt64(buf, logs[i].Timestamp.UnixNano())
		buf = appendInt64(buf, logs[i].IngestTimestamp.UnixNano())
		buf = appendString(buf, logs[i].LogID)
		buf = appendString(buf, logs[i].FractalID)
		buf = appendString(buf, logs[i].RawLog)
		buf = appendString(buf, logs[i].Normalizer)
		buf = appendUint32(buf, uint32(len(logs[i].Fields)))
		for k, v := range logs[i].Fields {
			buf = appendString(buf, k)
			buf = appendString(buf, v)
		}
	}
	return buf
}

// decodeBatch deserializes a payload buffer back into log entries.
func decodeBatch(payload []byte) ([]storage.LogEntry, error) {
	r := reader{b: payload}
	ver, err := r.byteVal()
	if err != nil {
		return nil, err
	}
	if ver < 1 || ver > codecVersion {
		return nil, fmt.Errorf("spool: unsupported codec version %d", ver)
	}
	count, err := r.uint32()
	if err != nil {
		return nil, err
	}
	// A record occupies at least minRecordBytes (two int64 timestamps + four
	// length/count uint32s). Reject any count that could not possibly fit in the
	// remaining payload before allocating - guards against a corrupt/misaligned
	// length driving a huge allocation.
	const minRecordBytes = 8 + 8 + 4 + 4 + 4 + 4
	if remain := len(payload) - r.pos; int64(count)*minRecordBytes > int64(remain) {
		return nil, fmt.Errorf("spool: batch count %d exceeds payload capacity (%d bytes)", count, remain)
	}
	logs := make([]storage.LogEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		var e storage.LogEntry
		tsNano, err := r.int64()
		if err != nil {
			return nil, err
		}
		itsNano, err := r.int64()
		if err != nil {
			return nil, err
		}
		e.Timestamp = time.Unix(0, tsNano).UTC()
		e.IngestTimestamp = time.Unix(0, itsNano).UTC()
		if e.LogID, err = r.str(); err != nil {
			return nil, err
		}
		if e.FractalID, err = r.str(); err != nil {
			return nil, err
		}
		if e.RawLog, err = r.str(); err != nil {
			return nil, err
		}
		if ver >= 2 {
			if e.Normalizer, err = r.str(); err != nil {
				return nil, err
			}
		}
		fieldCount, err := r.uint32()
		if err != nil {
			return nil, err
		}
		if fieldCount > 0 {
			e.Fields = make(map[string]string, fieldCount)
			for j := uint32(0); j < fieldCount; j++ {
				k, err := r.str()
				if err != nil {
					return nil, err
				}
				v, err := r.str()
				if err != nil {
					return nil, err
				}
				e.Fields[k] = v
			}
		}
		logs = append(logs, e)
	}
	return logs, nil
}

func appendUint32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}

func appendInt64(b []byte, v int64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(v))
	return append(b, tmp[:]...)
}

func appendString(b []byte, s string) []byte {
	b = appendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// reader is a bounds-checked cursor over a payload buffer.
type reader struct {
	b   []byte
	pos int
}

func (r *reader) byteVal() (byte, error) {
	if r.pos+1 > len(r.b) {
		return 0, errShort
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}

func (r *reader) uint32() (uint32, error) {
	if r.pos+4 > len(r.b) {
		return 0, errShort
	}
	v := binary.LittleEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *reader) int64() (int64, error) {
	if r.pos+8 > len(r.b) {
		return 0, errShort
	}
	v := int64(binary.LittleEndian.Uint64(r.b[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *reader) str() (string, error) {
	n, err := r.uint32()
	if err != nil {
		return "", err
	}
	if r.pos+int(n) > len(r.b) {
		return "", errShort
	}
	s := string(r.b[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, nil
}

var errShort = fmt.Errorf("spool: truncated batch payload")
