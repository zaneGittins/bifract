// Package jsonenc encodes JSON the way log content is stored and searched.
package jsonenc

import (
	"bytes"
	"encoding/json"
)

// Marshal is json.Marshal without HTML escaping, so '<', '>' and '&' stay literal
// as sent and match the hot store's toString(fields) that BQL search reads.
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
