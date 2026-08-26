package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

// Response is the envelope every JSON endpoint returns. It replaces the
// per-package envelopes that each package used to declare, so a client sees one
// shape across the whole API and a generated schema has one type to name.
//
// Data is generic so a handler can state its payload type; Response[any] is the
// untyped form packages start from.
type Response[T any] struct {
	Success bool   `json:"success"`
	Data    T      `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
	// Code classifies a failure for machine consumption. Error carries the
	// same failure in words, and its wording is not part of the contract.
	Code ErrorCode `json:"code,omitempty"`
}

// Page describes which slice of a collection a response carries. Total counts
// the whole collection, not the slice, so a client can size its own controls.
type Page struct {
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// PageParams reads limit and offset from the query string, clamping limit to
// [1, maxLimit] and falling back to defLimit when absent or unusable. A limit of
// 0 for defLimit means "no bound unless the caller asks for one".
func PageParams(r *http.Request, defLimit, maxLimit int) (limit, offset int) {
	limit = defLimit
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v > 0 {
		offset = v
	}
	return limit, offset
}

// Slice returns the requested window of items and the Page describing it. It
// bounds the response, not the query: callers that can push the limit into SQL
// should do that instead.
func Slice[T any](items []T, limit, offset int) ([]T, Page) {
	page := Page{Total: len(items), Limit: limit, Offset: offset}
	if offset >= len(items) {
		return []T{}, page
	}
	end := len(items)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return items[offset:end], page
}

// PagedResponse is the envelope for a slice of a collection. It exists rather
// than reusing Response so that data is always present: a page of an empty
// collection must answer "data": [], not omit the field and leave a client
// unable to tell an empty page from a missing one.
type PagedResponse[T any] struct {
	Success bool `json:"success"`
	Data    []T  `json:"data"`
	Page    Page `json:"page"`
}

// WritePage answers 200 with one slice of a collection.
func WritePage[T any](w http.ResponseWriter, data []T, page Page) {
	if data == nil {
		data = []T{}
	}
	WriteJSON(w, http.StatusOK, PagedResponse[T]{Success: true, Data: data, Page: page})
}

// WriteJSON encodes v as the response body. Encoding failures cannot be
// reported to the client, since the status and part of the body may already be
// on the wire, so they are logged instead of silently dropped.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	// Any error envelope written through here gets a code, including ones a
	// package builds itself rather than going through WriteError.
	if resp, ok := v.(Response[any]); ok && !resp.Success && resp.Error != "" && resp.Code == "" {
		resp.Code = CodeForStatus(status)
		v = resp
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[api] encoding %d response: %v", status, err)
	}
}

// WriteSuccess answers 200 with a payload.
func WriteSuccess[T any](w http.ResponseWriter, data T) {
	WriteJSON(w, http.StatusOK, Response[T]{Success: true, Data: data})
}

// WriteMessage answers 200 with a payload and a human-readable message.
func WriteMessage[T any](w http.ResponseWriter, message string, data T) {
	WriteJSON(w, http.StatusOK, Response[T]{Success: true, Message: message, Data: data})
}

// WriteError answers with an error envelope, classifying the failure from its
// HTTP status. Use WriteErrorCode where the handler knows something more
// specific than the status conveys.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteErrorCode(w, status, CodeForStatus(status), message)
}

// WriteErrorCode answers with an error envelope carrying an explicit code.
func WriteErrorCode(w http.ResponseWriter, status int, code ErrorCode, message string) {
	WriteJSON(w, status, Response[any]{Success: false, Error: message, Code: code})
}
