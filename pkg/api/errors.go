package api

import "net/http"

// ErrorCode is the stable, machine-readable classification of a failure. It is
// the field a client branches on: HTTP status is coarse, and the human-readable
// message is free to change without notice.
type ErrorCode string

const (
	CodeBadRequest       ErrorCode = "bad_request"
	CodeUnauthenticated  ErrorCode = "unauthenticated"
	CodeForbidden        ErrorCode = "forbidden"
	CodeNotFound         ErrorCode = "not_found"
	CodeMethodNotAllowed ErrorCode = "method_not_allowed"
	CodeConflict         ErrorCode = "conflict"
	CodeUnprocessable    ErrorCode = "unprocessable"
	CodePayloadTooLarge  ErrorCode = "payload_too_large"
	CodeRateLimited      ErrorCode = "rate_limited"
	CodeTimeout          ErrorCode = "timeout"
	CodeInternal         ErrorCode = "internal"
	CodeUnavailable      ErrorCode = "unavailable"
)

// CodeForStatus gives every error a code derived from its HTTP status. It is
// the floor, not the goal: a handler that can say something more specific
// should pass its own code to WriteErrorCode.
func CodeForStatus(status int) ErrorCode {
	switch status {
	case http.StatusBadRequest:
		return CodeBadRequest
	case http.StatusUnauthorized:
		return CodeUnauthenticated
	case http.StatusForbidden:
		return CodeForbidden
	case http.StatusNotFound:
		return CodeNotFound
	case http.StatusMethodNotAllowed:
		return CodeMethodNotAllowed
	case http.StatusConflict:
		return CodeConflict
	case http.StatusUnprocessableEntity:
		return CodeUnprocessable
	case http.StatusRequestEntityTooLarge:
		return CodePayloadTooLarge
	case http.StatusTooManyRequests:
		return CodeRateLimited
	case http.StatusGatewayTimeout:
		return CodeTimeout
	case http.StatusServiceUnavailable:
		return CodeUnavailable
	}
	if status >= 500 {
		return CodeInternal
	}
	return CodeBadRequest
}
