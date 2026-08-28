package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/api"
	"bifract/pkg/mfa"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

const (
	// totpIssuer is what an authenticator app shows above the account.
	totpIssuer = "Bifract"

	recoveryCodeCount  = 10
	recoveryCodeGroups = 3
	recoveryCodeGroup  = 4
	// recoveryAlphabet omits characters that are easy to confuse when a code is
	// read off a screen and typed back in.
	recoveryAlphabet = "abcdefghjkmnpqrstuvwxyz23456789"
)

// mfaVerifyPaths are the only endpoints a session may reach before it has
// passed the second factor.
var mfaVerifyPaths = map[string]bool{
	"/api/v1/auth/mfa/verify": true,
	"/api/v1/auth/user":       true,
	"/api/v1/auth/logout":     true,
}

// mfaEnrollPaths are what a user who must still enroll may reach.
var mfaEnrollPaths = map[string]bool{
	"/api/v1/auth/mfa/enroll":  true,
	"/api/v1/auth/mfa/confirm": true,
	"/api/v1/auth/mfa/status":  true,
	"/api/v1/auth/user":        true,
	"/api/v1/auth/logout":      true,
}

// mfaEnrollmentRequired reports whether the deployment requires this user to
// enroll before doing anything else. SSO accounts are exempt: their identity
// provider owns the second factor.
//
// The key check is not redundant with the one on the setting. If the pepper is
// ever removed or rotated away while the setting is on, enrollment cannot
// succeed, and gating on the setting alone would lock every local account out of
// an instance with no way back in.
func mfaEnrollmentRequired(user *storage.User) bool {
	if user == nil || user.TOTPEnrolled || user.AuthProvider == "oidc" {
		return false
	}
	return settings.Get().RequireMFA && mfa.KeyAvailable()
}

// MFAStatusResponse describes a user's second factor state.
type MFAStatusResponse struct {
	Enrolled               bool `json:"enrolled"`
	Pending                bool `json:"pending"`
	Required               bool `json:"required"`
	Available              bool `json:"available"`
	RecoveryCodesRemaining int  `json:"recovery_codes_remaining"`
}

// MFAEnrollResponse carries what the user needs to add the account to an
// authenticator app. The secret is returned exactly once, at enrollment.
type MFAEnrollResponse struct {
	Secret string `json:"secret"`
	URI    string `json:"uri"`
	QRSVG  string `json:"qr_svg"`
}

// MFACodeRequest is a submitted authenticator code or recovery code.
type MFACodeRequest struct {
	Code         string `json:"code,omitempty"`
	RecoveryCode string `json:"recovery_code,omitempty"`
}

// MFADisableRequest confirms identity before removing the second factor.
type MFADisableRequest struct {
	Password     string `json:"password"`
	Code         string `json:"code,omitempty"`
	RecoveryCode string `json:"recovery_code,omitempty"`
}

// MFAPasswordRequest re-confirms a password for a sensitive MFA operation.
type MFAPasswordRequest struct {
	Password string `json:"password"`
}

// AdminResetMFARequest names the user whose enrollment to clear.
type AdminResetMFARequest struct {
	Username string `json:"username"`
}

// hashRecoveryCode returns the digest stored for a recovery code. The codes are
// high entropy, so SHA-256 is enough and matches how invite tokens are stored.
func hashRecoveryCode(code string) string {
	h := sha256.Sum256([]byte(normalizeRecoveryCode(code)))
	return hex.EncodeToString(h[:])
}

// normalizeRecoveryCode makes matching insensitive to how the user typed it.
func normalizeRecoveryCode(code string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(code) {
		if strings.ContainsRune(recoveryAlphabet, r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// generateRecoveryCodes returns a fresh set of codes and their stored hashes.
func generateRecoveryCodes() ([]string, []string, error) {
	codes := make([]string, 0, recoveryCodeCount)
	hashes := make([]string, 0, recoveryCodeCount)
	for i := 0; i < recoveryCodeCount; i++ {
		buf := make([]byte, recoveryCodeGroups*recoveryCodeGroup)
		if _, err := rand.Read(buf); err != nil {
			return nil, nil, err
		}
		var groups []string
		for g := 0; g < recoveryCodeGroups; g++ {
			var chunk strings.Builder
			for c := 0; c < recoveryCodeGroup; c++ {
				chunk.WriteByte(recoveryAlphabet[int(buf[g*recoveryCodeGroup+c])%len(recoveryAlphabet)])
			}
			groups = append(groups, chunk.String())
		}
		code := strings.Join(groups, "-")
		codes = append(codes, code)
		hashes = append(hashes, hashRecoveryCode(code))
	}
	return codes, hashes, nil
}

// HandleMFAStatus reports the current user's second factor state.
func (h *AuthHandler) HandleMFAStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	user := r.Context().Value("user").(*storage.User)

	remaining := 0
	if user.TOTPEnrolled {
		if n, err := h.pg.CountUnusedRecoveryCodes(r.Context(), user.Username); err == nil {
			remaining = n
		}
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data: MFAStatusResponse{
			Enrolled:               user.TOTPEnrolled,
			Pending:                !user.TOTPEnrolled && user.TOTPSecret != "",
			Required:               settings.Get().RequireMFA && user.AuthProvider != "oidc",
			Available:              mfa.KeyAvailable(),
			RecoveryCodesRemaining: remaining,
		},
	})
}

// HandleMFAEnroll issues a new secret and the QR code to scan. The secret is
// stored unenrolled: it does nothing until the user proves they can produce a
// code, so an abandoned setup cannot lock anyone out.
func (h *AuthHandler) HandleMFAEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	user := r.Context().Value("user").(*storage.User)

	if user.AuthProvider == "oidc" {
		api.WriteError(w, http.StatusBadRequest, "SSO accounts get their second factor from the identity provider")
		return
	}
	if !mfa.KeyAvailable() {
		api.WriteError(w, http.StatusServiceUnavailable, "Two-factor authentication needs BIFRACT_PASSWORD_PEPPER set so enrollment secrets can be encrypted at rest")
		return
	}
	if user.TOTPEnrolled {
		api.WriteError(w, http.StatusConflict, "This account already has an authenticator. Remove it before enrolling a new one.")
		return
	}

	secret, err := mfa.GenerateSecret()
	if err != nil {
		log.Printf("[Auth] Failed to generate TOTP secret for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to start enrollment")
		return
	}
	sealed, err := mfa.Seal(secret)
	if err != nil {
		log.Printf("[Auth] Failed to encrypt TOTP secret for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to start enrollment")
		return
	}
	if err := h.pg.StartUserTOTP(r.Context(), user.Username, sealed); err != nil {
		log.Printf("[Auth] Failed to store TOTP secret for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to start enrollment")
		return
	}

	uri := mfa.ProvisioningURI(totpIssuer, user.Username, secret)
	svg, err := mfa.SVG(uri)
	if err != nil {
		// Manual entry still works, so this is not fatal to enrollment.
		log.Printf("[Auth] Failed to render enrollment QR for %s: %v", user.Username, err)
	}

	h.logAuthEvent("mfa_enroll_started", user.Username, clientIP(r), "")

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    MFAEnrollResponse{Secret: secret, URI: uri, QRSVG: svg},
	})
}

// HandleMFAConfirm completes enrollment and hands back the recovery codes.
func (h *AuthHandler) HandleMFAConfirm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	user := r.Context().Value("user").(*storage.User)
	ip := clientIP(r)

	if user.TOTPEnrolled {
		api.WriteError(w, http.StatusConflict, "This account already has an authenticator")
		return
	}
	if user.TOTPSecret == "" {
		api.WriteError(w, http.StatusBadRequest, "Start enrollment before confirming a code")
		return
	}

	var req MFACodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if h.mfaLimiter.check(user.Username) {
		h.logAuthEvent("mfa_enroll_failed", user.Username, ip, "rate limited")
		api.WriteError(w, http.StatusTooManyRequests, "Too many incorrect codes. Please wait and try again.")
		return
	}

	secret, err := mfa.Open(user.TOTPSecret)
	if err != nil {
		log.Printf("[Auth] Failed to decrypt TOTP secret for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Could not read the enrollment secret. Start enrollment again.")
		return
	}

	counter, err := mfa.Validate(secret, req.Code, time.Now().Unix(), user.TOTPLastCounter)
	if err != nil {
		h.mfaLimiter.recordFailure(user.Username)
		h.logAuthEvent("mfa_enroll_failed", user.Username, ip, "invalid code")
		api.WriteError(w, http.StatusUnauthorized, "That code is not correct. Check your authenticator and try again.")
		return
	}

	codes, hashes, err := generateRecoveryCodes()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to generate recovery codes")
		return
	}
	if err := h.pg.ConfirmUserTOTP(r.Context(), user.Username, counter); err != nil {
		log.Printf("[Auth] Failed to confirm TOTP for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to complete enrollment")
		return
	}
	if err := h.pg.ReplaceRecoveryCodes(r.Context(), user.Username, hashes); err != nil {
		log.Printf("[Auth] Failed to store recovery codes for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Enrollment saved but recovery codes could not be stored. Regenerate them from your account settings.")
		return
	}

	h.mfaLimiter.reset(user.Username)
	h.logAuthEvent("mfa_enrolled", user.Username, ip, "")

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Two-factor authentication is on for your account",
		Data:    map[string]interface{}{"recovery_codes": codes},
	})
}

// HandleMFAVerify completes a login that is waiting on a second factor. On
// success the half-authenticated session is replaced rather than promoted, so a
// session id observed before the code cannot be used after it.
func (h *AuthHandler) HandleMFAVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	user := r.Context().Value("user").(*storage.User)
	ip := clientIP(r)

	pending, _ := r.Context().Value("mfa_pending").(bool)
	if !pending {
		api.WriteError(w, http.StatusBadRequest, "This session is not waiting for a verification code")
		return
	}

	var req MFACodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if h.mfaLimiter.check(user.Username) {
		h.logAuthEvent("mfa_failed", user.Username, ip, "rate limited")
		api.WriteError(w, http.StatusTooManyRequests, "Too many incorrect codes. Please wait and try again.")
		return
	}

	usedRecovery, err := h.consumeSecondFactor(r, user, req.Code, req.RecoveryCode)
	if err != nil {
		h.mfaLimiter.recordFailure(user.Username)
		h.logAuthEvent("mfa_failed", user.Username, ip, err.Error())
		api.WriteError(w, http.StatusUnauthorized, "That code is not correct. Check your authenticator and try again.")
		return
	}

	// Replace the pending session with a full one.
	if cookie, cerr := r.Cookie(sessionCookieName); cerr == nil {
		h.deleteSession(cookie.Value)
	}
	sessionID, err := h.createSession(user.Username)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to create session")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.secureCookies,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(sessionDuration.Seconds()),
	})

	h.mfaLimiter.reset(user.Username)
	h.loginLimiter.reset(ip)
	if err := h.pg.UpdateLastLogin(r.Context(), user.Username); err != nil {
		log.Printf("Failed to update last login for %s: %v", user.Username, err)
	}

	detail := ""
	remaining := 0
	if usedRecovery {
		detail = "recovery code"
		remaining, _ = h.pg.CountUnusedRecoveryCodes(r.Context(), user.Username)
		h.logAuthEvent("mfa_recovery_used", user.Username, ip, fmt.Sprintf("%d codes remaining", remaining))
	}
	h.logAuthEvent("login_success", user.Username, ip, detail)

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Verification successful",
		Data: map[string]interface{}{
			"used_recovery_code":       usedRecovery,
			"recovery_codes_remaining": remaining,
		},
	})
}

// consumeSecondFactor validates and spends either an authenticator code or a
// recovery code. Both are spent in the database, so a replay of either loses the
// race rather than authenticating twice.
func (h *AuthHandler) consumeSecondFactor(r *http.Request, user *storage.User, code, recovery string) (bool, error) {
	if strings.TrimSpace(recovery) != "" {
		ok, err := h.pg.ConsumeRecoveryCode(r.Context(), user.Username, hashRecoveryCode(recovery))
		if err != nil {
			return false, err
		}
		if !ok {
			return false, errors.New("invalid recovery code")
		}
		return true, nil
	}

	if user.TOTPSecret == "" {
		return false, errors.New("no authenticator enrolled")
	}
	secret, err := mfa.Open(user.TOTPSecret)
	if err != nil {
		return false, err
	}
	counter, err := mfa.Validate(secret, code, time.Now().Unix(), user.TOTPLastCounter)
	if err != nil {
		return false, err
	}
	spent, err := h.pg.SpendTOTPCounter(r.Context(), user.Username, counter)
	if err != nil {
		return false, err
	}
	if !spent {
		return false, mfa.ErrCodeReused
	}
	return false, nil
}

// HandleMFADisable removes a user's second factor. It needs the password and a
// current code, so a hijacked session alone cannot strip the protection.
func (h *AuthHandler) HandleMFADisable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	user := r.Context().Value("user").(*storage.User)
	ip := clientIP(r)

	if !user.TOTPEnrolled {
		api.WriteError(w, http.StatusBadRequest, "This account does not have an authenticator")
		return
	}
	if settings.Get().RequireMFA {
		api.WriteError(w, http.StatusForbidden, "Your administrator requires two-factor authentication. Ask them to reset it if you have lost your device.")
		return
	}

	var req MFADisableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if h.mfaLimiter.check(user.Username) {
		api.WriteError(w, http.StatusTooManyRequests, "Too many incorrect attempts. Please wait and try again.")
		return
	}
	if err := verifyPassword(user.PasswordHash, req.Password); err != nil {
		h.mfaLimiter.recordFailure(user.Username)
		h.logAuthEvent("mfa_disable_failed", user.Username, ip, "invalid password")
		api.WriteError(w, http.StatusUnauthorized, "Password is incorrect")
		return
	}
	if _, err := h.consumeSecondFactor(r, user, req.Code, req.RecoveryCode); err != nil {
		h.mfaLimiter.recordFailure(user.Username)
		h.logAuthEvent("mfa_disable_failed", user.Username, ip, "invalid code")
		api.WriteError(w, http.StatusUnauthorized, "That code is not correct")
		return
	}

	if err := h.pg.ClearUserTOTP(r.Context(), user.Username); err != nil {
		log.Printf("[Auth] Failed to clear TOTP for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to remove the authenticator")
		return
	}

	h.mfaLimiter.reset(user.Username)
	h.logAuthEvent("mfa_disabled", user.Username, ip, "")

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Two-factor authentication removed",
	})
}

// HandleMFARecoveryCodes issues a fresh set of recovery codes, invalidating the
// previous ones so a leaked list cannot outlive its replacement.
func (h *AuthHandler) HandleMFARecoveryCodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	user := r.Context().Value("user").(*storage.User)
	ip := clientIP(r)

	if !user.TOTPEnrolled {
		api.WriteError(w, http.StatusBadRequest, "This account does not have an authenticator")
		return
	}

	var req MFAPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if h.mfaLimiter.check(user.Username) {
		api.WriteError(w, http.StatusTooManyRequests, "Too many incorrect attempts. Please wait and try again.")
		return
	}
	if err := verifyPassword(user.PasswordHash, req.Password); err != nil {
		h.mfaLimiter.recordFailure(user.Username)
		api.WriteError(w, http.StatusUnauthorized, "Password is incorrect")
		return
	}

	codes, hashes, err := generateRecoveryCodes()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to generate recovery codes")
		return
	}
	if err := h.pg.ReplaceRecoveryCodes(r.Context(), user.Username, hashes); err != nil {
		log.Printf("[Auth] Failed to replace recovery codes for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to store recovery codes")
		return
	}

	h.mfaLimiter.reset(user.Username)
	h.logAuthEvent("mfa_recovery_regenerated", user.Username, ip, "")

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "New recovery codes generated. The previous set no longer works.",
		Data:    map[string]interface{}{"recovery_codes": codes},
	})
}

// HandleAdminResetMFA clears a user's enrollment. This is the way back in for
// someone who has lost both their device and their recovery codes.
func (h *AuthHandler) HandleAdminResetMFA(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	admin := r.Context().Value("user").(*storage.User)

	var req AdminResetMFARequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.Username == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}

	target, err := h.pg.GetUser(r.Context(), req.Username)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "User not found")
		return
	}
	if !target.TOTPEnrolled && target.TOTPSecret == "" {
		api.WriteError(w, http.StatusBadRequest, "This account does not have an authenticator")
		return
	}

	if err := h.pg.ClearUserTOTP(r.Context(), req.Username); err != nil {
		log.Printf("[Auth] Failed to reset TOTP for %s: %v", req.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to reset two-factor authentication")
		return
	}
	// Sign them out so the next login goes through enrollment again.
	h.invalidateUserSessions(req.Username)

	h.logAuthEvent("mfa_reset", req.Username, clientIP(r), fmt.Sprintf("reset by admin %s", admin.Username))

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Two-factor authentication reset. The user will enroll again at their next sign in.",
	})
}
