package auth

import (
	"bifract/pkg/api"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"path/filepath"

	"bifract/internal/setup"
	"bifract/pkg/fractals"
	"bifract/pkg/ingest"
	"bifract/pkg/rbac"
	"bifract/pkg/settings"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"
)

const (
	bcryptCost        = 12
	inviteTokenBytes  = 32 // 64 hex chars
	inviteExpiry      = 7 * 24 * time.Hour
	minPasswordLength = 12

	// Login rate limiting
	loginMaxFailures    = 5 // failures before blocking
	loginBlockDuration  = 15 * time.Minute
	loginWindowDuration = 15 * time.Minute

	// Default admin password used in init-postgres.sql (hashed without pepper)
	defaultAdminPassword = "bifract"
)

var errSessionNotFound = fmt.Errorf("session not found")

// loginAttempt tracks failed login attempts for an IP
type loginAttempt struct {
	failures     int
	firstFailure time.Time
	blockedUntil time.Time
}

// loginRateLimiter provides per-IP brute force protection
type loginRateLimiter struct {
	mu       sync.Mutex
	attempts map[string]*loginAttempt
}

func newLoginRateLimiter() *loginRateLimiter {
	rl := &loginRateLimiter{
		attempts: make(map[string]*loginAttempt),
	}
	go rl.cleanup()
	return rl
}

// cleanup removes stale entries every 5 minutes
func (rl *loginRateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, a := range rl.attempts {
			if now.After(a.blockedUntil) && now.Sub(a.firstFailure) > loginWindowDuration {
				delete(rl.attempts, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// check returns true if the IP is currently blocked
func (rl *loginRateLimiter) check(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	a, exists := rl.attempts[ip]
	if !exists {
		return false
	}

	now := time.Now()

	// Currently blocked
	if now.Before(a.blockedUntil) {
		return true
	}

	// Window expired, reset
	if now.Sub(a.firstFailure) > loginWindowDuration {
		delete(rl.attempts, ip)
		return false
	}

	return false
}

// recordFailure records a failed login attempt and returns true if now blocked
func (rl *loginRateLimiter) recordFailure(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	a, exists := rl.attempts[ip]

	if !exists || now.Sub(a.firstFailure) > loginWindowDuration {
		rl.attempts[ip] = &loginAttempt{
			failures:     1,
			firstFailure: now,
		}
		return false
	}

	a.failures++
	if a.failures >= loginMaxFailures {
		a.blockedUntil = now.Add(loginBlockDuration)
		return true
	}

	return false
}

// reset clears rate limit state for an IP after successful login
func (rl *loginRateLimiter) reset(ip string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.attempts, ip)
}

// hashInviteToken returns the SHA-256 hex digest of a raw invite token.
func hashInviteToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// generateInviteToken creates a cryptographically random invite token
// and returns (plaintext, sha256Hash).
func generateInviteToken() (string, string, error) {
	b := make([]byte, inviteTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	plain := hex.EncodeToString(b)
	return plain, hashInviteToken(plain), nil
}

// pepperPassword applies HMAC-SHA256 with the server pepper before bcrypt.
// If no pepper is configured, the password is returned as-is.
// This provides defense-in-depth: even if the DB is compromised,
// the attacker cannot crack hashes without the pepper.
func pepperPassword(password string) string {
	pepper := os.Getenv("BIFRACT_PASSWORD_PEPPER")
	if pepper == "" {
		return password
	}
	mac := hmac.New(sha256.New, []byte(pepper))
	mac.Write([]byte(password))
	// Return hex-encoded HMAC so it stays within bcrypt's 72-byte limit
	return hex.EncodeToString(mac.Sum(nil))
}

// hashPassword applies optional pepper then bcrypt with cost 12.
func hashPassword(password string) (string, error) {
	peppered := pepperPassword(password)
	hash, err := bcrypt.GenerateFromPassword([]byte(peppered), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// verifyPassword checks a plaintext password against a bcrypt hash,
// applying the pepper if configured.
func verifyPassword(storedHash, password string) error {
	peppered := pepperPassword(password)
	return bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(peppered))
}

const (
	sessionCookieName = "bifract_session"
	sessionDuration   = 24 * time.Hour
	// mfaPendingDuration bounds how long a password-only session may sit
	// waiting for a code.
	mfaPendingDuration = 5 * time.Minute
)

type Session struct {
	Username        string
	CreatedAt       time.Time
	ExpiresAt       time.Time
	SelectedFractal string // fractal UUID selected for this session (empty when prism is selected)
	SelectedPrism   string // prism UUID selected for this session (empty when fractal is selected)
	// MFAPending marks a session that passed the password but not the second
	// factor. It authorizes nothing until a code is verified.
	MFAPending bool
}

// APIKeyValidator interface for validating API keys (to avoid circular dependency)
type APIKeyValidator interface {
	ValidateAPIKey(ctx context.Context, key string) (*ValidatedAPIKey, error)
	UpdateLastUsed(ctx context.Context, keyID string) error
}

type AuthHandler struct {
	pg              *storage.PostgresClient
	ch              *storage.ClickHouseClient
	store           SessionStore
	fractalManager  *fractals.Manager
	apiKeyValidator APIKeyValidator
	secureCookies   bool
	loginLimiter    *loginRateLimiter
	// mfaLimiter is keyed by username, not IP: an attacker who already has the
	// password is otherwise free to walk the six digit space from new addresses.
	mfaLimiter      *loginRateLimiter
	keyLimiterMu    sync.Mutex
	keyLimiter      *ingest.RateLimiter
	keyLimiterRate  int
	systemFractalID string
	rbacResolver    *rbac.Resolver
	clientCADir     string // path to client CA dir for mTLS cert generation
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RegisterRequest struct {
	Username    string `json:"username"`
	DisplayName string `json:"display_name,omitempty"`
	Role        string `json:"role"` // "admin" or "user"
}

type AcceptInviteRequest struct {
	Token    string `json:"token"`
	Password string `json:"password"`
}

type ResetInviteRequest struct {
	Username string `json:"username"`
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	User    interface{} `json:"user,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func NewAuthHandlerWithAPIKeys(pg *storage.PostgresClient, ch *storage.ClickHouseClient, fractalManager *fractals.Manager, apiKeyValidator APIKeyValidator) *AuthHandler {
	handler := &AuthHandler{
		pg:              pg,
		ch:              ch,
		store:           newPgSessionStore(pg.DB()),
		fractalManager:  fractalManager,
		apiKeyValidator: apiKeyValidator,
		secureCookies:   os.Getenv("BIFRACT_SECURE_COOKIES") == "true",
		loginLimiter:    newLoginRateLimiter(),
		mfaLimiter:      newLoginRateLimiter(),
		rbacResolver:    rbac.NewResolver(pg),
		clientCADir:     os.Getenv("BIFRACT_CLIENT_CA_DIR"),
	}

	// Resolve system fractal ID for auth event logging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if sysFractal, err := fractalManager.GetFractalByName(ctx, "system"); err == nil {
		handler.systemFractalID = sysFractal.ID
	} else {
		log.Printf("[Auth] Warning: could not resolve system fractal: %v", err)
	}

	// Migrate the default admin hash if a pepper is now configured
	handler.migrateDefaultAdminHash()

	// Start cleanup goroutine for expired sessions
	go handler.cleanupExpiredSessions()

	return handler
}

// migrateDefaultAdminHash replaces the default admin password hash from
// init-postgres.sql with the deployment-generated hash. This handles two cases:
//   - BIFRACT_ADMIN_PASSWORD_HASH is set (K8s/production): use the provided hash
//     directly (already peppered by the setup wizard)
//   - Only BIFRACT_PASSWORD_PEPPER is set: re-hash the default password with the
//     pepper so login works
//
// This is a no-op if the admin password has already been changed from the default.
func (h *AuthHandler) migrateDefaultAdminHash() {
	adminHash := os.Getenv("BIFRACT_ADMIN_PASSWORD_HASH")
	pepper := os.Getenv("BIFRACT_PASSWORD_PEPPER")
	if adminHash == "" && pepper == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var storedHash string
	err := h.pg.DB().QueryRowContext(ctx,
		"SELECT password_hash FROM users WHERE username = 'admin'").Scan(&storedHash)
	if err != nil {
		return
	}

	// Only migrate if the stored hash still matches the default unpeppered password.
	if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(defaultAdminPassword)) != nil {
		return
	}

	var newHash string
	if adminHash != "" {
		// Use the pre-computed hash from the setup wizard
		newHash = adminHash
	} else {
		// Re-hash the default password with the pepper
		h, err := hashPassword(defaultAdminPassword)
		if err != nil {
			log.Printf("[Auth] Warning: failed to re-hash default admin password: %v", err)
			return
		}
		newHash = string(h)
	}

	_, err = h.pg.DB().ExecContext(ctx,
		"UPDATE users SET password_hash = $1, force_password_change = FALSE WHERE username = 'admin' AND password_hash = $2",
		newHash, storedHash)
	if err != nil {
		log.Printf("[Auth] Warning: failed to update default admin hash: %v", err)
		return
	}
	log.Printf("[Auth] Migrated default admin password hash for deployment")
}

func (h *AuthHandler) cleanupExpiredSessions() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		h.store.Cleanup()
	}
}

// logAuthEvent inserts an authentication event into the system fractal (fire-and-forget).
func (h *AuthHandler) logAuthEvent(event, user, ip, detail string) {
	if h.ch == nil || h.systemFractalID == "" {
		return
	}

	fields := map[string]string{
		"event":  event,
		"user":   user,
		"src_ip": ip,
	}
	if detail != "" {
		fields["detail"] = detail
	}

	now := time.Now()
	rawLog := fmt.Sprintf(`{"event":"%s","user":"%s","src_ip":"%s","detail":"%s"}`, event, user, ip, detail)
	entry := storage.LogEntry{
		Timestamp: now,
		RawLog:    rawLog,
		LogID:     storage.GenerateLogID(now, rawLog),
		Fields:    fields,
		FractalID: h.systemFractalID,
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.ch.InsertLogs(ctx, []storage.LogEntry{entry}); err != nil {
			log.Printf("[Auth] failed to log auth event: %v", err)
		}
	}()
}

// invalidateUserSessions removes all sessions for a given username
func (h *AuthHandler) invalidateUserSessions(username string) {
	h.store.DeleteByUsername(username)
}

func (h *AuthHandler) generateSessionID() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func (h *AuthHandler) createSession(username string) (string, error) {
	return h.createSessionWithMFA(username, false)
}

// createSessionWithMFA builds a session, optionally in the half-authenticated
// state that only the MFA endpoints accept. A pending session gets a short
// expiry so an abandoned login does not leave a usable ticket lying around.
func (h *AuthHandler) createSessionWithMFA(username string, mfaPending bool) (string, error) {
	sessionID, err := h.generateSessionID()
	if err != nil {
		return "", err
	}

	// Get default fractal ID for the session
	var selectedFractal string
	if h.fractalManager != nil {
		defaultFractal, err := h.fractalManager.GetDefaultFractal(context.Background())
		if err == nil {
			selectedFractal = defaultFractal.ID
		}
	}

	// For non-admin users, verify they have access to the selected fractal.
	// If not, find the first fractal they can access.
	if h.rbacResolver != nil && selectedFractal != "" {
		user, err := h.pg.GetUser(context.Background(), username)
		if err == nil && !user.IsAdmin {
			role, _ := h.rbacResolver.ResolveFractalRole(context.Background(), username, selectedFractal)
			if role == rbac.RoleNone {
				accessible, _ := h.rbacResolver.GetAccessibleFractals(context.Background(), username)
				if len(accessible) > 0 {
					selectedFractal = accessible[0].FractalID
				} else {
					selectedFractal = ""
				}
			}
		}
	}

	expiry := sessionDuration
	if mfaPending {
		expiry = mfaPendingDuration
	}

	session := &Session{
		Username:        username,
		CreatedAt:       time.Now(),
		ExpiresAt:       time.Now().Add(expiry),
		SelectedFractal: selectedFractal,
		MFAPending:      mfaPending,
	}

	h.store.Set(sessionID, session)

	return sessionID, nil
}

// CreateSessionForUser creates a session for the given username (exported for OIDC handler).
func (h *AuthHandler) CreateSessionForUser(username string) (string, error) {
	return h.createSession(username)
}

// IsSecureCookies returns whether secure cookie mode is enabled.
func (h *AuthHandler) IsSecureCookies() bool {
	return h.secureCookies
}

// RBACResolver returns the RBAC resolver for use by other handlers.
func (h *AuthHandler) RBACResolver() *rbac.Resolver {
	return h.rbacResolver
}

// LogAuthEvent logs an authentication event to the system fractal (exported for OIDC handler).
func (h *AuthHandler) LogAuthEvent(event, user, ip, detail string) {
	h.logAuthEvent(event, user, ip, detail)
}

// SessionUser resolves the browser session cookie to a user, or nil when the
// request carries no usable session. Unlike AuthMiddleware it never writes a
// response, so page routes can redirect to the login screen instead of handing
// a browser navigation a JSON 401.
func (h *AuthHandler) SessionUser(r *http.Request) *storage.User {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return nil
	}
	session, exists := h.getSession(cookie.Value)
	if !exists {
		return nil
	}
	if session.MFAPending {
		return nil
	}
	user, err := h.pg.GetUser(r.Context(), session.Username)
	if err != nil || user == nil || !user.Enabled || user.ForcePasswordChange {
		return nil
	}
	if mfaEnrollmentRequired(user) {
		return nil
	}
	return user
}

func (h *AuthHandler) getSession(sessionID string) (*Session, bool) {
	return h.store.Get(sessionID)
}

func (h *AuthHandler) deleteSession(sessionID string) {
	h.store.Delete(sessionID)
}

// clientIP extracts the real client IP, accounting for reverse proxy headers.
func clientIP(r *http.Request) string {
	// X-Forwarded-For may contain multiple IPs; the leftmost is the original client
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if parts := strings.SplitN(xff, ",", 2); len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	// Strip port from RemoteAddr
	host, _, err := strings.Cut(r.RemoteAddr, ":")
	if err {
		return host
	}
	return r.RemoteAddr
}

// HandleLogin handles user login
func (h *AuthHandler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Rate limiting: check if IP is blocked
	ip := clientIP(r)
	if h.loginLimiter.check(ip) {
		h.logAuthEvent("login_failed", "", ip, "rate limited")
		api.WriteError(w, http.StatusTooManyRequests, "Too many failed login attempts. Please try again later.")
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Get user from database
	user, err := h.pg.GetUser(r.Context(), req.Username)
	if err != nil {
		h.loginLimiter.recordFailure(ip)
		h.logAuthEvent("login_failed", req.Username, ip, "invalid username or password")
		api.WriteError(w, http.StatusUnauthorized, "Invalid username or password")
		return
	}

	// Block login for disabled accounts
	if !user.Enabled {
		h.logAuthEvent("login_failed", req.Username, ip, "account disabled")
		api.WriteError(w, http.StatusForbidden, "This account has been disabled. Please contact an administrator.")
		return
	}

	// Block password login for OIDC-provisioned users
	if user.AuthProvider == "oidc" {
		api.WriteError(w, http.StatusBadRequest, "This account uses SSO. Please sign in with SSO.")
		return
	}

	// Check if user has a pending invite (password not yet set)
	if user.PasswordHash == "!invite" {
		h.logAuthEvent("login_failed", req.Username, ip, "pending invite")
		api.WriteError(w, http.StatusBadRequest, "Account setup pending. Please use your invite link to set a password.")
		return
	}

	// Verify password (applies pepper if configured)
	if err := verifyPassword(user.PasswordHash, req.Password); err != nil {
		h.loginLimiter.recordFailure(ip)
		h.logAuthEvent("login_failed", req.Username, ip, "invalid username or password")
		api.WriteError(w, http.StatusUnauthorized, "Invalid username or password")
		return
	}

	// Password accepted. A user with an authenticator is not logged in yet: the
	// session stays half-authenticated until a code is verified.
	h.loginLimiter.reset(ip)
	mfaPending := user.TOTPEnrolled

	if mfaPending {
		h.logAuthEvent("mfa_challenge", user.Username, ip, "")
	} else {
		h.logAuthEvent("login_success", user.Username, ip, "")
		if err := h.pg.UpdateLastLogin(r.Context(), user.Username); err != nil {
			log.Printf("Failed to update last login for %s: %v", user.Username, err)
		}
	}

	sessionID, err := h.createSessionWithMFA(user.Username, mfaPending)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to create session")
		return
	}

	cookieMaxAge := int(sessionDuration.Seconds())
	if mfaPending {
		cookieMaxAge = int(mfaPendingDuration.Seconds())
	}

	// Set session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.secureCookies,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   cookieMaxAge,
	})

	w.Header().Set("Content-Type", "application/json")
	resp := Response{
		Success: true,
		Message: "Login successful",
		User: map[string]interface{}{
			"username":         user.Username,
			"display_name":     user.DisplayName,
			"gravatar_color":   user.GravatarColor,
			"gravatar_initial": user.GravatarInitial,
			"is_admin":         user.IsAdmin,
		},
	}
	if mfaPending {
		resp.Message = "Enter your verification code"
		resp.User.(map[string]interface{})["mfa_required"] = true
	} else if mfaEnrollmentRequired(user) {
		resp.User.(map[string]interface{})["mfa_enrollment_required"] = true
	}
	if user.ForcePasswordChange {
		resp.User.(map[string]interface{})["force_password_change"] = true
	}
	json.NewEncoder(w).Encode(resp)
}

// HandleLogout handles user logout
func (h *AuthHandler) HandleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	cookie, err := r.Cookie(sessionCookieName)
	if err == nil {
		h.deleteSession(cookie.Value)
	}

	// Clear cookie
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   h.secureCookies,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Logout successful",
	})
}

// HandleCurrentUser returns the current authenticated user
func (h *AuthHandler) HandleCurrentUser(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := ""
	if role, ok := r.Context().Value("fractal_role").(string); ok {
		fractalRole = role
	}
	prismRole := ""
	if role, ok := r.Context().Value("prism_role").(string); ok {
		prismRole = role
	}
	// Expose the server's session scope so the UI can render the correct
	// current fractal/prism on page load without a split-brain between
	// client localStorage and the server session.
	selectedFractal, _ := r.Context().Value("selected_fractal").(string)
	selectedPrism, _ := r.Context().Value("selected_prism").(string)

	userData := map[string]interface{}{
		"username":         user.Username,
		"display_name":     user.DisplayName,
		"gravatar_color":   user.GravatarColor,
		"gravatar_initial": user.GravatarInitial,
		"is_admin":         user.IsAdmin,
		"fractal_role":     fractalRole,
		"prism_role":       prismRole,
		"selected_fractal": selectedFractal,
		"selected_prism":   selectedPrism,
		"display_timezone": storage.SafeTimezone(user.DisplayTimezone),
	}
	if user.ForcePasswordChange {
		userData["force_password_change"] = true
	}
	if pending, _ := r.Context().Value("mfa_pending").(bool); pending {
		userData["mfa_required"] = true
	} else if mfaEnrollmentRequired(user) {
		userData["mfa_enrollment_required"] = true
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		User:    userData,
	})
}

// HandleRegister handles new user registration (admin only).
// Creates the user with a one-time invite token instead of a password.
// The admin receives the invite URL to share with the new user.
func (h *AuthHandler) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Username == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}

	if req.Role != "admin" && req.Role != "user" {
		req.Role = "user"
	}

	// Generate invite token
	plainToken, tokenHash, err := generateInviteToken()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to generate invite token")
		return
	}

	newUser := storage.User{
		Username:    req.Username,
		DisplayName: req.DisplayName,
		IsAdmin:     req.Role == "admin",
	}

	expiresAt := time.Now().Add(inviteExpiry)
	if err := h.pg.CreateUserWithInvite(r.Context(), newUser, tokenHash, expiresAt); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to create user")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "User created successfully",
		Data: map[string]interface{}{
			"invite_token": plainToken,
			"invite_url":   "/login.html?invite=" + plainToken,
			"expires_at":   expiresAt,
		},
	})
}

// HandleValidateInvite checks whether an invite token is valid (public, no auth).
func (h *AuthHandler) HandleValidateInvite(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		api.WriteError(w, http.StatusBadRequest, "Token is required")
		return
	}

	tokenHash := hashInviteToken(token)
	user, err := h.pg.GetUserByInviteToken(r.Context(), tokenHash)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Invalid or expired invite link")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data: map[string]interface{}{
			"username":     user.Username,
			"display_name": user.DisplayName,
		},
	})
}

// HandleAcceptInvite lets a new user set their password via an invite token (public, no auth).
func (h *AuthHandler) HandleAcceptInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req AcceptInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Token == "" || req.Password == "" {
		api.WriteError(w, http.StatusBadRequest, "Token and password are required")
		return
	}

	if len(req.Password) < minPasswordLength {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Password must be at least %d characters", minPasswordLength))
		return
	}

	tokenHash := hashInviteToken(req.Token)

	// Verify the token is valid before hashing the password
	if _, err := h.pg.GetUserByInviteToken(r.Context(), tokenHash); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid or expired invite link")
		return
	}

	passwordHash, err := hashPassword(req.Password)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to set password")
		return
	}

	if err := h.pg.AcceptInvite(r.Context(), tokenHash, passwordHash); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to set password")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Password set successfully. You can now sign in.",
	})
}

// HandleResetInvite regenerates the invite token for a pending user (admin only).
func (h *AuthHandler) HandleResetInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req ResetInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Username == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}

	plainToken, tokenHash, err := generateInviteToken()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to generate invite token")
		return
	}

	expiresAt := time.Now().Add(inviteExpiry)
	if err := h.pg.RegenerateInvite(r.Context(), req.Username, tokenHash, expiresAt); err != nil {
		api.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Invite regenerated successfully",
		Data: map[string]interface{}{
			"invite_token": plainToken,
			"invite_url":   "/login.html?invite=" + plainToken,
			"expires_at":   expiresAt,
		},
	})
}

// UpdatePreferencesRequest carries the caller display preferences to change.
type UpdatePreferencesRequest struct {
	DisplayTimezone *string `json:"display_timezone"`
}

// HandleUpdatePreferences stores per-user display preferences. Display timezone
// is the only one today; it changes how the UI renders timestamps and nothing
// about how they are stored or queried.
func (h *AuthHandler) HandleUpdatePreferences(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	var req UpdatePreferencesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.DisplayTimezone != nil {
		tz := strings.TrimSpace(*req.DisplayTimezone)
		if !storage.ValidTimezone(tz) {
			api.WriteError(w, http.StatusBadRequest, "Unknown timezone")
			return
		}
		if err := h.pg.SetUserDisplayTimezone(r.Context(), user.Username, tz); err != nil {
			log.Printf("Failed to save display timezone for %s: %v", user.Username, err)
			api.WriteError(w, http.StatusInternalServerError, "Failed to save preferences")
			return
		}
		user.DisplayTimezone = tz
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true})
}

// ChangePasswordRequest carries a self-service password change.
type ChangePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

// HandleChangePassword lets an authenticated user change their own password.
func (h *AuthHandler) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	user := r.Context().Value("user").(*storage.User)

	// Block OIDC users
	if user.AuthProvider == "oidc" {
		api.WriteError(w, http.StatusBadRequest, "Password changes are not available for SSO accounts")
		return
	}

	var req ChangePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.CurrentPassword == "" || req.NewPassword == "" {
		api.WriteError(w, http.StatusBadRequest, "Current password and new password are required")
		return
	}

	if len(req.NewPassword) < minPasswordLength {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("New password must be at least %d characters", minPasswordLength))
		return
	}

	// Verify current password
	if err := verifyPassword(user.PasswordHash, req.CurrentPassword); err != nil {
		ip := clientIP(r)
		h.logAuthEvent("password_change_failed", user.Username, ip, "invalid current password")
		api.WriteError(w, http.StatusUnauthorized, "Current password is incorrect")
		return
	}

	// Hash and store new password
	newHash, err := hashPassword(req.NewPassword)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update password")
		return
	}

	if err := h.pg.UpdatePasswordHash(r.Context(), user.Username, newHash); err != nil {
		log.Printf("[Auth] Failed to update password for %s: %v", user.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to update password")
		return
	}

	// Invalidate all existing sessions so compromised sessions cannot be reused.
	h.invalidateUserSessions(user.Username)

	// Create a fresh session for the current user so they stay logged in.
	newSessionID, err := h.createSession(user.Username)
	if err != nil {
		log.Printf("[Auth] Failed to create new session after password change for %s: %v", user.Username, err)
	}

	ip := clientIP(r)
	h.logAuthEvent("password_changed", user.Username, ip, "")

	// Set the new session cookie so the user is not logged out.
	if newSessionID != "" {
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    newSessionID,
			Path:     "/",
			HttpOnly: true,
			Secure:   h.secureCookies,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   int(sessionDuration.Seconds()),
		})
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Password changed successfully",
	})
}

// AdminResetPasswordRequest names the user whose password to reset.
type AdminResetPasswordRequest struct {
	Username string `json:"username"`
}

// HandleAdminResetPassword allows an admin to reset a non-SSO user's password
// by putting them back into the invite flow with a new invite token.
func (h *AuthHandler) HandleAdminResetPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	admin := r.Context().Value("user").(*storage.User)

	var req AdminResetPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Username == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}

	// Verify target user exists and is not an OIDC user
	targetUser, err := h.pg.GetUser(r.Context(), req.Username)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "User not found")
		return
	}

	if targetUser.AuthProvider == "oidc" {
		api.WriteError(w, http.StatusBadRequest, "Cannot reset password for SSO users")
		return
	}

	// Generate new invite token
	plainToken, tokenHash, err := generateInviteToken()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to generate invite token")
		return
	}

	expiresAt := time.Now().Add(inviteExpiry)
	if err := h.pg.ResetUserToInvite(r.Context(), req.Username, tokenHash, expiresAt); err != nil {
		log.Printf("[Auth] Failed to reset password for %s: %v", req.Username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to reset password")
		return
	}

	// Invalidate all sessions for the user
	h.invalidateUserSessions(req.Username)

	ip := clientIP(r)
	h.logAuthEvent("password_reset", req.Username, ip, fmt.Sprintf("reset by admin %s", admin.Username))

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Password reset successfully. Share the invite link with the user.",
		Data: map[string]interface{}{
			"invite_token": plainToken,
			"invite_url":   "/login.html?invite=" + plainToken,
			"expires_at":   expiresAt,
		},
	})
}

// ScopeHeader lets a single request override the session's fractal/prism scope.
// Value is "fractal:<id>" or "prism:<id>". The web UI sends it on every API call
// so that browser tabs sharing one session cookie can hold different scopes
// without silently repointing each other.
const ScopeHeader = "X-Bifract-Scope"

// ScopeInvalidHeader marks a rejection caused by ScopeHeader so the client can
// drop its stale context instead of surfacing a generic permission error.
const ScopeInvalidHeader = "X-Bifract-Scope-Invalid"

// scopeHeaderExempt lists paths that must stay reachable while a client still
// holds a scope it can no longer use, otherwise a stale tab can never recover.
func scopeHeaderExempt(path string) bool {
	return strings.HasPrefix(path, "/api/v1/auth/") ||
		path == "/api/v1/fractals" || path == "/api/v1/prisms" ||
		strings.HasSuffix(path, "/select")
}

// parseScopeHeader splits "fractal:<id>" / "prism:<id>". Exactly one of the
// returned ids is non-empty.
func parseScopeHeader(v string) (fractalID, prismID string, err error) {
	kind, id, ok := strings.Cut(strings.TrimSpace(v), ":")
	if !ok || id == "" || len(id) > 36 || !isScopeID(id) {
		return "", "", fmt.Errorf("invalid scope header")
	}
	switch kind {
	case "fractal":
		return id, "", nil
	case "prism":
		return "", id, nil
	default:
		return "", "", fmt.Errorf("unknown scope kind %q", kind)
	}
}

func isScopeID(s string) bool {
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// canAccessScope reports whether the user holds at least viewer on the requested
// scope. A deleted fractal/prism resolves to no role, so stale ids fail here too.
func (h *AuthHandler) canAccessScope(ctx context.Context, user *storage.User, fractalID, prismID string) bool {
	if user.IsAdmin {
		return true
	}
	if h.rbacResolver == nil {
		return false
	}
	if fractalID != "" {
		role, err := h.rbacResolver.ResolveFractalRole(ctx, user.Username, fractalID)
		return err == nil && rbac.HasAccess(user, role, rbac.RoleViewer)
	}
	role, err := h.rbacResolver.ResolvePrismRole(ctx, user.Username, prismID)
	return err == nil && rbac.HasAccess(user, role, rbac.RoleViewer)
}

func writeScopeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(ScopeInvalidHeader, "1")
	api.WriteError(w, status, msg)
}

// AuthMiddleware validates session or API key and loads user into context
func (h *AuthHandler) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try session authentication first (existing flow)
		if cookie, err := r.Cookie(sessionCookieName); err == nil {
			if session, exists := h.getSession(cookie.Value); exists {
				// Session auth successful - load user from database
				if user, err := h.pg.GetUser(r.Context(), session.Username); err == nil {
					// Lock out users whose account was disabled. Because the
					// user is loaded fresh from the DB each request, this takes
					// effect on the next request even with an active session.
					// Logout stays reachable so the client can clear its cookie.
					if !user.Enabled && r.URL.Path != "/api/v1/auth/logout" {
						api.WriteError(w, http.StatusForbidden, "Your account has been disabled.")
						return
					}

					ctx := context.WithValue(r.Context(), "user", user)
					ctx = context.WithValue(ctx, "auth_type", "session")
					ctx = context.WithValue(ctx, storage.AttributionUserKey, user.Username)
					ctx = context.WithValue(ctx, "mfa_pending", session.MFAPending)

					// A session that has passed the password but not the second
					// factor authorizes nothing beyond finishing that step. The
					// check is here, ahead of any scope or role resolution, so a
					// half-authenticated session cannot reach the rest of the API
					// through any route.
					if session.MFAPending && !mfaVerifyPaths[r.URL.Path] {
						api.WriteError(w, http.StatusForbidden, "Two-factor verification required")
						return
					}

					// Per-request scope. The session scope is the default, but a
					// request may override it with the X-Bifract-Scope header so
					// that browser tabs sharing one session cookie can hold
					// different fractals/prisms. The override is authorized here,
					// never trusted blindly.
					scopeFractal, scopePrism := session.SelectedFractal, session.SelectedPrism
					if hdr := r.Header.Get(ScopeHeader); hdr != "" && !scopeHeaderExempt(r.URL.Path) {
						hdrFractal, hdrPrism, err := parseScopeHeader(hdr)
						if err != nil {
							writeScopeError(w, http.StatusBadRequest, "Malformed scope header")
							return
						}
						if !h.canAccessScope(ctx, user, hdrFractal, hdrPrism) {
							writeScopeError(w, http.StatusForbidden, "No access to the requested fractal or prism")
							return
						}
						scopeFractal, scopePrism = hdrFractal, hdrPrism
					}
					ctx = context.WithValue(ctx, "selected_fractal", scopeFractal)
					ctx = context.WithValue(ctx, "selected_prism", scopePrism)

					// Resolve fractal/prism role for RBAC
					if user.IsAdmin {
						ctx = context.WithValue(ctx, "fractal_role", "admin")
						ctx = context.WithValue(ctx, "prism_role", "admin")
					} else if h.rbacResolver != nil {
						if scopeFractal != "" {
							if role, err := h.rbacResolver.ResolveFractalRole(ctx, user.Username, scopeFractal); err == nil {
								ctx = context.WithValue(ctx, "fractal_role", string(role))
							}
						}
						if scopePrism != "" {
							if role, err := h.rbacResolver.ResolvePrismRole(ctx, user.Username, scopePrism); err == nil {
								ctx = context.WithValue(ctx, "prism_role", string(role))
							}
						}
					}

					// Block users who must change their password from accessing
					// any endpoint except auth essentials (change password, get
					// current user so the UI can detect the flag, and logout).
					if user.ForcePasswordChange &&
						r.URL.Path != "/api/v1/auth/change-password" &&
						r.URL.Path != "/api/v1/auth/user" &&
						r.URL.Path != "/api/v1/auth/logout" {
						api.WriteError(w, http.StatusForbidden, "Password change required")
						return
					}

					// Same shape for a deployment that requires an authenticator:
					// a user who has not enrolled reaches only the enrollment
					// endpoints. A pending password change takes precedence and
					// has already narrowed the path set above; gating again here
					// would leave that user unable to reach the change endpoint
					// and stuck with no way forward.
					if !user.ForcePasswordChange && mfaEnrollmentRequired(user) && !mfaEnrollPaths[r.URL.Path] {
						api.WriteError(w, http.StatusForbidden, "Two-factor enrollment required")
						return
					}

					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
		}

		// Only try API key authentication if:
		// 1. There's no session cookie (or it's invalid)
		// 2. There are API key headers present
		// 3. API key validator is available
		if h.apiKeyValidator != nil {
			if apiKey := h.extractAPIKey(r); apiKey != "" {
				// Try to validate API key, but don't fail if there are database issues
				keyData, err := h.validateAPIKey(r.Context(), apiKey)
				if err == nil {
					if !h.apiKeyAllowed(keyData.KeyID) {
						w.Header().Set("Retry-After", "1")
						api.WriteError(w, http.StatusTooManyRequests, "API key rate limit exceeded")
						return
					}
					// Create user context for API key authentication
					// A key granted tenant administration is a tenant admin, the
					// same as a person: the instance-wide checks read IsAdmin.
					user := &storage.User{
						Username:    fmt.Sprintf("apikey_%s", keyData.KeyID),
						DisplayName: fmt.Sprintf("API Key: %s", keyData.Name),
						IsAdmin:     keyData.TenantAdmin,
					}
					if keyData.TenantAdmin {
						h.logAuthEvent("apikey_tenant_admin", user.Username, clientIP(r), r.Method+" "+r.URL.Path)
					}

					ctx := context.WithValue(r.Context(), "user", user)
					ctx = context.WithValue(ctx, "auth_type", "api_key")
					ctx = context.WithValue(ctx, storage.AttributionUserKey, keyData.CreatedBy)
					ctx = context.WithValue(ctx, "api_key", keyData)

					// The key's role applies to the scope it was issued for. An
					// instance-wide key was issued for none, so it names the
					// fractal or prism it wants per request with the scope
					// header, and is admin on whichever it names.
					switch {
					case keyData.TenantAdmin:
						if hdr := r.Header.Get(ScopeHeader); hdr != "" && !scopeHeaderExempt(r.URL.Path) {
							hdrFractal, hdrPrism, err := parseScopeHeader(hdr)
							if err != nil {
								writeScopeError(w, http.StatusBadRequest, "Malformed scope header")
								return
							}
							ctx = context.WithValue(ctx, "selected_fractal", hdrFractal)
							ctx = context.WithValue(ctx, "selected_prism", hdrPrism)
						}
						ctx = context.WithValue(ctx, "fractal_role", "admin")
						ctx = context.WithValue(ctx, "prism_role", "admin")
					case keyData.PrismID != "":
						ctx = context.WithValue(ctx, "selected_prism", keyData.PrismID)
						ctx = context.WithValue(ctx, "prism_role", keyData.Role)
					default:
						ctx = context.WithValue(ctx, "selected_fractal", keyData.FractalID)
						ctx = context.WithValue(ctx, "fractal_role", keyData.Role)
					}

					// Update usage stats (async) - but only if validation succeeds
					go func() {
						if err := h.apiKeyValidator.UpdateLastUsed(context.Background(), keyData.KeyID); err != nil {
							// Log error but don't fail the request
							log.Printf("Warning: Failed to update API key usage for %s: %v", keyData.KeyID, err)
						}
					}()

					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				// If API key was provided but validation failed, log only if it's not a simple "invalid key" error
				if err != nil && !strings.Contains(err.Error(), "invalid API key") && !strings.Contains(err.Error(), "no rows") {
					log.Printf("Warning: API key validation error (table may not exist): %v", err)
				}
			}
		}

		// Both session and API key authentication failed
		api.WriteError(w, http.StatusUnauthorized, "Unauthorized")
	})
}

// HandleListUsers lists all users (admin only)
const (
	defaultUserPageSize = 100
	maxUserPageSize     = 500
)

func (h *AuthHandler) HandleListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.pg.ListUsers(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to list users")
		return
	}

	// Convert to response format with role
	userList := make([]map[string]interface{}, len(users))
	for i, u := range users {
		role := "user"
		if u.IsAdmin {
			role = "admin"
		}
		userList[i] = map[string]interface{}{
			"username":         u.Username,
			"display_name":     u.DisplayName,
			"gravatar_color":   u.GravatarColor,
			"gravatar_initial": u.GravatarInitial,
			"role":             role,
			"created_at":       u.CreatedAt,
			"last_login":       u.LastLogin,
			"invite_pending":   u.InvitePending,
			"enabled":          u.Enabled,
			"auth_provider":    u.AuthProvider,
			"totp_enrolled":    u.TOTPEnrolled,
		}
	}

	limit, offset := api.PageParams(r, defaultUserPageSize, maxUserPageSize)
	window, page := api.Slice(userList, limit, offset)
	api.WritePage(w, window, page)
}

// HandleDeleteUser deletes a user (admin only)
func (h *AuthHandler) HandleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Check if user is admin
	user := r.Context().Value("user").(*storage.User)

	// Get username from URL path
	username := r.URL.Query().Get("username")
	if username == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}

	// Prevent self-deletion
	if username == user.Username {
		api.WriteError(w, http.StatusBadRequest, "Cannot delete your own account")
		return
	}

	err := h.pg.DeleteUser(r.Context(), username)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to delete user")
		return
	}

	// Invalidate all active sessions for the deleted user
	h.invalidateUserSessions(username)

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "User deleted successfully",
	})
}

// UpdateUserRequest carries the user fields an admin may change.
type UpdateUserRequest struct {
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
}

// HandleUpdateUser allows an admin to update a user's display name or role
func (h *AuthHandler) HandleUpdateUser(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	rawUsername := chi.URLParam(r, "username")
	if rawUsername == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}
	username, err := url.PathUnescape(rawUsername)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid username")
		return
	}

	var req UpdateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Role != "" && req.Role != "admin" && req.Role != "user" {
		api.WriteError(w, http.StatusBadRequest, "Role must be 'admin' or 'user'")
		return
	}

	// Prevent removing your own admin role
	if username == user.Username && req.Role == "user" {
		api.WriteError(w, http.StatusBadRequest, "Cannot remove your own admin role")
		return
	}

	if err := h.pg.UpdateUser(r.Context(), username, req.DisplayName, req.Role); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update user")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Message: "User updated successfully"})
}

// SetUserEnabledRequest enables or disables an account.
type SetUserEnabledRequest struct {
	Enabled bool `json:"enabled"`
}

// HandleSetUserEnabled allows an admin to enable or disable a user account.
// Disabling locks the user out of new logins and terminates active sessions.
func (h *AuthHandler) HandleSetUserEnabled(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	user := r.Context().Value("user").(*storage.User)

	rawUsername := chi.URLParam(r, "username")
	if rawUsername == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}
	username, err := url.PathUnescape(rawUsername)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid username")
		return
	}

	var req SetUserEnabledRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Prevent locking yourself out.
	if username == user.Username && !req.Enabled {
		api.WriteError(w, http.StatusBadRequest, "Cannot disable your own account")
		return
	}

	if err := h.pg.SetUserEnabled(r.Context(), username, req.Enabled); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update user")
		return
	}

	// Terminate active sessions so a disabled user is logged out immediately.
	if !req.Enabled {
		h.invalidateUserSessions(username)
	}

	msg := "User enabled successfully"
	if !req.Enabled {
		msg = "User disabled successfully"
	}
	api.WriteJSON(w, http.StatusOK, Response{Success: true, Message: msg})
}

// ============================
// Fractal Selection Methods
// ============================

// SetSelectedFractalInSession updates the selected fractal for a user's session, clearing any selected prism.
func (h *AuthHandler) SetSelectedFractalInSession(sessionID, fractalID string) error {
	return h.store.UpdateFractal(sessionID, fractalID)
}

// SetSelectedFractalInSessionFromRequest updates the selected fractal using the session cookie from the request.
func (h *AuthHandler) SetSelectedFractalInSessionFromRequest(r *http.Request, fractalID string) error {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return fmt.Errorf("no session found")
	}

	return h.SetSelectedFractalInSession(cookie.Value, fractalID)
}

// SetSelectedPrismInSession updates the selected prism for a user's session, clearing any selected fractal.
func (h *AuthHandler) SetSelectedPrismInSession(sessionID, prismID string) error {
	return h.store.UpdatePrism(sessionID, prismID)
}

// SetSelectedPrismInSessionFromRequest updates the selected prism using the session cookie from the request.
func (h *AuthHandler) SetSelectedPrismInSessionFromRequest(r *http.Request, prismID string) error {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return fmt.Errorf("no session found")
	}

	return h.SetSelectedPrismInSession(cookie.Value, prismID)
}

// ============================
// API Key Authentication Methods
// ============================

// ValidatedAPIKey represents an API key validated for authentication
type ValidatedAPIKey struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	KeyID       string `json:"key_id"`
	FractalID   string `json:"fractal_id,omitempty"`
	FractalName string `json:"fractal_name,omitempty"`
	PrismID     string `json:"prism_id,omitempty"`
	PrismName   string `json:"prism_name,omitempty"`
	CreatedBy   string `json:"created_by"`
	// Role is the key's RBAC role on its scope; TenantAdmin grants
	// instance-wide administration.
	Role        string `json:"role"`
	TenantAdmin bool   `json:"tenant_admin"`
}

// apiKeyLimiter throttles each key separately, so one runaway integration
// cannot crowd out the rest. The bucket is keyed by key id, never by IP: a key
// is the thing being limited, and it may be used from many hosts.
func (h *AuthHandler) apiKeyAllowed(keyID string) bool {
	limit := settings.Get().APIKeyRateLimit
	if limit <= 0 {
		return true
	}

	h.keyLimiterMu.Lock()
	// The limit is live-editable from the admin settings page, so re-rate the
	// limiter when it changes rather than caching the rate it started with.
	if h.keyLimiter == nil {
		h.keyLimiter = ingest.NewRateLimiter(float64(limit), limit)
	} else if h.keyLimiterRate != limit {
		h.keyLimiter.SetRate(float64(limit), limit)
	}
	h.keyLimiterRate = limit
	limiter := h.keyLimiter
	h.keyLimiterMu.Unlock()

	return limiter.Allow(keyID)
}

// extractAPIKey reads the API key from request headers only: a key in a query
// string would end up in access logs, proxies, and browser history.
func (h *AuthHandler) extractAPIKey(r *http.Request) string {
	// Check Authorization header: "Bearer bifract_..."
	if auth := r.Header.Get("Authorization"); auth != "" {
		if parts := strings.SplitN(auth, " ", 2); len(parts) == 2 && parts[0] == "Bearer" {
			if strings.HasPrefix(parts[1], "bifract_") {
				return parts[1]
			}
		}
	}

	// Check X-API-Key header
	if key := r.Header.Get("X-API-Key"); key != "" && strings.HasPrefix(key, "bifract_") {
		return key
	}

	return ""
}

// validateAPIKey validates an API key using the injected validator
func (h *AuthHandler) validateAPIKey(ctx context.Context, apiKey string) (*ValidatedAPIKey, error) {
	if h.apiKeyValidator == nil {
		return nil, fmt.Errorf("API key validation not available")
	}

	return h.apiKeyValidator.ValidateAPIKey(ctx, apiKey)
}

// HandleMTLSStatus returns whether mTLS client cert generation is available.
func (h *AuthHandler) HandleMTLSStatus(w http.ResponseWriter, r *http.Request) {
	enabled := false
	if h.clientCADir != "" {
		caPath := filepath.Join(h.clientCADir, "ca.pem")
		keyPath := filepath.Join(h.clientCADir, "ca-key.pem")
		if _, err := os.Stat(caPath); err == nil {
			if _, err := os.Stat(keyPath); err == nil {
				enabled = true
			}
		}
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: map[string]bool{"mtls_enabled": enabled}})
}

// GenerateClientCertRequest carries the passphrase protecting the PKCS#12 bundle.
type GenerateClientCertRequest struct {
	Password string `json:"password"`
}

// HandleGenerateClientCert generates a PKCS#12 client certificate for a user
// and streams it as a download. Admin only.
func (h *AuthHandler) HandleGenerateClientCert(w http.ResponseWriter, r *http.Request) {
	rawUsername := chi.URLParam(r, "username")
	if rawUsername == "" {
		api.WriteError(w, http.StatusBadRequest, "Username is required")
		return
	}
	username, err := url.PathUnescape(rawUsername)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid username")
		return
	}

	// Verify the target user exists
	_, err = h.pg.GetUser(r.Context(), username)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "User not found")
		return
	}

	if h.clientCADir == "" {
		api.WriteError(w, http.StatusBadRequest, "mTLS is not configured")
		return
	}

	caCertPEM, err := os.ReadFile(filepath.Join(h.clientCADir, "ca.pem"))
	if err != nil {
		log.Printf("[Auth] Failed to read CA cert: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "mTLS CA not available")
		return
	}
	caKeyPEM, err := os.ReadFile(filepath.Join(h.clientCADir, "ca-key.pem"))
	if err != nil {
		log.Printf("[Auth] Failed to read CA key: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "mTLS CA not available")
		return
	}

	// Parse password from request body
	var req GenerateClientCertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Password == "" {
		api.WriteError(w, http.StatusBadRequest, "Password is required to protect the certificate")
		return
	}

	p12Data, err := setup.GenerateClientCertBytes(caCertPEM, caKeyPEM, username, req.Password)
	if err != nil {
		log.Printf("[Auth] Failed to generate client cert for %s: %v", username, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to generate certificate")
		return
	}

	w.Header().Set("Content-Type", "application/x-pkcs12")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.p12"`, username))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(p12Data)))
	w.WriteHeader(http.StatusOK)
	w.Write(p12Data)
}
