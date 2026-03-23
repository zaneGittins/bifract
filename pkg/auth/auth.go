package auth

import (
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
	"bifract/pkg/rbac"
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
	loginMaxFailures    = 5               // failures before blocking
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
)

type Session struct {
	Username        string
	CreatedAt       time.Time
	ExpiresAt       time.Time
	SelectedFractal string // fractal UUID selected for this session (empty when prism is selected)
	SelectedPrism   string // prism UUID selected for this session (empty when fractal is selected)
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

	session := &Session{
		Username:        username,
		CreatedAt:       time.Now(),
		ExpiresAt:       time.Now().Add(sessionDuration),
		SelectedFractal: selectedFractal,
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
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Rate limiting: check if IP is blocked
	ip := clientIP(r)
	if h.loginLimiter.check(ip) {
		h.logAuthEvent("login_failed", "", ip, "rate limited")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Too many failed login attempts. Please try again later.",
		})
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	// Get user from database
	user, err := h.pg.GetUser(r.Context(), req.Username)
	if err != nil {
		h.loginLimiter.recordFailure(ip)
		h.logAuthEvent("login_failed", req.Username, ip, "invalid username or password")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid username or password",
		})
		return
	}

	// Block password login for OIDC-provisioned users
	if user.AuthProvider == "oidc" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "This account uses SSO. Please sign in with SSO.",
		})
		return
	}

	// Check if user has a pending invite (password not yet set)
	if user.PasswordHash == "!invite" {
		h.logAuthEvent("login_failed", req.Username, ip, "pending invite")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Account setup pending. Please use your invite link to set a password.",
		})
		return
	}

	// Verify password (applies pepper if configured)
	if err := verifyPassword(user.PasswordHash, req.Password); err != nil {
		h.loginLimiter.recordFailure(ip)
		h.logAuthEvent("login_failed", req.Username, ip, "invalid username or password")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid username or password",
		})
		return
	}

	// Successful login - clear rate limit state
	h.loginLimiter.reset(ip)
	h.logAuthEvent("login_success", user.Username, ip, "")

	// Update last login
	if err := h.pg.UpdateLastLogin(r.Context(), user.Username); err != nil {
		log.Printf("Failed to update last login for %s: %v", user.Username, err)
	}

	// Create session
	sessionID, err := h.createSession(user.Username)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to create session",
		})
		return
	}

	// Set session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.secureCookies,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(sessionDuration.Seconds()),
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
	if user.ForcePasswordChange {
		resp.User.(map[string]interface{})["force_password_change"] = true
	}
	json.NewEncoder(w).Encode(resp)
}

// HandleLogout handles user logout
func (h *AuthHandler) HandleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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

	userData := map[string]interface{}{
		"username":         user.Username,
		"display_name":     user.DisplayName,
		"gravatar_color":   user.GravatarColor,
		"gravatar_initial": user.GravatarInitial,
		"is_admin":         user.IsAdmin,
		"fractal_role":     fractalRole,
		"prism_role":       prismRole,
	}
	if user.ForcePasswordChange {
		userData["force_password_change"] = true
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		User:    userData,
	})
}

// HandleRegister handles new user registration (admin only).
// Creates the user with a one-time invite token instead of a password.
// The admin receives the invite URL to share with the new user.
func (h *AuthHandler) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if user is admin
	user := r.Context().Value("user").(*storage.User)
	if !user.IsAdmin {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Only administrators can register new users",
		})
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	if req.Username == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Username is required",
		})
		return
	}

	if req.Role != "admin" && req.Role != "user" {
		req.Role = "user"
	}

	// Generate invite token
	plainToken, tokenHash, err := generateInviteToken()
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to generate invite token",
		})
		return
	}

	newUser := storage.User{
		Username:    req.Username,
		DisplayName: req.DisplayName,
		IsAdmin:     req.Role == "admin",
	}

	expiresAt := time.Now().Add(inviteExpiry)
	if err := h.pg.CreateUserWithInvite(r.Context(), newUser, tokenHash, expiresAt); err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to create user",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Token is required"})
		return
	}

	tokenHash := hashInviteToken(token)
	user, err := h.pg.GetUserByInviteToken(r.Context(), tokenHash)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid or expired invite link"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AcceptInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if req.Token == "" || req.Password == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Token and password are required"})
		return
	}

	if len(req.Password) < minPasswordLength {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   fmt.Sprintf("Password must be at least %d characters", minPasswordLength),
		})
		return
	}

	tokenHash := hashInviteToken(req.Token)

	// Verify the token is valid before hashing the password
	if _, err := h.pg.GetUserByInviteToken(r.Context(), tokenHash); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid or expired invite link"})
		return
	}

	passwordHash, err := hashPassword(req.Password)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to set password"})
		return
	}

	if err := h.pg.AcceptInvite(r.Context(), tokenHash, passwordHash); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to set password"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Password set successfully. You can now sign in.",
	})
}

// HandleResetInvite regenerates the invite token for a pending user (admin only).
func (h *AuthHandler) HandleResetInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	user := r.Context().Value("user").(*storage.User)
	if !user.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Only administrators can reset invites"})
		return
	}

	var req ResetInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if req.Username == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Username is required"})
		return
	}

	plainToken, tokenHash, err := generateInviteToken()
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to generate invite token"})
		return
	}

	expiresAt := time.Now().Add(inviteExpiry)
	if err := h.pg.RegenerateInvite(r.Context(), req.Username, tokenHash, expiresAt); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Invite regenerated successfully",
		Data: map[string]interface{}{
			"invite_token": plainToken,
			"invite_url":   "/login.html?invite=" + plainToken,
			"expires_at":   expiresAt,
		},
	})
}

// HandleChangePassword lets an authenticated user change their own password.
func (h *AuthHandler) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	user := r.Context().Value("user").(*storage.User)

	// Block OIDC users
	if user.AuthProvider == "oidc" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Password changes are not available for SSO accounts",
		})
		return
	}

	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if req.CurrentPassword == "" || req.NewPassword == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Current password and new password are required"})
		return
	}

	if len(req.NewPassword) < minPasswordLength {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   fmt.Sprintf("New password must be at least %d characters", minPasswordLength),
		})
		return
	}

	// Verify current password
	if err := verifyPassword(user.PasswordHash, req.CurrentPassword); err != nil {
		ip := clientIP(r)
		h.logAuthEvent("password_change_failed", user.Username, ip, "invalid current password")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Current password is incorrect"})
		return
	}

	// Hash and store new password
	newHash, err := hashPassword(req.NewPassword)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to update password"})
		return
	}

	if err := h.pg.UpdatePasswordHash(r.Context(), user.Username, newHash); err != nil {
		log.Printf("[Auth] Failed to update password for %s: %v", user.Username, err)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to update password"})
		return
	}

	ip := clientIP(r)
	h.logAuthEvent("password_changed", user.Username, ip, "")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Password changed successfully",
	})
}

// HandleAdminResetPassword allows an admin to reset a non-SSO user's password
// by putting them back into the invite flow with a new invite token.
func (h *AuthHandler) HandleAdminResetPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	admin := r.Context().Value("user").(*storage.User)
	if !admin.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Only administrators can reset passwords"})
		return
	}

	var req struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if req.Username == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Username is required"})
		return
	}

	// Verify target user exists and is not an OIDC user
	targetUser, err := h.pg.GetUser(r.Context(), req.Username)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "User not found"})
		return
	}

	if targetUser.AuthProvider == "oidc" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Cannot reset password for SSO users"})
		return
	}

	// Generate new invite token
	plainToken, tokenHash, err := generateInviteToken()
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to generate invite token"})
		return
	}

	expiresAt := time.Now().Add(inviteExpiry)
	if err := h.pg.ResetUserToInvite(r.Context(), req.Username, tokenHash, expiresAt); err != nil {
		log.Printf("[Auth] Failed to reset password for %s: %v", req.Username, err)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to reset password"})
		return
	}

	// Invalidate all sessions for the user
	h.invalidateUserSessions(req.Username)

	ip := clientIP(r)
	h.logAuthEvent("password_reset", req.Username, ip, fmt.Sprintf("reset by admin %s", admin.Username))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Password reset successfully. Share the invite link with the user.",
		Data: map[string]interface{}{
			"invite_token": plainToken,
			"invite_url":   "/login.html?invite=" + plainToken,
			"expires_at":   expiresAt,
		},
	})
}

// AuthMiddleware validates session or API key and loads user into context
func (h *AuthHandler) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try session authentication first (existing flow)
		if cookie, err := r.Cookie(sessionCookieName); err == nil {
			if session, exists := h.getSession(cookie.Value); exists {
				// Session auth successful - load user from database
				if user, err := h.pg.GetUser(r.Context(), session.Username); err == nil {
					ctx := context.WithValue(r.Context(), "user", user)
					ctx = context.WithValue(ctx, "auth_type", "session")
					ctx = context.WithValue(ctx, "selected_fractal", session.SelectedFractal)
					ctx = context.WithValue(ctx, "selected_prism", session.SelectedPrism)

					// Resolve fractal/prism role for RBAC
					if user.IsAdmin {
						ctx = context.WithValue(ctx, "fractal_role", "admin")
						ctx = context.WithValue(ctx, "prism_role", "admin")
					} else if h.rbacResolver != nil {
						if session.SelectedFractal != "" {
							if role, err := h.rbacResolver.ResolveFractalRole(ctx, user.Username, session.SelectedFractal); err == nil {
								ctx = context.WithValue(ctx, "fractal_role", string(role))
							}
						}
						if session.SelectedPrism != "" {
							if role, err := h.rbacResolver.ResolvePrismRole(ctx, user.Username, session.SelectedPrism); err == nil {
								ctx = context.WithValue(ctx, "prism_role", string(role))
							}
						}
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
					// Create user context for API key authentication
					user := &storage.User{
						Username:    fmt.Sprintf("apikey_%s", keyData.KeyID),
						DisplayName: fmt.Sprintf("API Key: %s", keyData.Name),
						IsAdmin:     false, // API keys are not admin by default
					}

					ctx := context.WithValue(r.Context(), "user", user)
					ctx = context.WithValue(ctx, "auth_type", "api_key")
					ctx = context.WithValue(ctx, "api_key", keyData)
					ctx = context.WithValue(ctx, "selected_fractal", keyData.FractalID)
					ctx = context.WithValue(ctx, "api_key_permissions", keyData.Permissions)

					// Resolve an RBAC role from the API key's permission map.
					// Individual handlers still check the permission map for fine-grained enforcement.
					ctx = context.WithValue(ctx, "fractal_role", resolveAPIKeyRole(keyData.Permissions))

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
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

// resolveAPIKeyRole maps API key permissions to the minimum RBAC role
// that covers the granted capabilities. Individual handlers still enforce
// fine-grained permission checks on top of this.
func resolveAPIKeyRole(perms map[string]interface{}) string {
	if perms == nil {
		return ""
	}
	// Any write permission requires analyst level
	for _, key := range []string{"alert_manage", "comment", "notebook", "dashboard"} {
		if v, ok := perms[key].(bool); ok && v {
			return "analyst"
		}
	}
	// query-only maps to viewer
	if v, ok := perms["query"].(bool); ok && v {
		return "viewer"
	}
	return ""
}

// RequireAPIKeyPermission returns middleware that blocks API key requests
// lacking the specified permission. Session-authenticated requests pass through.
func RequireAPIKeyPermission(permission string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
				perms, _ := r.Context().Value("api_key_permissions").(map[string]interface{})
				if allowed, ok := perms[permission].(bool); !ok || !allowed {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprintf(w, `{"success":false,"error":"API key does not have %s permission"}`, permission)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// HandleListUsers lists all users (admin only)
func (h *AuthHandler) HandleListUsers(w http.ResponseWriter, r *http.Request) {
	// Check if user is admin
	user := r.Context().Value("user").(*storage.User)
	if !user.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Only administrators can list users",
		})
		return
	}

	users, err := h.pg.ListUsers(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to list users",
		})
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
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Data:    userList,
	})
}

// HandleDeleteUser deletes a user (admin only)
func (h *AuthHandler) HandleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if user is admin
	user := r.Context().Value("user").(*storage.User)
	if !user.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Only administrators can delete users",
		})
		return
	}

	// Get username from URL path
	username := r.URL.Query().Get("username")
	if username == "" {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Username is required",
		})
		return
	}

	// Prevent self-deletion
	if username == user.Username {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Cannot delete your own account",
		})
		return
	}

	err := h.pg.DeleteUser(r.Context(), username)
	if err != nil {
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to delete user",
		})
		return
	}

	// Invalidate all active sessions for the deleted user
	h.invalidateUserSessions(username)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "User deleted successfully",
	})
}

// HandleUpdateUser allows an admin to update a user's display name or role
func (h *AuthHandler) HandleUpdateUser(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	if !user.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Only administrators can edit users",
		})
		return
	}

	rawUsername := chi.URLParam(r, "username")
	if rawUsername == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Username is required"})
		return
	}
	username, err := url.PathUnescape(rawUsername)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid username"})
		return
	}

	var req struct {
		DisplayName string `json:"display_name"`
		Role        string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if req.Role != "" && req.Role != "admin" && req.Role != "user" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Role must be 'admin' or 'user'"})
		return
	}

	// Prevent removing your own admin role
	if username == user.Username && req.Role == "user" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Cannot remove your own admin role"})
		return
	}

	if err := h.pg.UpdateUser(r.Context(), username, req.DisplayName, req.Role); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to update user"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "User updated successfully"})
}

// ============================
// Fractal Selection Methods
// ============================

// GetSelectedFractalFromSession retrieves the selected fractal ID from a user's session
func (h *AuthHandler) GetSelectedFractalFromSession(r *http.Request) (string, error) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return "", fmt.Errorf("no session found")
	}

	session, exists := h.getSession(cookie.Value)
	if !exists {
		return "", fmt.Errorf("invalid session")
	}

	// If no selected fractal, return default fractal
	if session.SelectedFractal == "" && h.fractalManager != nil {
		defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
		if err != nil {
			return "", fmt.Errorf("failed to get default fractal: %w", err)
		}
		return defaultFractal.ID, nil
	}

	return session.SelectedFractal, nil
}

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
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	KeyID       string            `json:"key_id"`
	FractalID   string            `json:"fractal_id"`
	FractalName string            `json:"fractal_name"`
	CreatedBy   string            `json:"created_by"`
	Permissions map[string]interface{} `json:"permissions"`
}

// extractAPIKey extracts API key from request headers or query parameters
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
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Admin access required"})
		return
	}

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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: map[string]bool{"mtls_enabled": enabled}})
}

// HandleGenerateClientCert generates a PKCS#12 client certificate for a user
// and streams it as a download. Admin only.
func (h *AuthHandler) HandleGenerateClientCert(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Admin access required"})
		return
	}

	rawUsername := chi.URLParam(r, "username")
	if rawUsername == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Username is required"})
		return
	}
	username, err := url.PathUnescape(rawUsername)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid username"})
		return
	}

	// Verify the target user exists
	_, err = h.pg.GetUser(r.Context(), username)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "User not found"})
		return
	}

	if h.clientCADir == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "mTLS is not configured"})
		return
	}

	caCertPEM, err := os.ReadFile(filepath.Join(h.clientCADir, "ca.pem"))
	if err != nil {
		log.Printf("[Auth] Failed to read CA cert: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "mTLS CA not available"})
		return
	}
	caKeyPEM, err := os.ReadFile(filepath.Join(h.clientCADir, "ca-key.pem"))
	if err != nil {
		log.Printf("[Auth] Failed to read CA key: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "mTLS CA not available"})
		return
	}

	// Parse password from request body
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Password == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Password is required to protect the certificate"})
		return
	}

	p12Data, err := setup.GenerateClientCertBytes(caCertPEM, caKeyPEM, username, req.Password)
	if err != nil {
		log.Printf("[Auth] Failed to generate client cert for %s: %v", username, err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to generate certificate"})
		return
	}

	w.Header().Set("Content-Type", "application/x-pkcs12")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.p12"`, username))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(p12Data)))
	w.WriteHeader(http.StatusOK)
	w.Write(p12Data)
}

