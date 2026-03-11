package oidc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
	"bifract/pkg/storage"
)

// Config holds OIDC provider configuration loaded from environment variables.
type Config struct {
	IssuerURL      string
	ClientID       string
	ClientSecret   string
	RedirectURL    string
	Scopes         []string
	DefaultRole    string
	AllowedDomains []string
	ButtonText     string
}

// LoadConfigFromEnv reads OIDC configuration from environment variables.
// Returns nil if OIDC is not configured (issuer or client ID missing).
func LoadConfigFromEnv() *Config {
	issuer := os.Getenv("BIFRACT_OIDC_ISSUER_URL")
	clientID := os.Getenv("BIFRACT_OIDC_CLIENT_ID")
	if issuer == "" || clientID == "" {
		return nil
	}

	scopes := []string{"openid", "profile", "email"}
	if s := os.Getenv("BIFRACT_OIDC_SCOPES"); s != "" {
		scopes = strings.Split(s, ",")
		for i := range scopes {
			scopes[i] = strings.TrimSpace(scopes[i])
		}
	}

	var domains []string
	if d := os.Getenv("BIFRACT_OIDC_ALLOWED_DOMAINS"); d != "" {
		for _, domain := range strings.Split(d, ",") {
			domain = strings.TrimSpace(domain)
			if domain != "" {
				domains = append(domains, strings.ToLower(domain))
			}
		}
	}

	redirectURL := os.Getenv("BIFRACT_OIDC_REDIRECT_URL")
	if redirectURL == "" {
		base := os.Getenv("BIFRACT_BASE_URL")
		if base == "" {
			if domain := os.Getenv("BIFRACT_DOMAIN"); domain != "" {
				base = "https://" + domain
			} else {
				base = "http://localhost:8080"
			}
		}
		redirectURL = strings.TrimRight(base, "/") + "/api/v1/auth/oidc/callback"
	}

	defaultRole := os.Getenv("BIFRACT_OIDC_DEFAULT_ROLE")
	if defaultRole == "" {
		defaultRole = "user"
	}

	buttonText := os.Getenv("BIFRACT_OIDC_BUTTON_TEXT")
	if buttonText == "" {
		buttonText = "Sign in with SSO"
	}

	return &Config{
		IssuerURL:      issuer,
		ClientID:       clientID,
		ClientSecret:   os.Getenv("BIFRACT_OIDC_CLIENT_SECRET"),
		RedirectURL:    redirectURL,
		Scopes:         scopes,
		DefaultRole:    defaultRole,
		AllowedDomains: domains,
		ButtonText:     buttonText,
	}
}

type stateEntry struct {
	nonce     string
	expiresAt time.Time
}

// Handler manages OIDC authentication flow.
type Handler struct {
	config        *Config
	provider      *gooidc.Provider
	oauth2Config  oauth2.Config
	verifier      *gooidc.IDTokenVerifier
	pg            *storage.PostgresClient
	createSess    func(username string) (string, error)
	logAuthEvent  func(event, user, ip, detail string)
	secureCookies bool

	states   map[string]stateEntry
	statesMu sync.Mutex
}

// NewHandler initializes the OIDC handler by performing provider discovery.
func NewHandler(cfg *Config, pg *storage.PostgresClient, createSession func(string) (string, error), logEvent func(event, user, ip, detail string), secureCookies bool) (*Handler, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	provider, err := gooidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("OIDC provider discovery failed: %w", err)
	}

	oauth2Config := oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  cfg.RedirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       cfg.Scopes,
	}

	verifier := provider.Verifier(&gooidc.Config{ClientID: cfg.ClientID})

	h := &Handler{
		config:        cfg,
		provider:      provider,
		oauth2Config:  oauth2Config,
		verifier:      verifier,
		pg:            pg,
		createSess:    createSession,
		logAuthEvent:  logEvent,
		secureCookies: secureCookies,
		states:        make(map[string]stateEntry),
	}

	go h.cleanupStates()

	return h, nil
}

func (h *Handler) generateState() (state, nonce string, err error) {
	stateBytes := make([]byte, 32)
	if _, err = rand.Read(stateBytes); err != nil {
		return "", "", err
	}
	state = hex.EncodeToString(stateBytes)

	nonceBytes := make([]byte, 32)
	if _, err = rand.Read(nonceBytes); err != nil {
		return "", "", err
	}
	nonce = hex.EncodeToString(nonceBytes)

	h.statesMu.Lock()
	h.states[state] = stateEntry{
		nonce:     nonce,
		expiresAt: time.Now().Add(10 * time.Minute),
	}
	h.statesMu.Unlock()

	return state, nonce, nil
}

func (h *Handler) consumeState(state string) (nonce string, valid bool) {
	h.statesMu.Lock()
	defer h.statesMu.Unlock()

	entry, ok := h.states[state]
	if !ok || time.Now().After(entry.expiresAt) {
		delete(h.states, state)
		return "", false
	}
	delete(h.states, state)
	return entry.nonce, true
}

func (h *Handler) cleanupStates() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		h.statesMu.Lock()
		now := time.Now()
		for k, v := range h.states {
			if now.After(v.expiresAt) {
				delete(h.states, k)
			}
		}
		h.statesMu.Unlock()
	}
}

// HandleConfig returns OIDC availability for the login page.
func (h *Handler) HandleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled":     true,
		"button_text": h.config.ButtonText,
	})
}

// HandleLogin initiates the OIDC authorization code flow.
func (h *Handler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	state, nonce, err := h.generateState()
	if err != nil {
		log.Printf("[OIDC] Failed to generate state: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	url := h.oauth2Config.AuthCodeURL(state, gooidc.Nonce(nonce))
	http.Redirect(w, r, url, http.StatusFound)
}

// HandleCallback processes the OIDC provider callback after user authentication.
func (h *Handler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	// Validate state
	state := r.URL.Query().Get("state")
	nonce, valid := h.consumeState(state)
	if !valid {
		log.Printf("[OIDC] Invalid or expired state parameter")
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	// Check for provider error
	if errParam := r.URL.Query().Get("error"); errParam != "" {
		desc := r.URL.Query().Get("error_description")
		log.Printf("[OIDC] Provider error: %s - %s", errParam, desc)
		http.Redirect(w, r, "/login.html?error=oidc_denied", http.StatusFound)
		return
	}

	// Exchange code for tokens
	code := r.URL.Query().Get("code")
	if code == "" {
		log.Printf("[OIDC] Missing authorization code")
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	token, err := h.oauth2Config.Exchange(ctx, code)
	if err != nil {
		log.Printf("[OIDC] Token exchange failed: %v", err)
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	// Extract and verify ID token
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		log.Printf("[OIDC] No id_token in token response")
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	idToken, err := h.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		log.Printf("[OIDC] ID token verification failed: %v", err)
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	// Verify nonce
	if idToken.Nonce != nonce {
		log.Printf("[OIDC] Nonce mismatch")
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	// Extract claims
	var claims struct {
		Sub              string `json:"sub"`
		Email            string `json:"email"`
		EmailVerified    bool   `json:"email_verified"`
		Name             string `json:"name"`
		PreferredUsername string `json:"preferred_username"`
	}
	if err := idToken.Claims(&claims); err != nil {
		log.Printf("[OIDC] Failed to extract claims: %v", err)
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	// Domain restriction
	ip := r.RemoteAddr
	if len(h.config.AllowedDomains) > 0 {
		parts := strings.SplitN(claims.Email, "@", 2)
		if len(parts) != 2 {
			log.Printf("[OIDC] Email missing or invalid: %s", claims.Email)
			h.logAuthEvent("oidc_login_failed", claims.Sub, ip, "email missing or invalid")
			http.Redirect(w, r, "/login.html?error=oidc_domain", http.StatusFound)
			return
		}
		domain := strings.ToLower(parts[1])
		allowed := false
		for _, d := range h.config.AllowedDomains {
			if d == domain {
				allowed = true
				break
			}
		}
		if !allowed {
			log.Printf("[OIDC] Domain %s not in allowed list", domain)
			h.logAuthEvent("oidc_login_failed", claims.Email, ip, "domain not allowed: "+domain)
			http.Redirect(w, r, "/login.html?error=oidc_domain", http.StatusFound)
			return
		}
	}

	// Look up existing user by OIDC subject
	user, err := h.pg.GetUserByOIDCSubject(ctx, claims.Sub)
	if err != nil {
		log.Printf("[OIDC] Database error looking up subject: %v", err)
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	// JIT provisioning: create user if not found
	if user == nil {
		username := deriveUsername(claims.Email, claims.PreferredUsername, claims.Sub)

		// Handle username collision
		baseUsername := username
		found := false
		for attempt := 0; attempt <= 10; attempt++ {
			existing, _ := h.pg.GetUser(ctx, username)
			if existing == nil {
				found = true
				break
			}
			username = fmt.Sprintf("%s_%d", baseUsername, attempt+1)
		}
		if !found {
			log.Printf("[OIDC] Could not find available username for sub: %s", claims.Sub)
			h.logAuthEvent("oidc_login_failed", claims.Email, ip, "username collision exhausted")
			http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
			return
		}

		displayName := claims.Name
		if displayName == "" {
			displayName = username
		}
		isAdmin := h.config.DefaultRole == "admin"

		if err := h.pg.CreateOIDCUser(ctx, username, displayName, claims.Sub, isAdmin); err != nil {
			log.Printf("[OIDC] Failed to create user: %v", err)
			http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
			return
		}
		log.Printf("[OIDC] Provisioned user: %s (sub: %s)", username, claims.Sub)
		user = &storage.User{Username: username}
	}

	// Update last login
	_ = h.pg.UpdateLastLogin(ctx, user.Username)

	// Create session (reuses existing session infrastructure)
	sessionID, err := h.createSess(user.Username)
	if err != nil {
		log.Printf("[OIDC] Failed to create session: %v", err)
		http.Redirect(w, r, "/login.html?error=oidc_failed", http.StatusFound)
		return
	}

	h.logAuthEvent("oidc_login_success", user.Username, ip, "")

	// Set session cookie with SameSite=Lax (required for cross-site redirect from OIDC provider)
	http.SetCookie(w, &http.Cookie{
		Name:     "bifract_session",
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.secureCookies,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400,
	})

	http.Redirect(w, r, "/", http.StatusFound)
}

// deriveUsername picks a username from available OIDC claims.
func deriveUsername(email, preferredUsername, sub string) string {
	username := sub
	if email != "" {
		parts := strings.SplitN(email, "@", 2)
		if len(parts) > 0 && parts[0] != "" {
			username = parts[0]
		}
	} else if preferredUsername != "" {
		username = preferredUsername
	}
	username = strings.ToLower(username)
	if len(username) > 50 {
		username = username[:50]
	}
	return username
}
