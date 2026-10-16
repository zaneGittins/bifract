package oidc

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

var (
	keyOnce          sync.Once
	signingKey       *rsa.PrivateKey
	strangerKey      *rsa.PrivateKey
	errKeyGeneration error
)

// testKeys returns the IdP's signing key and an unrelated key, generated once
// per test binary because RSA generation is slow.
func testKeys(t *testing.T) (*rsa.PrivateKey, *rsa.PrivateKey) {
	t.Helper()
	keyOnce.Do(func() {
		if signingKey, errKeyGeneration = rsa.GenerateKey(rand.Reader, 2048); errKeyGeneration != nil {
			return
		}
		strangerKey, errKeyGeneration = rsa.GenerateKey(rand.Reader, 2048)
	})
	if errKeyGeneration != nil {
		t.Fatal(errKeyGeneration)
	}
	return signingKey, strangerKey
}

const testClientID = "bifract-test"

// fakeIdP is a minimal OpenID provider: discovery, JWKS, and a token endpoint
// that answers with whatever the test scripted.
type fakeIdP struct {
	*httptest.Server
	t   *testing.T
	key *rsa.PrivateKey

	mu sync.Mutex
	// idToken is the signed ID token the token endpoint returns; "" omits it.
	idToken string
	// tokenStatus, when non-zero, fails the token endpoint with this status.
	tokenStatus int
	// codes records the authorization codes redeemed.
	codes []string
}

func newFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	key, _ := testKeys(t)
	idp := &fakeIdP{t: t, key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                idp.URL,
			"authorization_endpoint":                idp.URL + "/authorize",
			"token_endpoint":                        idp.URL + "/token",
			"jwks_uri":                              idp.URL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		pub := idp.key.PublicKey
		json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
			"n": b64(pub.N.Bytes()),
			"e": b64(big.NewInt(int64(pub.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		idp.mu.Lock()
		defer idp.mu.Unlock()
		_ = r.ParseForm()
		idp.codes = append(idp.codes, r.PostForm.Get("code"))
		if idp.tokenStatus != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(idp.tokenStatus)
			w.Write([]byte(`{"error":"invalid_grant"}`))
			return
		}
		resp := map[string]any{"access_token": "at", "token_type": "Bearer", "expires_in": 3600}
		if idp.idToken != "" {
			resp["id_token"] = idp.idToken
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	idp.Server = httptest.NewServer(mux)
	t.Cleanup(idp.Close)
	return idp
}

// claims returns a valid claim set for this IdP; tests override fields.
func (idp *fakeIdP) claims(nonce string) map[string]any {
	now := time.Now()
	return map[string]any{
		"iss":   idp.URL,
		"aud":   testClientID,
		"sub":   "sub-123",
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
		"nonce": nonce,
		"email": "Alice@Example.com",
		"name":  "Alice Example",
	}
}

// issue scripts the token endpoint to return claims signed by key.
func (idp *fakeIdP) issue(claims map[string]any, key *rsa.PrivateKey) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	idp.idToken = signJWT(idp.t, claims, key)
}

func signJWT(t *testing.T, claims map[string]any, key *rsa.PrivateKey) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "k1", "typ": "JWT"})
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signing := b64(header) + "." + b64(payload)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + b64(sig)
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
