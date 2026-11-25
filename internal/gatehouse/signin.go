package gatehouse

// Signing in to another grid's GateHouse, for a device of a person that
// grid knows (a friend of its owner): `storeboard login` does it once in the
// browser and keeps the refresh token; the daemon turns it into an access
// token for each connection it makes there, and keeps the newest refresh
// token it is given (they last 30 days from the last use).

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Credentials is what a sign-in keeps on disk.
type Credentials struct {
	GateHouse    string `json:"gatehouse"`
	Person       string `json:"person,omitempty"`
	RefreshToken string `json:"refresh_token"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Error        string `json:"error"`
	Description  string `json:"error_description"`
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

func postToken(gatehouse string, form url.Values) (*tokenResponse, error) {
	form.Set("client_id", Service)
	resp, err := httpClient.PostForm(strings.TrimSuffix(gatehouse, "/")+"/token", form)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("GateHouse answered %s", resp.Status)
	}
	if resp.StatusCode != http.StatusOK || out.AccessToken == "" {
		msg := out.Error
		if out.Description != "" {
			msg += ": " + out.Description
		}
		return nil, fmt.Errorf("GateHouse refused: %s", msg)
	}
	return &out, nil
}

func writeCredentials(path string, c Credentials) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Login signs a person in through the browser: open is given the address
// to show them (and may open a browser). It returns who signed in.
func Login(ctx context.Context, gatehouse, credentials string, open func(string)) (string, error) {
	verifier := randomString(32)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	state := randomString(16)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	redirect := fmt.Sprintf("http://%s/callback", ln.Addr().String())
	got := make(chan url.Values, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/callback" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("state") != state {
			http.Error(w, "this sign-in was not started here", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<!doctype html><title>Signed in</title><p>Signed in to %s. You can close this tab.</p>", html.EscapeString(gatehouse))
		select {
		case got <- q:
		default:
		}
	}), ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln)
	defer srv.Close()

	q := url.Values{"client_id": {Service}, "redirect_uri": {redirect}, "state": {state},
		"code_challenge": {challenge}, "code_challenge_method": {"S256"}, "response_type": {"code"}}
	open(strings.TrimSuffix(gatehouse, "/") + "/authorize?" + q.Encode())

	var back url.Values
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case back = <-got:
	}
	code := back.Get("code")
	if code == "" {
		return "", errors.New("GateHouse sent no code back")
	}
	tok, err := postToken(gatehouse, url.Values{"grant_type": {"authorization_code"}, "code": {code}, "code_verifier": {verifier}, "redirect_uri": {redirect}})
	if err != nil {
		return "", err
	}
	person := claimName(tok.AccessToken)
	if err := writeCredentials(credentials, Credentials{GateHouse: gatehouse, Person: person, RefreshToken: tok.RefreshToken}); err != nil {
		return "", err
	}
	return person, nil
}

// claimName reads the name out of a token without checking it: only to
// tell the person who they signed in as. The receiving grid checks it.
func claimName(tok string) string {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return ""
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var c struct {
		Name string `json:"name"`
	}
	json.Unmarshal(b, &c)
	return c.Name
}

func randomString(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// Tokens gives access tokens from sign-ins kept on disk, renewing them a
// little before they expire.
type Tokens struct {
	mu     sync.Mutex
	cached map[string]cachedToken
}

type cachedToken struct {
	token   string
	expires time.Time
}

// Token is a current access token for the sign-in kept at credentials.
func (t *Tokens) Token(credentials string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.cached[credentials]; ok && time.Until(c.expires) > time.Minute {
		return c.token, nil
	}
	b, err := os.ReadFile(credentials)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("not signed in: run `storeboard login` (%s)", credentials)
		}
		return "", err
	}
	var cr Credentials
	if err := json.Unmarshal(b, &cr); err != nil {
		return "", fmt.Errorf("%s: %w", credentials, err)
	}
	tok, err := postToken(cr.GateHouse, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {cr.RefreshToken}})
	if err != nil {
		return "", err
	}
	if tok.RefreshToken != "" && tok.RefreshToken != cr.RefreshToken {
		cr.RefreshToken = tok.RefreshToken
		if err := writeCredentials(credentials, cr); err != nil {
			return "", err
		}
	}
	if t.cached == nil {
		t.cached = map[string]cachedToken{}
	}
	t.cached[credentials] = cachedToken{tok.AccessToken, time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)}
	return tok.AccessToken, nil
}
