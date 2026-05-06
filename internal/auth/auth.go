// Package auth handles MQTT authentication (JWT, bcrypt) and ACL enforcement.
package auth

import (
	"crypto/subtle"
	"crypto/x509"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// ─── Errors ───────────────────────────────────────────────────────────────────

var (
	ErrBadCredentials = errors.New("bad credentials")
	ErrTokenExpired   = errors.New("token expired")
	ErrTokenInvalid   = errors.New("token invalid")
	ErrBanned         = errors.New("client banned")
	ErrDenied         = errors.New("operation not permitted")
)

// ─── Permission Flags ─────────────────────────────────────────────────────────

type Perm uint8

const (
	PermNone      Perm = 0
	PermSubscribe Perm = 1
	PermPublish   Perm = 2
	PermAll       Perm = PermSubscribe | PermPublish
)

func (p Perm) CanSubscribe() bool { return p&PermSubscribe != 0 }
func (p Perm) CanPublish() bool   { return p&PermPublish != 0 }

// ─── ACL Rule ─────────────────────────────────────────────────────────────────

type ACLRule struct {
	ClientGlob  string // "*" or exact clientID
	TopicFilter string // MQTT filter (+ and # supported)
	Perm        Perm
}

func (r *ACLRule) matches(clientID, topic string) bool {
	return globMatch(r.ClientGlob, clientID) && topicFilterMatches(r.TopicFilter, topic)
}

// ─── JWT Claims ───────────────────────────────────────────────────────────────

type Claims struct {
	ClientID string   `json:"cid,omitempty"`
	Username string   `json:"sub"`
	Roles    []string `json:"roles"`
	jwt.RegisteredClaims
}

// ─── User ────────────────────────────────────────────────────────────────────

type User struct {
	Username     string
	PasswordHash string // bcrypt
	Roles        []string
	Banned       bool
}

// ─── Token Cache ─────────────────────────────────────────────────────────────

type cachedClaims struct {
	claims    *Claims
	expiresAt time.Time
}

// ─── Authenticator ───────────────────────────────────────────────────────────

type Authenticator struct {
	secret   []byte
	issuer   string
	duration time.Duration
	cost     int

	mu       sync.RWMutex
	users    map[string]*User
	aclRules []*ACLRule

	// JWT cache: avoids bcrypt-level work for repeated JWT validations
	cacheMu            sync.RWMutex
	tokenCache         map[string]*cachedClaims
	revokedCertSerials map[string]struct{}
}

// New creates a new Authenticator.
func New(secret, issuer string, duration time.Duration, bcryptCost int) *Authenticator {
	a := &Authenticator{
		secret:             []byte(secret),
		issuer:             issuer,
		duration:           duration,
		cost:               bcryptCost,
		users:              make(map[string]*User),
		tokenCache:         make(map[string]*cachedClaims),
		revokedCertSerials: make(map[string]struct{}),
	}
	a.loadDefaultACL()
	return a
}

func (a *Authenticator) SetRevokedSerials(serials []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revokedCertSerials = make(map[string]struct{}, len(serials))
	for _, s := range serials {
		s = strings.TrimSpace(strings.ToLower(s))
		if s != "" {
			a.revokedCertSerials[s] = struct{}{}
		}
	}
}

func (a *Authenticator) AuthX509(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", ErrBadCredentials
	}
	serial := strings.ToLower(cert.SerialNumber.Text(16))
	a.mu.RLock()
	_, revoked := a.revokedCertSerials[serial]
	a.mu.RUnlock()
	if revoked {
		return "", ErrBanned
	}
	identity := cert.Subject.CommonName
	if identity == "" {
		identity = cert.Subject.String()
	}
	return identity, nil
}

func (a *Authenticator) loadDefaultACL() {
	// Read-only access to $SYS for everyone
	a.aclRules = []*ACLRule{
		{ClientGlob: "*", TopicFilter: "$SYS/#", Perm: PermSubscribe},
	}
}

// ─── User Management ─────────────────────────────────────────────────────────

func (a *Authenticator) AddUser(username, plainPassword string, roles []string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(plainPassword), a.cost)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.users[username] = &User{
		Username:     username,
		PasswordHash: string(hash),
		Roles:        roles,
	}
	a.mu.Unlock()
	return nil
}

func (a *Authenticator) SetBanned(username string, banned bool) {
	a.mu.Lock()
	if u, ok := a.users[username]; ok {
		u.Banned = banned
	}
	a.mu.Unlock()
}

func (a *Authenticator) AddACLRule(rule *ACLRule) {
	a.mu.Lock()
	// Prepend for higher priority
	a.aclRules = append([]*ACLRule{rule}, a.aclRules...)
	a.mu.Unlock()
}

// ─── Authentication ───────────────────────────────────────────────────────────

// AuthPassword verifies username + password credentials.
func (a *Authenticator) AuthPassword(username string, password []byte) error {
	a.mu.RLock()
	u, ok := a.users[username]
	a.mu.RUnlock()

	if !ok {
		// Constant-time dummy to prevent timing attacks
		_ = subtle.ConstantTimeCompare(password, []byte("🔒"))
		return ErrBadCredentials
	}
	if u.Banned {
		return ErrBanned
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), password); err != nil {
		return ErrBadCredentials
	}
	return nil
}

// AuthJWT validates a JWT token string (passed as MQTT password field).
func (a *Authenticator) AuthJWT(tokenStr string) (*Claims, error) {
	// Check cache first (avoids JWT parsing overhead on reconnect storms)
	a.cacheMu.RLock()
	if cached, ok := a.tokenCache[tokenStr]; ok {
		if time.Now().Before(cached.expiresAt) {
			a.cacheMu.RUnlock()
			return cached.claims, nil
		}
	}
	a.cacheMu.RUnlock()

	token, err := jwt.ParseWithClaims(tokenStr, &Claims{},
		func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, ErrTokenInvalid
			}
			return a.secret, nil
		},
		jwt.WithIssuer(a.issuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrTokenInvalid
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrTokenInvalid
	}

	// Cache the validated claims
	a.cacheMu.Lock()
	a.tokenCache[tokenStr] = &cachedClaims{
		claims:    claims,
		expiresAt: claims.ExpiresAt.Time,
	}
	// Trim cache to 10k entries
	if len(a.tokenCache) > 10000 {
		for k := range a.tokenCache {
			delete(a.tokenCache, k)
			break
		}
	}
	a.cacheMu.Unlock()

	return claims, nil
}

// IssueToken generates a new signed JWT for the given user.
func (a *Authenticator) IssueToken(username, clientID string, roles []string) (string, error) {
	now := time.Now()
	claims := &Claims{
		ClientID: clientID,
		Username: username,
		Roles:    roles,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    a.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(a.duration)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(a.secret)
}

// ─── Authorization ────────────────────────────────────────────────────────────

func (a *Authenticator) CanPublish(clientID, topic string) bool {
	if strings.HasPrefix(topic, "$SYS/") {
		return false // clients cannot publish to $SYS
	}
	return a.checkACL(clientID, topic, PermPublish)
}

func (a *Authenticator) CanSubscribe(clientID, filter string) bool {
	return a.checkACL(clientID, filter, PermSubscribe)
}

func (a *Authenticator) checkACL(clientID, topic string, perm Perm) bool {
	a.mu.RLock()
	rules := a.aclRules
	a.mu.RUnlock()

	for _, r := range rules {
		if r.matches(clientID, topic) {
			return r.Perm&perm != 0
		}
	}
	return true // default allow for authenticated clients
}

// ─── Rate Limiter (Token Bucket) ─────────────────────────────────────────────

// RateLimiter is a per-client token bucket rate limiter.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64
	burst   float64
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewRateLimiter(rate, burst float64) *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*bucket, 1024),
		rate:    rate,
		burst:   burst,
	}
}

func (r *RateLimiter) Allow(clientID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	b, ok := r.buckets[clientID]
	if !ok {
		b = &bucket{tokens: r.burst, last: now}
		r.buckets[clientID] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * r.rate
	if b.tokens > r.burst {
		b.tokens = r.burst
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (r *RateLimiter) Cleanup(maxAge time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for id, b := range r.buckets {
		if b.last.Before(cutoff) {
			delete(r.buckets, id)
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func globMatch(pattern, s string) bool {
	return pattern == "*" || pattern == s
}

func topicFilterMatches(filter, topic string) bool {
	fSegs := strings.Split(filter, "/")
	tSegs := strings.Split(topic, "/")
	return matchSegs(fSegs, tSegs, 0, 0)
}

func matchSegs(f, t []string, fi, ti int) bool {
	for fi < len(f) {
		seg := f[fi]
		if seg == "#" {
			return true
		}
		if ti >= len(t) {
			return false
		}
		if seg != "+" && seg != t[ti] {
			return false
		}
		fi++
		ti++
	}
	return ti == len(t)
}
