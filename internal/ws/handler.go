// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"errors"
	"net/http"
	"strings"

	log "github.com/Bugs5382/go-log"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

// TokenAudience is the audience claim that every valid collab token must carry.
// IssueToken mints tokens with this audience; the handler rejects any
// token whose audience does not match.
const TokenAudience = "steward.collab.ws" // #nosec G101 -- a JWT audience name, not a credential

// wwwAuthChallenge is the value of the WWW-Authenticate header sent on 401.
const wwwAuthChallenge = `Bearer realm="steward.collab", error="invalid_token"`

// CollabClaims are the claims of the short-lived co-editing token. IssueToken
// mints them; the JSON names must match grpcsvc.CollabTokenClaims exactly.
type CollabClaims struct {
	jwt.RegisteredClaims
	UserID            string `json:"uid"`
	PolicyID          string `json:"pid"`
	DraftID           string `json:"did"`
	TemplateVersionID string `json:"tvid"`
	// DisplayName is the presence label; presence falls back to UserID when
	// it is empty, so it isn't a required claim.
	DisplayName string `json:"name"`
	// Impersonator is the real admin when the token was minted during act-as.
	Impersonator string `json:"imp,omitempty"`
}

// Handler upgrades HTTP requests to websockets after verifying a short-lived
// HS256-signed JWT supplied as ?token=... or in an Authorization: Bearer header.
type Handler struct {
	hub    *Hub
	secret []byte
	logger log.Logger

	// allowedOrigins is the browser-Origin allowlist enforced by checkOrigin.
	// nil/empty rejects every request that carries an Origin header at all,
	// which is the intended production posture: browsers reach collab through
	// the gateway's WebSocket reverse proxy, never directly.
	allowedOrigins []string
}

// NewHandler returns an HTTP handler that upgrades connections to websockets
// after verifying the short-lived collab token. The returned handler has an
// EMPTY origin allowlist — every request carrying a browser Origin header is
// refused. Call WithAllowedOrigins to widen it (production wiring passes
// AllowedOriginsFromEnv).
func NewHandler(hub *Hub, secret string, logger log.Logger) *Handler {
	return &Handler{hub: hub, secret: []byte(secret), logger: logger}
}

// WithAllowedOrigins sets the browser-Origin allowlist and returns h so the
// call can be chained onto NewHandler. Origins are compared case-insensitively
// against the raw Origin header value (scheme + host + optional port, no
// trailing slash), e.g. "https://policies.example.org".
func (h *Handler) WithAllowedOrigins(origins []string) *Handler {
	h.allowedOrigins = origins
	return h
}

// upgrader returns the websocket upgrader for this handler. Buffer sizes match
// Client's read/write pumps; CheckOrigin is bound to this handler's allowlist.
func (h *Handler) upgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     h.checkOrigin,
	}
}

// checkOrigin is the Upgrader's CheckOrigin. Origin is NOT authentication —
// the bearer JWT is, and collab remains the sole authority for it — but every
// browser reaches this socket through the gateway's
// same-origin WebSocket reverse proxy (GET /collab/ws/{draftID}), so a
// browser-originated upgrade landing here directly is off the sanctioned path
// and is refused by default. Two cases are allowed:
//
//   - No Origin header. Not a browser: the gateway proxy dials collab
//     server-to-server and deliberately sends no Origin, as do the Go
//     e2e/integration clients. The JWT is the credential on this path.
//   - An Origin explicitly allowlisted via COLLAB_ALLOWED_ORIGINS. This exists
//     for a deployment that (temporarily) exposes collab on its own hostname;
//     it is empty in dev/qa/prod.
//
// Allowing every origin would let any web page spend a leaked token from a
// victim's browser. A cross-site page still can't mint one: the gateway's mint
// is a CSRF-protected same-origin mutation.
func (h *Handler) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, allowed := range h.allowedOrigins {
		if strings.EqualFold(allowed, origin) {
			return true
		}
	}
	h.logger.Warn("collab ws upgrade refused: origin not allowed", log.F("origin", origin))
	return false
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tokenStr := extractToken(r)
	if tokenStr == "" {
		h.unauthorized(w, "missing token")
		return
	}

	claims, err := h.verifyToken(tokenStr)
	if err != nil {
		h.logger.Warn("invalid collab token", log.F("error", err.Error()))
		h.unauthorized(w, "invalid token")
		return
	}

	// Extract draftID from URL path: /ws/{draftID}. The path must match the
	// draft claim in the token; otherwise a stolen token could be used against
	// a different draft.
	draftID := strings.TrimPrefix(r.URL.Path, "/ws/")
	if draftID == "" || draftID != claims.DraftID {
		h.unauthorized(w, "token/path mismatch")
		return
	}

	conn, err := h.upgrader().Upgrade(w, r, nil)
	if err != nil {
		// upgrader.Upgrade has already written an error response.
		h.logger.Error(err, "ws upgrade")
		return
	}

	// The websocket outlives this HTTP handler: r.Context() is cancelled when
	// ServeHTTP returns, which would immediately tear the conn down. Use a
	// background context for the room and client lifecycle; the conn's natural
	// close (peer disconnect, readPump error) drives cleanup. Process shutdown
	// is handled by closing the http.Server, which kills the underlying conn.
	bgCtx := context.Background()

	room, err := h.hub.GetOrCreateRoom(bgCtx, claims.DraftID, claims.PolicyID, claims.TemplateVersionID)
	if err != nil {
		h.logger.Error(err, "get or create room", log.F("draft_id", claims.DraftID))
		_ = conn.Close()
		return
	}

	NewImpersonatedClient(bgCtx, room, conn, claims.UserID, claims.DisplayName, claims.Impersonator, h.logger)
}

// extractToken returns the token from ?token= or from an
// Authorization: Bearer <token> header (in that order). Empty if neither is set.
func extractToken(r *http.Request) string {
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	if auth := r.Header.Get("Authorization"); auth != "" {
		const p = "Bearer "
		if after, ok := strings.CutPrefix(auth, p); ok {
			return after
		}
	}
	return ""
}

// verifyToken parses and validates the HS256-signed JWT. It enforces:
//   - signature using the handler's secret with HS256 specifically (no alg=none, no asymmetric algs);
//   - the token's audience equals TokenAudience;
//   - exp/nbf time bounds (the default jwt.v5 validator handles these);
//   - presence of the required claims (draft_id, policy_id, user_id).
func (h *Handler) verifyToken(tokenStr string) (*CollabClaims, error) {
	claims := &CollabClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithAudience(TokenAudience),
		jwt.WithExpirationRequired(),
	)
	_, err := parser.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		return h.secret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims.DraftID == "" || claims.PolicyID == "" || claims.UserID == "" {
		return nil, errors.New("missing required claims")
	}
	return claims, nil
}

// unauthorized writes a 401 with a Bearer challenge consistent with the
// platform authmw package.
func (h *Handler) unauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", wwwAuthChallenge)
	http.Error(w, msg, http.StatusUnauthorized)
}
