// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/ws"
	"github.com/gorilla/websocket"
)

// dialWithOrigin dials srv's /ws/{draftID} with a valid token and the given
// Origin header ("" sends none), returning the handshake status code. It never
// returns a usable conn — every caller here only cares about accept/reject.
func dialWithOrigin(t *testing.T, srvURL, draftID, token, origin string) int {
	t.Helper()
	u, err := url.Parse(srvURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	hdr := http.Header{}
	if origin != "" {
		hdr.Set("Origin", origin)
	}
	conn, resp, err := websocket.DefaultDialer.Dial("ws://"+u.Host+"/ws/"+draftID+"?token="+token, hdr)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		return http.StatusSwitchingProtocols
	}
	if resp == nil {
		t.Fatalf("dial failed with no response: %v", err)
	}
	return resp.StatusCode
}

// originTestServer stands up a collab ws handler with a live hub over an
// httptest.Server (a real TCP conn — the upgrader cannot hijack a recorder).
func originTestServer(t *testing.T, allowed []string) *httptest.Server {
	t.Helper()
	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(t.Context())
	h := ws.NewHandler(hub, testSecret, log.Nop()).WithAllowedOrigins(allowed)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// The gateway's WebSocket reverse proxy dials collab server-to-server and sends
// no Origin header. That is the ONLY sanctioned path to this socket, so it must
// stay accepted with an empty allowlist.
func TestCheckOrigin_AllowsMissingOrigin(t *testing.T) {
	srv := originTestServer(t, nil)
	tok := signToken(t, validClaims("draft-o1", "p-1", "tv-1", "u-1"), testSecret)
	if got := dialWithOrigin(t, srv.URL, "draft-o1", tok, ""); got != http.StatusSwitchingProtocols {
		t.Fatalf("no-Origin upgrade: got status %d, want 101", got)
	}
}

// A browser dialing collab DIRECTLY is off the sanctioned path.
// With no allowlist configured (dev/qa/prod) the upgrade must be refused even
// though the token itself is perfectly valid — this is the behaviour change
// from CheckOrigin returning true unconditionally.
func TestCheckOrigin_RejectsBrowserOriginByDefault(t *testing.T) {
	srv := originTestServer(t, nil)
	tok := signToken(t, validClaims("draft-o2", "p-1", "tv-1", "u-1"), testSecret)
	if got := dialWithOrigin(t, srv.URL, "draft-o2", tok, "https://evil.example.com"); got != http.StatusForbidden {
		t.Fatalf("cross-origin upgrade: got status %d, want 403", got)
	}
}

// COLLAB_ALLOWED_ORIGINS is the per-env escape hatch for a deployment that
// exposes collab on its own hostname. An allowlisted origin is accepted...
func TestCheckOrigin_AllowsAllowlistedOrigin(t *testing.T) {
	srv := originTestServer(t, []string{"https://policies.example.org"})
	tok := signToken(t, validClaims("draft-o3", "p-1", "tv-1", "u-1"), testSecret)
	if got := dialWithOrigin(t, srv.URL, "draft-o3", tok, "https://policies.example.org"); got != http.StatusSwitchingProtocols {
		t.Fatalf("allowlisted upgrade: got status %d, want 101", got)
	}
}

// ...and matching is case-insensitive on the scheme/host, per RFC 6454 origin
// serialisation, while anything NOT on the list stays refused.
func TestCheckOrigin_AllowlistIsCaseInsensitiveAndExact(t *testing.T) {
	srv := originTestServer(t, []string{"https://Policies.Example.ORG"})
	tok := signToken(t, validClaims("draft-o4", "p-1", "tv-1", "u-1"), testSecret)
	if got := dialWithOrigin(t, srv.URL, "draft-o4", tok, "https://policies.example.org"); got != http.StatusSwitchingProtocols {
		t.Fatalf("case-insensitive match: got status %d, want 101", got)
	}
	// A different host that merely shares a suffix must NOT match.
	if got := dialWithOrigin(t, srv.URL, "draft-o4", tok, "https://evil.policies.example.org"); got != http.StatusForbidden {
		t.Fatalf("suffix impostor: got status %d, want 403", got)
	}
}
