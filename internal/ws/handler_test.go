// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/store"
	"github.com/Steward-GRC/steward-collab/internal/ws"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

// memStore is an in-memory store.Store implementation for tests.
type memStore struct {
	docs map[string]store.Document
}

func newMemStore() *memStore { return &memStore{docs: make(map[string]store.Document)} }

func (m *memStore) Load(_ context.Context, draftID string) (*store.Document, error) {
	d, ok := m.docs[draftID]
	if !ok {
		return nil, nil
	}
	cp := d
	cp.YjsState = append([]byte(nil), d.YjsState...)
	return &cp, nil
}

func (m *memStore) Save(_ context.Context, doc store.Document) error {
	doc.YjsState = append([]byte(nil), doc.YjsState...)
	m.docs[doc.DraftID] = doc
	return nil
}

const testSecret = "test-secret-do-not-use-in-prod"

// signToken returns a signed HS256 JWT with the given claims.
func signToken(t *testing.T, claims ws.CollabClaims, secret string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

// validClaims returns a baseline set of valid claims for draftID.
func validClaims(draftID, policyID, tvID, userID string) ws.CollabClaims {
	return ws.CollabClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-1 * time.Second)),
			Audience:  jwt.ClaimStrings{ws.TokenAudience},
		},
		UserID:            userID,
		PolicyID:          policyID,
		DraftID:           draftID,
		TemplateVersionID: tvID,
	}
}

func TestHandler_RejectsMissingToken(t *testing.T) {
	h := ws.NewHandler(nil, testSecret, log.Nop())
	req := httptest.NewRequest(http.MethodGet, "/ws/draft-1", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if got := rr.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer ") {
		t.Fatalf("expected WWW-Authenticate Bearer challenge, got %q", got)
	}
}

func TestHandler_RejectsBadToken(t *testing.T) {
	h := ws.NewHandler(nil, testSecret, log.Nop())
	req := httptest.NewRequest(http.MethodGet, "/ws/draft-1?token=not-a-jwt", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if got := rr.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer ") {
		t.Fatalf("expected WWW-Authenticate Bearer challenge, got %q", got)
	}
}

func TestHandler_RejectsWrongSignature(t *testing.T) {
	h := ws.NewHandler(nil, testSecret, log.Nop())
	tok := signToken(t, validClaims("draft-1", "p-1", "tv-1", "u-1"), "wrong-secret")
	req := httptest.NewRequest(http.MethodGet, "/ws/draft-1?token="+tok, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandler_RejectsExpiredToken(t *testing.T) {
	h := ws.NewHandler(nil, testSecret, log.Nop())
	c := validClaims("draft-1", "p-1", "tv-1", "u-1")
	c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-1 * time.Minute))
	c.IssuedAt = jwt.NewNumericDate(time.Now().Add(-2 * time.Minute))
	tok := signToken(t, c, testSecret)
	req := httptest.NewRequest(http.MethodGet, "/ws/draft-1?token="+tok, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandler_RejectsWrongAudience(t *testing.T) {
	h := ws.NewHandler(nil, testSecret, log.Nop())
	c := validClaims("draft-1", "p-1", "tv-1", "u-1")
	c.Audience = jwt.ClaimStrings{"some-other-aud"}
	tok := signToken(t, c, testSecret)
	req := httptest.NewRequest(http.MethodGet, "/ws/draft-1?token="+tok, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandler_RejectsPathMismatch(t *testing.T) {
	h := ws.NewHandler(nil, testSecret, log.Nop())
	// Token says draft-1 but URL asks for draft-2.
	tok := signToken(t, validClaims("draft-1", "p-1", "tv-1", "u-1"), testSecret)
	req := httptest.NewRequest(http.MethodGet, "/ws/draft-2?token="+tok, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandler_AcceptsBearerHeader(t *testing.T) {
	// With a real hub, verify the bearer header path is accepted and the conn
	// is upgraded. We use an httptest.Server so the upgrader sees a real TCP conn.
	ctx := t.Context()

	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(ctx)
	h := ws.NewHandler(hub, testSecret, log.Nop())

	srv := httptest.NewServer(h)
	defer srv.Close()
	tok := signToken(t, validClaims("draft-hdr", "p-1", "tv-1", "u-1"), testSecret)

	u, _ := url.Parse(srv.URL)
	wsURL := "ws://" + u.Host + "/ws/draft-hdr"
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+tok)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial: %v (status=%v)", err, resp)
	}
	defer func() { _ = conn.Close() }()
	// Wait for the room to be registered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.RoomCount() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := hub.RoomCount(); got != 1 {
		t.Fatalf("expected 1 room, got %d", got)
	}
}

func TestHandler_AcceptsValidTokenAndJoinsRoom(t *testing.T) {
	ctx := t.Context()

	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(ctx)
	h := ws.NewHandler(hub, testSecret, log.Nop())

	srv := httptest.NewServer(h)
	defer srv.Close()
	tok := signToken(t, validClaims("draft-42", "p-42", "tv-42", "u-42"), testSecret)

	u, _ := url.Parse(srv.URL)
	wsURL := "ws://" + u.Host + "/ws/draft-42?token=" + tok
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v (status=%v)", err, resp)
	}
	defer func() { _ = conn.Close() }()
	// First message from server is the initial Sync1.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read initial sync: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("expected binary message, got type %d", mt)
	}
	if len(msg) == 0 {
		t.Fatalf("empty initial sync message")
	}

	// Verify the hub now has a room for this draft.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.RoomCount() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected 1 room, got %d", hub.RoomCount())
}

// TestHandler_AcceptsTokenWithEmptyTemplateVersion pins the WS half of
// freeform co-editing. A free-form policy's token carries an EMPTY tvid claim, and the
// socket must accept it and open a room: pid/did/uid are the required claims,
// tvid is not. This is the link between the mint and the snapshot path, so it
// is asserted rather than inferred from reading the required-claims check.
func TestHandler_AcceptsTokenWithEmptyTemplateVersion(t *testing.T) {
	ctx := t.Context()

	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(ctx)
	h := ws.NewHandler(hub, testSecret, log.Nop())

	srv := httptest.NewServer(h)
	defer srv.Close()
	// No template version: this draft is free-form.
	tok := signToken(t, validClaims("draft-ff", "p-ff", "", "u-ff"), testSecret)

	u, _ := url.Parse(srv.URL)
	wsURL := "ws://" + u.Host + "/ws/draft-ff?token=" + tok
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial with an empty tvid claim: %v (status=%v)", err, resp)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read initial sync: %v", err)
	}
	if mt != websocket.BinaryMessage || len(msg) == 0 {
		t.Fatalf("expected a non-empty binary initial sync, got type=%d len=%d", mt, len(msg))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.RoomCount() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected 1 room for a free-form draft, got %d", hub.RoomCount())
}

// TestHandler_StillRejectsMissingRequiredClaims confirms freeform co-editing relaxed only
// tvid: a token missing pid, did or uid is still refused at the socket.
func TestHandler_StillRejectsMissingRequiredClaims(t *testing.T) {
	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	h := ws.NewHandler(hub, testSecret, log.Nop())
	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, tc := range []struct {
		name                string
		draft, policy, user string
	}{
		{"no policy id", "draft-x", "", "u-x"},
		{"no user id", "draft-x", "p-x", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// tvid empty too, so the only defect is the required claim.
			tok := signToken(t, validClaims(tc.draft, tc.policy, "", tc.user), testSecret)
			u, _ := url.Parse(srv.URL)
			wsURL := "ws://" + u.Host + "/ws/" + tc.draft + "?token=" + tok
			conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err == nil {
				_ = conn.Close()
				t.Fatal("expected the dial to be refused")
			}
			if resp == nil || resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %v", resp)
			}
		})
	}
}
