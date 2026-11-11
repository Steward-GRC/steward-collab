// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"

	"github.com/Steward-GRC/steward-collab/internal/ws"
)

// Mock "does not open the policy's own room: a token names its own room only".
// An appendix token names the room "appendix:<id>"; it opens that room, by the
// raw path or the percent-encoded one the gateway forwards, and no other.
func TestHandler_AppendixTokenOpensItsOwnRoomOnly(t *testing.T) {
	ctx := t.Context()
	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(ctx)
	srv := httptest.NewServer(ws.NewHandler(hub, testSecret, log.Nop()))
	defer srv.Close()
	host := func() string { u, _ := url.Parse(srv.URL); return u.Host }()
	tok := signToken(t, validClaims("appendix:appendix-1", "policy-1", "", "u-bob"), testSecret)

	for _, path := range []string{"/ws/appendix:appendix-1", "/ws/appendix%3Aappendix-1"} {
		conn, resp, err := websocket.DefaultDialer.Dial("ws://"+host+path+"?token="+tok, nil)
		if err != nil {
			t.Fatalf("dial %s: %v (resp %v)", path, err, resp)
		}
		_ = conn.Close()
	}
	deadline := time.Now().Add(2 * time.Second)
	for hub.RoomCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := hub.LookupRoom("appendix:appendix-1"); !ok && hub.RoomCount() == 0 {
		t.Fatal("no appendix room was opened")
	}

	for _, path := range []string{"/ws/version-2", "/ws/appendix-1", "/ws/"} {
		rr := httptest.NewRecorder()
		ws.NewHandler(hub, testSecret, log.Nop()).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path+"?token="+tok, nil))
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("%s: got %d want 401", path, rr.Code)
		}
	}
}

// The policy room's token does not open an appendix room either.
func TestHandler_PolicyTokenDoesNotOpenAnAppendixRoom(t *testing.T) {
	tok := signToken(t, validClaims("draft-1", "policy-1", "tv-1", "u-bob"), testSecret)
	rr := httptest.NewRecorder()
	ws.NewHandler(nil, testSecret, log.Nop()).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/ws/appendix:appendix-1?token="+tok, nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("got %d want 401", rr.Code)
	}
}
