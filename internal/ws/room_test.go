// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/store"
	"github.com/gorilla/websocket"
)

// memStore is an in-memory implementation of store.Store for tests. It is
// mutex-guarded because saves now happen on the room's flush goroutine while
// the test body inspects what was written.
type memStore struct {
	mu    sync.Mutex
	docs  map[string]store.Document
	saves int
}

func newMemStore() *memStore { return &memStore{docs: make(map[string]store.Document)} }

func (m *memStore) Load(_ context.Context, draftID string) (*store.Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[draftID]
	if !ok {
		return nil, nil
	}
	cp := d
	cp.YjsState = append([]byte(nil), d.YjsState...)
	return &cp, nil
}

func (m *memStore) Save(_ context.Context, doc store.Document) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	doc.YjsState = append([]byte(nil), doc.YjsState...)
	m.docs[doc.DraftID] = doc
	m.saves++
	return nil
}

// savedState returns the persisted Yjs state for a draft, or nil if never saved.
func (m *memStore) savedState(draftID string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[draftID]
	if !ok {
		return nil
	}
	return append([]byte(nil), d.YjsState...)
}

// saveCount reports how many times Save was called.
func (m *memStore) saveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saves
}

// wsTestServer spins up an httptest.Server that upgrades requests to websocket
// and hands each connection to a callback. Returns the server and a ws:// URL.
func wsTestServer(t *testing.T, onConn func(conn *websocket.Conn)) (*httptest.Server, string) {
	t.Helper()
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		onConn(conn)
	}))
	u, _ := url.Parse(srv.URL)
	wsURL := "ws://" + u.Host
	return srv, wsURL
}

func dialWS(t *testing.T, wsURL string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

// readUntilUpdate reads messages from conn until it sees a Sync/Update message
// or the deadline expires. Returns the inner update payload.
func readUntilUpdate(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		msgType, payload, err := DecodeMessage(msg)
		if err != nil {
			continue
		}
		if msgType != MsgSync {
			continue
		}
		syncType, data, err := DecodeSyncMessage(payload)
		if err != nil {
			continue
		}
		if syncType == SyncUpdate {
			return data
		}
	}
}

// waitForClients blocks until the room has registered n clients. Reading a
// frame from a client's socket only proves its write pump started, NOT that the
// room goroutine has picked its registration off the channel — and the room's
// select has no ordering guarantee between a pending registration and a pending
// broadcast, so a test that skips this can legitimately miss the fan-out.
func waitForClients(t *testing.T, room *Room, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if room.ClientCount() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("room registered %d client(s), want %d", room.ClientCount(), n)
}

// drainInitialSync reads and discards the server's initial Sync1 message.
func drainInitialSync(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read initial sync: %v", err)
	}
}

func TestRoom_BroadcastsUpdateToOtherClients(t *testing.T) {
	ctx := t.Context()

	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	go room.run(ctx)

	// Spin up a server that wires each incoming connection into the room.
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "u-"+conn.RemoteAddr().String(), "", log.Nop())
	})
	defer srv.Close()
	a := dialWS(t, wsURL)
	defer func() { _ = a.Close() }()
	b := dialWS(t, wsURL)
	defer func() { _ = b.Close() }()
	// Both clients receive an initial Sync1 from the server.
	drainInitialSync(t, a)
	drainInitialSync(t, b)
	// …and both must be registered before A types, or the fan-out can race the
	// registration and B never sees the update.
	waitForClients(t, room, 2)

	// A sends a SyncUpdate.
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	update := EncodeUpdate(payload)
	if err := a.WriteMessage(websocket.BinaryMessage, update); err != nil {
		t.Fatalf("write: %v", err)
	}

	// B should receive the update.
	got := readUntilUpdate(t, b)
	if string(got) != string(payload) {
		t.Fatalf("broadcast payload mismatch: got %x, want %x", got, payload)
	}
}

func TestHub_CreatesAndRemovesRoom(t *testing.T) {
	ctx := t.Context()

	hub := NewHub(newMemStore(), nil, nil, log.Nop())
	if got := hub.RoomCount(); got != 0 {
		t.Fatalf("initial RoomCount: got %d, want 0", got)
	}

	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	if got := hub.RoomCount(); got != 1 {
		t.Fatalf("after first join RoomCount: got %d, want 1", got)
	}

	// Second call must return the same room without creating a new one.
	room2, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("second GetOrCreateRoom: %v", err)
	}
	if room != room2 {
		t.Fatalf("expected same room instance on second call")
	}
	if got := hub.RoomCount(); got != 1 {
		t.Fatalf("after second call RoomCount: got %d, want 1", got)
	}

	// Simulate a client lifecycle: register then unregister; room should drop itself.
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "u-1", "", log.Nop())
	})
	defer srv.Close()
	c := dialWS(t, wsURL)
	drainInitialSync(t, c)
	// Close the client; readPump should unregister, triggering room removal.
	_ = c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.RoomCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("room was not removed after last client left; RoomCount=%d", hub.RoomCount())
}

func TestClient_ReadPumpTerminatesOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	go room.run(ctx)

	clientCtx, clientCancel := context.WithCancel(ctx)

	connClosed := make(chan struct{})
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		// Wrap the conn to observe close.
		c := NewClient(clientCtx, room, conn, "u-1", "", log.Nop())
		_ = c
		go func() {
			// Poll for conn close by attempting writes on a separate goroutine
			// is not reliable; instead, watch for the readPump to terminate by
			// reading from a sentinel after Close().
			<-clientCtx.Done()
			// Give the readPump a moment to act on the cancel.
			time.Sleep(50 * time.Millisecond)
			close(connClosed)
		}()
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	// Cancel the client context; the readPump should close the conn.
	clientCancel()

	// The remote side should observe a closed connection.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("expected read error after context cancel, got nil")
	}
	// Sanity: error indicates a closed/unexpected EOF, not a timeout.
	msg := err.Error()
	if strings.Contains(msg, "i/o timeout") {
		t.Fatalf("read timed out instead of failing on closed conn: %v", err)
	}

	select {
	case <-connClosed:
	case <-time.After(2 * time.Second):
		t.Fatalf("client readPump did not terminate within timeout")
	}
}
