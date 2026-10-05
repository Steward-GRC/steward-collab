// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"encoding/hex"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
)

// TestDroppedSyncFrameSchedulesResync is the core of resync: a full send buffer
// used to make the room discard a document frame silently, which desyncs that
// client permanently — Yjs has no gap detection, so the peer never asks again.
// The drop must instead flag the client for resync.
func TestDroppedSyncFrameSchedulesResync(t *testing.T) {
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	// A client whose send buffer is already full, with no write pump draining it.
	c := &Client{room: room, send: make(chan []byte, 1), userID: "u-1", logger: log.Nop()}
	c.send <- []byte("occupied")

	c.queueSync(EncodeUpdate([]byte{0x01}))

	if !c.resync.Load() {
		t.Fatal("dropped sync frame did not schedule a resync")
	}
}

// TestDroppedAwarenessFrameDoesNotScheduleResync guards the deliberate
// asymmetry: presence is transient and re-advertised, so dropping it must NOT
// trigger a document resync.
func TestDroppedAwarenessFrameDoesNotScheduleResync(t *testing.T) {
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	// A client whose send buffer is already full, with no write pump draining it.
	c := &Client{room: room, send: make(chan []byte, 1), userID: "u-1", logger: log.Nop()}
	c.send <- []byte("occupied")

	c.queueAwareness(awarenessFrameFor(1, 1, `{"a":1}`))

	if c.resync.Load() {
		t.Fatal("dropped awareness frame scheduled a document resync")
	}
}

// TestResyncDeliversRoomStateToStaleClient is the end-to-end repair: once the
// write pump drains, a client flagged for resync receives a SyncStep2 carrying
// the room's document state, with no cooperation required from the client.
func TestResyncDeliversRoomStateToStaleClient(t *testing.T) {
	ctx := t.Context()
	stored := mustHex(t, realFullUpdate)
	room := newRoom("draft-1", "policy-1", "tpl-1", stored, newMemStore(), nil, nil, log.Nop())
	go room.run(ctx)

	clients := make(chan *Client, 1)
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		clients <- NewClient(ctx, room, conn, "u-1", "", log.Nop())
	})
	defer srv.Close()

	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	c := <-clients
	// Simulate the drop, then push a frame so the write pump runs its repair.
	c.resync.Store(true)
	c.queueSync(EncodeUpdate([]byte{0xAA}))

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		msgType, payload, err := DecodeMessage(msg)
		if err != nil || msgType != MsgSync {
			continue
		}
		syncType, data, err := DecodeSyncMessage(payload)
		if err != nil {
			t.Fatalf("decode sync: %v", err)
		}
		if syncType != SyncStep2 {
			continue
		}
		if hex.EncodeToString(data) != realFullUpdate {
			t.Fatalf("resync payload\n got: %s\nwant: %s", hex.EncodeToString(data), realFullUpdate)
		}
		if c.resync.Load() {
			t.Fatal("resync flag was not cleared after the repair")
		}
		return
	}
	t.Fatal("stale client never received a resync SyncStep2")
}
