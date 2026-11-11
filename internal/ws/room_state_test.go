// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"bytes"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
)

// These tests pin the room-state behaviour steward-mock-server models in its
// collab relay (src/product/collab.test.ts), where the issue asked for
// collab to match it. The differences kept on purpose are in docs/api.md.

const testUser = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"

// roomClient joins room as a connected websocket client and drains the
// server's opening sync step 1.
func roomClient(t *testing.T, room *Room, userID string) *websocket.Conn {
	t.Helper()
	ctx := t.Context()
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, userID, "", log.Nop())
	})
	t.Cleanup(srv.Close)
	conn := dialWS(t, wsURL)
	t.Cleanup(func() { _ = conn.Close() })
	drainInitialSync(t, conn)
	return conn
}

// TestCheckpointThatIsNotJSONIsRefusedAtOnceAndChangesNothing: the mock answers
// a checkpoint whose content is not JSON with snapshot.rejected straight away
// and keeps nothing. Collab used to hold the garbage until the 5 second
// debounce, adopt and store its Yjs state, and only then answer.
func TestCheckpointThatIsNotJSONIsRefusedAtOnceAndChangesNothing(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	st := newMemStore()
	// The production 5 second debounce stays: an answer inside readUntilControl's
	// 3 second budget can only be an immediate one.
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, st, testPublisher(core), nil, log.Nop())
	go room.run(ctx)
	sender := roomClient(t, room, testUser)
	peer := roomClient(t, room, "8f9c1a2e-3b4d-4c5e-8f60-112233445566")
	waitForClients(t, room, 2)

	if err := sender.WriteMessage(websocket.BinaryMessage, EncodeSnapshot("this is not json", []byte{0x09, 0x09})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ctrl := readUntilControl(t, sender)
	if ctrl.Type != ControlSnapshotRejected || ctrl.Reason != "pre_check_failed" {
		t.Fatalf("control: got %+v want snapshot.rejected / pre_check_failed", ctrl)
	}
	if ctrl.DraftID != "draft-1" || ctrl.Detail == "" {
		t.Errorf("control should name the draft and say why: %+v", ctrl)
	}

	room.FlushSnapshot(ctx)
	if got := core.recorded(); len(got) != 0 {
		t.Errorf("a refused checkpoint reached core: %+v", got)
	}
	if st.saveCount() != 0 {
		t.Errorf("a refused checkpoint was stored (%d saves)", st.saveCount())
	}
	if len(room.CurrentState()) != 0 {
		t.Errorf("a refused checkpoint changed the room state: %x", room.CurrentState())
	}

	// Only the sender is answered: the mock replies to the socket that sent it.
	_ = peer.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	for {
		_, msg, err := peer.ReadMessage()
		if err != nil {
			break
		}
		if typ, _, derr := DecodeMessage(msg); derr == nil && typ == MsgControl {
			t.Fatal("a peer received the sender's refusal")
		}
	}
}

// TestCheckpointForAPublishedDraftIsRefused: a published version is not an
// editable draft. The mock answers snapshot.rejected; collab dropped the frame
// without a word, so a late editor believed its edits were kept.
func TestCheckpointForAPublishedDraftIsRefused(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	go room.run(ctx)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	room.MarkPublished(ctx, 3)
	if ctrl := readUntilControl(t, conn); ctrl.Type != ControlDraftPublished {
		t.Fatalf("control: got %+v want draft.published", ctrl)
	}

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	ctrl := readUntilControl(t, conn)
	if ctrl.Type != ControlSnapshotRejected || ctrl.Reason != "server_rejected" {
		t.Fatalf("control: got %+v want snapshot.rejected / server_rejected", ctrl)
	}
	if got := core.recorded(); len(got) != 0 {
		t.Errorf("a checkpoint for a published draft reached core: %+v", got)
	}
}

// TestNextRoomStartsFromTheLastCheckpointAfterEveryoneLeft is the mock's
// "starts the next room from the last checkpoint's Yjs state" over the wire:
// a checkpoint arrives on a socket, the last editor leaves, and a person who
// opens the draft afterwards gets that state on their sync step 1, not an empty
// document.
func TestNextRoomStartsFromTheLastCheckpointAfterEveryoneLeft(t *testing.T) {
	ctx := t.Context()
	st := newMemStore()
	hub := NewHub(st, testPublisher(&fakePolicyClient{}), nil, log.Nop())
	compacted := []byte{0x21, 0x22, 0x23}

	first, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	conn := roomClient(t, first, testUser)
	waitForClients(t, first, 1)
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, compacted)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	// Leaving flushes; the room is dropped from the hub once that is done.
	waitForPending(t, first)
	_ = conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for hub.RoomCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the emptied room was never dropped")
		}
		time.Sleep(10 * time.Millisecond)
	}

	second, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("second GetOrCreateRoom: %v", err)
	}
	if second == first {
		t.Fatal("expected a new room")
	}
	joiner := roomClient(t, second, "8f9c1a2e-3b4d-4c5e-8f60-112233445566")
	if err := joiner.WriteMessage(websocket.BinaryMessage, EncodeSync1(EmptyStateVector())); err != nil {
		t.Fatalf("write sync1: %v", err)
	}
	_ = joiner.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := joiner.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		typ, payload, derr := DecodeMessage(msg)
		if derr != nil || typ != MsgSync {
			continue
		}
		st2, data, derr := DecodeSyncMessage(payload)
		if derr != nil || st2 != SyncStep2 {
			continue
		}
		if !bytes.Equal(data, compacted) {
			t.Fatalf("joiner got %x want the checkpoint %x", data, compacted)
		}
		return
	}
}

// waitForPending blocks until the room holds a checkpoint awaiting its flush.
func waitForPending(t *testing.T, room *Room) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		room.mu.RLock()
		p := room.pending != nil
		room.mu.RUnlock()
		if p {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the room never recorded the checkpoint")
}
