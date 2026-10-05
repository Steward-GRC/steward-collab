// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"encoding/json"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
)

// awarenessFrameFor builds a MsgAwareness frame the way a Yjs client would, for
// a single client id carrying the given state JSON.
func awarenessFrameFor(clientID, clock uint32, stateJSON string) []byte {
	return EncodeAwareness(encodeAwarenessUpdate([]awarenessEntry{
		{ClientID: clientID, Clock: clock, State: []byte(stateJSON)},
	}))
}

// readAwarenessStates reads until an awareness frame arrives and returns its
// decoded entries.
func readAwarenessStates(t *testing.T, conn *websocket.Conn) []awarenessEntry {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		msgType, payload, err := DecodeMessage(msg)
		if err != nil || msgType != MsgAwareness {
			continue
		}
		body, err := DecodeAwarenessMessage(payload)
		if err != nil {
			t.Fatalf("decode awareness message: %v", err)
		}
		entries, err := decodeAwarenessUpdate(body)
		if err != nil {
			t.Fatalf("decode awareness update: %v", err)
		}
		return entries
	}
}

// TestQueryAwarenessAnswersWithRoomPresence is the joiner case: a y-websocket
// client sends MsgQueryAwareness on connect and must be told who is already in
// the room, instead of being met with silence until an existing peer moves.
func TestQueryAwarenessAnswersWithRoomPresence(t *testing.T) {
	ctx := t.Context()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	go room.run(ctx)

	uids := make(chan string, 4)
	uids <- "alice"
	uids <- "bob"
	names := map[string]string{"alice": "Alice", "bob": "Dave"}

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		uid := <-uids
		NewClient(ctx, room, conn, uid, names[uid], log.Nop())
	})
	defer srv.Close()

	// Alice joins and announces her presence.
	alice := dialWS(t, wsURL)
	defer func() { _ = alice.Close() }()
	drainInitialSync(t, alice)
	if err := alice.WriteMessage(websocket.BinaryMessage,
		awarenessFrameFor(101, 1, `{"user":{"uid":"nope","name":"nope","color":"#000"},"cursor":{"anchor":0}}`)); err != nil {
		t.Fatalf("alice awareness write: %v", err)
	}
	// Alice sees her own bound presence fanned back out, which also tells us the
	// room has recorded it before Bob asks.
	if got := readAwarenessStates(t, alice); len(got) != 1 || got[0].ClientID != 101 {
		t.Fatalf("alice presence fan-out: %+v", got)
	}

	// Bob joins and queries.
	bob := dialWS(t, wsURL)
	defer func() { _ = bob.Close() }()
	drainInitialSync(t, bob)
	if err := bob.WriteMessage(websocket.BinaryMessage, encodeVarUint(MsgQueryAwareness)); err != nil {
		t.Fatalf("bob query write: %v", err)
	}

	entries := readAwarenessStates(t, bob)
	if len(entries) != 1 {
		t.Fatalf("query reply entries: got %d want 1 (%+v)", len(entries), entries)
	}
	if entries[0].ClientID != 101 || entries[0].Clock != 1 {
		t.Fatalf("query reply entry: got client %d clock %d want 101/1", entries[0].ClientID, entries[0].Clock)
	}

	var state struct {
		User awarenessUser `json:"user"`
	}
	if err := json.Unmarshal(entries[0].State, &state); err != nil {
		t.Fatalf("unmarshal queried state: %v", err)
	}
	if state.User.UID != "alice" {
		t.Fatalf("queried uid: got %q want %q", state.User.UID, "alice")
	}
	// Fix 6: presence carries the token display name, not the raw uid.
	if state.User.Name != "Alice" {
		t.Fatalf("queried name: got %q want %q", state.User.Name, "Alice")
	}
}

// TestQueryAwarenessOnEmptyRoomIsSilent confirms an empty room sends nothing
// back rather than an awareness frame claiming zero clients.
func TestQueryAwarenessOnEmptyRoomIsSilent(t *testing.T) {
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	if frame := room.AwarenessState(); frame != nil {
		t.Fatalf("empty room returned a presence frame: %x", frame)
	}
}

// TestPresenceMapRespectsClocksAndClears covers the room's presence bookkeeping:
// newer clocks win, stale clocks are ignored, and a cleared state removes the
// peer entirely.
func TestPresenceMapRespectsClocksAndClears(t *testing.T) {
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())

	room.recordAwareness([]awarenessEntry{{ClientID: 7, Clock: 5, State: []byte(`{"a":1}`)}})
	room.recordAwareness([]awarenessEntry{{ClientID: 7, Clock: 2, State: []byte(`{"a":"stale"}`)}})
	entries := decodeFrame(t, room.AwarenessState())
	if len(entries) != 1 || entries[0].Clock != 5 || string(entries[0].State) != `{"a":1}` {
		t.Fatalf("stale clock overwrote current presence: %+v", entries)
	}

	room.recordAwareness([]awarenessEntry{{ClientID: 7, Clock: 6, State: []byte(awarenessStateCleared)}})
	if frame := room.AwarenessState(); frame != nil {
		t.Fatalf("cleared peer still present: %x", frame)
	}
}

// TestReleaseAwarenessBroadcastsClearedState checks that a disconnecting peer's
// presence is actively cleared for everyone else, rather than lingering in the
// room map until the client-side timeout.
func TestReleaseAwarenessBroadcastsClearedState(t *testing.T) {
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	room.recordAwareness([]awarenessEntry{{ClientID: 9, Clock: 3, State: []byte(`{"a":1}`)}})

	room.releaseAwareness(map[uint32]struct{}{9: {}})

	if frame := room.AwarenessState(); frame != nil {
		t.Fatalf("released peer still in presence map: %x", frame)
	}
	select {
	case frame := <-room.awareness:
		entries := decodeFrame(t, frame)
		if len(entries) != 1 {
			t.Fatalf("cleared broadcast entries: got %d want 1", len(entries))
		}
		if !entries[0].cleared() {
			t.Fatalf("broadcast state is not cleared: %s", entries[0].State)
		}
		// The clock must advance or peers discard the clear as stale.
		if entries[0].Clock != 4 {
			t.Fatalf("cleared clock: got %d want 4", entries[0].Clock)
		}
	default:
		t.Fatal("no cleared-presence frame was broadcast")
	}
}

// decodeFrame unwraps a MsgAwareness frame into entries.
func decodeFrame(t *testing.T, frame []byte) []awarenessEntry {
	t.Helper()
	if frame == nil {
		t.Fatal("nil awareness frame")
	}
	msgType, payload, err := DecodeMessage(frame)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != MsgAwareness {
		t.Fatalf("msgType: got %d want %d", msgType, MsgAwareness)
	}
	body, err := DecodeAwarenessMessage(payload)
	if err != nil {
		t.Fatalf("DecodeAwarenessMessage: %v", err)
	}
	entries, err := decodeAwarenessUpdate(body)
	if err != nil {
		t.Fatalf("decodeAwarenessUpdate: %v", err)
	}
	return entries
}

// TestPresenceFallsBackToUIDWithoutDisplayName keeps the pre-display-name
// behaviour for tokens minted without a `name` claim.
func TestPresenceFallsBackToUIDWithoutDisplayName(t *testing.T) {
	u := newAwarenessUser("u-42", "")
	if u.Name != "u-42" {
		t.Fatalf("name fallback: got %q want %q", u.Name, "u-42")
	}
	if named := newAwarenessUser("u-42", "Carol"); named.Name != "Carol" {
		t.Fatalf("display name ignored: got %q", named.Name)
	}
}
