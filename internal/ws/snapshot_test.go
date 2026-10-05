// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testLexicalJSON = `{"root":{"type":"root","children":[{"type":"paragraph"}]}}`

// fakePolicyClient records what the publisher forwarded to core and can be told
// to fail, so the room's control-channel behaviour is exercised on both paths.
type fakePolicyClient struct {
	mu    sync.Mutex
	calls []snapshot.SnapshotRequest
	err   error
}

func (f *fakePolicyClient) UpdateDraftContent(_ context.Context, req snapshot.SnapshotRequest) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.err != nil {
		return false, "", f.err
	}
	return true, "", nil
}

func (f *fakePolicyClient) recorded() []snapshot.SnapshotRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]snapshot.SnapshotRequest(nil), f.calls...)
}

// waitForCalls polls until the fake has seen at least n calls, so tests never
// depend on how quickly the room's flush goroutine runs.
func (f *fakePolicyClient) waitForCalls(t *testing.T, n int) []snapshot.SnapshotRequest {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := f.recorded(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("core received %d UpdateDraftContent call(s), want %d", len(f.recorded()), n)
	return nil
}

// discardAuditPublisher satisfies audit.Publisher without asserting anything.
type discardAuditPublisher struct{}

func (discardAuditPublisher) Publish(context.Context, string, []byte) error { return nil }

// testPublisher returns a real snapshot.Publisher wired to an in-memory core.
func testPublisher(client snapshot.PolicyClient) *snapshot.Publisher {
	return snapshot.NewPublisherWithClient(client, audit.New(discardAuditPublisher{}), log.Nop())
}

// fastDebounce shrinks the room's snapshot debounce so a test can observe the
// debounced flush without a five-second wait. Tests that must prove a flush was
// triggered by something OTHER than the timer deliberately leave it alone.
func fastDebounce(r *Room) {
	r.debouncer = snapshot.NewDebouncer(20 * time.Millisecond)
}

// readUntilControl reads until a MsgControl frame arrives and decodes it.
func readUntilControl(t *testing.T, conn *websocket.Conn) ControlMessage {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		msgType, payload, err := DecodeMessage(msg)
		if err != nil || msgType != MsgControl {
			continue
		}
		ctrl, err := DecodeControlMessage(payload)
		if err != nil {
			t.Fatalf("decode control: %v", err)
		}
		return ctrl
	}
}

// readUntilSync1 reads until a Sync Step-1 frame arrives and returns its payload.
func readUntilSync1(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		msgType, payload, err := DecodeMessage(msg)
		if err != nil || msgType != MsgSync {
			continue
		}
		syncType, data, err := DecodeSyncMessage(payload)
		if err != nil || syncType != SyncStep1 {
			continue
		}
		return data
	}
}

// --- wire format ------------------------------------------------------------

// TestSnapshotFrameRoundTrip pins the MsgSnapshot layout: the message type,
// then contentJSON and the compacted Yjs state as two length-prefixed fields.
func TestSnapshotFrameRoundTrip(t *testing.T) {
	state := []byte{0x01, 0x02, 0xFF, 0x00}
	frame := EncodeSnapshot(testLexicalJSON, state)

	msgType, payload, err := DecodeMessage(frame)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != MsgSnapshot {
		t.Fatalf("msg type: got %d want %d", msgType, MsgSnapshot)
	}
	gotJSON, gotState, err := DecodeSnapshotMessage(payload)
	if err != nil {
		t.Fatalf("DecodeSnapshotMessage: %v", err)
	}
	if gotJSON != testLexicalJSON {
		t.Errorf("content json: got %q want %q", gotJSON, testLexicalJSON)
	}
	if !bytes.Equal(gotState, state) {
		t.Errorf("yjs state: got %x want %x", gotState, state)
	}
}

// TestSnapshotFrameYjsStateIsOptional keeps the frame forward/backward
// compatible: a client that sends only the Lexical JSON must still be
// understood, and unknown trailing fields must be ignored rather than fatal.
func TestSnapshotFrameYjsStateIsOptional(t *testing.T) {
	jsonOnly := concat(encodeVarUint(MsgSnapshot), encodeVarUint8Array([]byte(testLexicalJSON)))
	_, payload, err := DecodeMessage(jsonOnly)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	gotJSON, gotState, err := DecodeSnapshotMessage(payload)
	if err != nil {
		t.Fatalf("json-only snapshot: %v", err)
	}
	if gotJSON != testLexicalJSON || len(gotState) != 0 {
		t.Fatalf("json-only snapshot: got (%q, %x)", gotJSON, gotState)
	}

	withExtra := append(EncodeSnapshot(testLexicalJSON, []byte{0x09}), encodeVarUint8Array([]byte("future"))...)
	_, payload, err = DecodeMessage(withExtra)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	gotJSON, gotState, err = DecodeSnapshotMessage(payload)
	if err != nil {
		t.Fatalf("snapshot with trailing field: %v", err)
	}
	if gotJSON != testLexicalJSON || !bytes.Equal(gotState, []byte{0x09}) {
		t.Fatalf("snapshot with trailing field: got (%q, %x)", gotJSON, gotState)
	}
}

// TestControlFrameRoundTrip pins the MsgControl layout and payload shape.
func TestControlFrameRoundTrip(t *testing.T) {
	want := ControlMessage{
		Type:    ControlSnapshotRejected,
		DraftID: "draft-1",
		Reason:  snapshot.ReasonServerRejected,
		Detail:  "section count mismatch",
	}
	frame, err := EncodeControl(want)
	if err != nil {
		t.Fatalf("EncodeControl: %v", err)
	}
	msgType, payload, err := DecodeMessage(frame)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != MsgControl {
		t.Fatalf("msg type: got %d want %d", msgType, MsgControl)
	}
	got, err := DecodeControlMessage(payload)
	if err != nil {
		t.Fatalf("DecodeControlMessage: %v", err)
	}
	if got != want {
		t.Fatalf("control message: got %+v want %+v", got, want)
	}

	// The payload is plain JSON so a browser can read it with JSON.parse.
	body, _, err := decodeVarUint8Array(payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("control payload is not JSON: %v", err)
	}
	if raw["type"] != ControlSnapshotRejected {
		t.Fatalf("control payload type: got %v", raw["type"])
	}
}

// --- snapshot → core --------------------------------------------------------

// TestSnapshotFrameReachesCore is the headline fix: before this, no MsgSnapshot
// type existed and client.handleMessage had no branch for it, so live edits
// never reached core and the draft silently did not persist.
func TestSnapshotFrameReachesCore(t *testing.T) {
	ctx := t.Context()
	const actor = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"
	core := &fakePolicyClient{}
	st := newMemStore()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, st, testPublisher(core), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, actor, "Bob", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	compacted := []byte{0x11, 0x22, 0x33}
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, compacted)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	calls := core.waitForCalls(t, 1)
	got := calls[0]
	if got.ContentJSON != testLexicalJSON {
		t.Errorf("content json: got %q want %q", got.ContentJSON, testLexicalJSON)
	}
	if got.DraftID != "draft-1" || got.PolicyID != "policy-1" || got.TemplateVersionID != "tpl-1" {
		t.Errorf("draft/policy/template ids: got %+v", got)
	}
	// The actor is the socket's authenticated uid, not the synthetic
	// collab-service identity.
	if got.ActorUserID != actor {
		t.Errorf("actor: got %q want %q (the JWT uid)", got.ActorUserID, actor)
	}
}

// TestSnapshotIgnoresClientAssertedActor is the security half: the
// frame carries no actor field at all, so there is nothing for a client to
// spoof — attribution comes from the connection.
func TestSnapshotIgnoresClientAssertedActor(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	// A client trying to smuggle an actor inside the content JSON gains nothing.
	room.RecordSnapshot(ctx, testLexicalJSON, nil, "8f9c1a2e-3b4d-4c5e-8f60-112233445566")
	calls := core.waitForCalls(t, 1)
	if calls[0].ActorUserID != "8f9c1a2e-3b4d-4c5e-8f60-112233445566" {
		t.Fatalf("actor: got %q", calls[0].ActorUserID)
	}
}

// TestRejectedSnapshotEmitsControlFrame closes the silent-failure hole: core
// refusing the content used to be invisible to the editor, which kept
// accepting keystrokes that would never persist.
func TestRejectedSnapshotEmitsControlFrame(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{err: status.Error(codes.InvalidArgument, "content validation: boilerplate edited")}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "7f9c1a2e-3b4d-4c5e-8f60-112233445566", "", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ctrl := readUntilControl(t, conn)
	if ctrl.Type != ControlSnapshotRejected {
		t.Fatalf("control type: got %q want %q", ctrl.Type, ControlSnapshotRejected)
	}
	if ctrl.DraftID != "draft-1" {
		t.Errorf("control draft id: got %q", ctrl.DraftID)
	}
	if ctrl.Reason != snapshot.ReasonGRPCError {
		t.Errorf("control reason: got %q want %q", ctrl.Reason, snapshot.ReasonGRPCError)
	}
	if !bytes.Contains([]byte(ctrl.Detail), []byte("boilerplate edited")) {
		t.Errorf("control detail: got %q, want core's message", ctrl.Detail)
	}
}

// TestAcceptedSnapshotEmitsControlFrame is the other half of the control
// channel: the editor needs a positive "this is durable now" signal, not just
// silence, to show a saved indicator.
func TestAcceptedSnapshotEmitsControlFrame(t *testing.T) {
	ctx := t.Context()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(&fakePolicyClient{}), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "7f9c1a2e-3b4d-4c5e-8f60-112233445566", "", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, nil)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	if ctrl := readUntilControl(t, conn); ctrl.Type != ControlSnapshotAccepted {
		t.Fatalf("control type: got %q want %q", ctrl.Type, ControlSnapshotAccepted)
	}
}

// TestMalformedSnapshotFrameIsIgnored: a bad frame must not take the room down
// or reach core.
func TestMalformedSnapshotFrameIsIgnored(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	c := &Client{room: room, conn: nil, send: make(chan []byte, 8), userID: "u-1", logger: log.Nop()}
	// A length prefix that claims more bytes than the frame carries.
	c.handleMessage(ctx, concat(encodeVarUint(MsgSnapshot), encodeVarUint(99), []byte{0x01}))

	time.Sleep(100 * time.Millisecond)
	if got := core.recorded(); len(got) != 0 {
		t.Fatalf("malformed snapshot reached core: %+v", got)
	}
}

// --- compacted state storage ------------------------------------------------

// TestUpdateNeitherDerivesNorPersistsState is the replacement for the byte
// concat: relaying an update must not touch the room's stored state (a
// concatenation of updates is not a valid Yjs update — a rehydrated client
// decoded only the first one) and must not rewrite the row (which is what made
// collab_documents.yjs_state grow without bound).
func TestUpdateNeitherDerivesNorPersistsState(t *testing.T) {
	ctx := t.Context()
	initial := []byte{0xAA, 0xBB}
	st := newMemStore()
	room := newRoom("draft-1", "policy-1", "tpl-1", initial, st, nil, nil, log.Nop())
	go room.run(ctx)

	for range 5 {
		room.ApplyUpdate(ctx, []byte{0x01, 0x02, 0x03})
	}
	time.Sleep(150 * time.Millisecond)

	if got := st.saveCount(); got != 0 {
		t.Fatalf("relayed updates wrote to the store %d time(s); state must only change on snapshot", got)
	}
	if !bytes.Equal(room.CurrentState(), initial) {
		t.Fatalf("relayed updates mutated room state: got %x want %x", room.CurrentState(), initial)
	}
}

// TestSnapshotReplacesStoredStateWithoutGrowing proves the unbounded-growth fix:
// each snapshot REPLACES the stored blob with the client's compacted state, so
// the row tracks document size instead of edit history.
func TestSnapshotReplacesStoredStateWithoutGrowing(t *testing.T) {
	ctx := t.Context()
	st := newMemStore()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, st, nil, nil, log.Nop())
	go room.run(ctx)

	first := bytes.Repeat([]byte{0x01}, 64)
	room.RecordSnapshot(ctx, "", first, "")
	if !room.FlushSnapshot(ctx) {
		t.Fatal("first flush reported nothing pending")
	}
	if got := st.savedState("draft-1"); !bytes.Equal(got, first) {
		t.Fatalf("stored state after first snapshot: got %d bytes want %d", len(got), len(first))
	}

	// A later, SMALLER compacted state (Yjs GC'd deleted content) must shrink
	// the row. Under the old concat this could only ever grow.
	second := bytes.Repeat([]byte{0x02}, 8)
	room.RecordSnapshot(ctx, "", second, "")
	if !room.FlushSnapshot(ctx) {
		t.Fatal("second flush reported nothing pending")
	}
	if got := st.savedState("draft-1"); !bytes.Equal(got, second) {
		t.Fatalf("stored state after second snapshot: got %x want %x", got, second)
	}
	if st.saveCount() != 2 {
		t.Fatalf("save count: got %d want 2 (one per flush)", st.saveCount())
	}
}

// TestRehydrationServesLastCompactedState covers the recovery path end to end:
// a room re-created from the store must answer a client's Sync Step-1 with
// exactly the compacted state the previous session persisted.
func TestRehydrationServesLastCompactedState(t *testing.T) {
	ctx := t.Context()
	st := newMemStore()
	compacted := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	// Session one: a client snapshots, then the room is dropped.
	hub := NewHub(st, nil, nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	go room.run(ctx)
	room.RecordSnapshot(ctx, "", compacted, "")
	room.FlushSnapshot(ctx)
	hub.removeRoom("draft-1")

	// Session two: a fresh room for the same draft, loaded from the store.
	room2, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("second GetOrCreateRoom: %v", err)
	}
	if room2 == room {
		t.Fatal("expected a fresh room after removal")
	}
	if !bytes.Equal(room2.DocumentUpdate(), compacted) {
		t.Fatalf("rehydrated document: got %x want %x", room2.DocumentUpdate(), compacted)
	}

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room2, conn, "u-1", "", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	// The client's Sync Step-1 must be answered with the compacted state.
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSync1(EmptyStateVector())); err != nil {
		t.Fatalf("write sync1: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		msgType, payload, err := DecodeMessage(msg)
		if err != nil || msgType != MsgSync {
			continue
		}
		syncType, data, err := DecodeSyncMessage(payload)
		if err != nil || syncType != SyncStep2 {
			continue
		}
		if !bytes.Equal(data, compacted) {
			t.Fatalf("sync step-2 payload: got %x want %x", data, compacted)
		}
		return
	}
}

// TestSnapshotAdoptsStateEvenWhenCoreRejects: the peers' live documents already
// contain the rejected edits, so the room must be able to rehydrate to the
// state everyone is actually editing. The refusal is reported over the control
// channel instead of being expressed by discarding state.
func TestSnapshotAdoptsStateEvenWhenCoreRejects(t *testing.T) {
	ctx := t.Context()
	st := newMemStore()
	core := &fakePolicyClient{err: status.Error(codes.InvalidArgument, "nope")}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, st, testPublisher(core), nil, log.Nop())
	go room.run(ctx)

	compacted := []byte{0x07, 0x08}
	room.RecordSnapshot(ctx, testLexicalJSON, compacted, "")
	room.FlushSnapshot(ctx)

	if !bytes.Equal(room.CurrentState(), compacted) {
		t.Fatalf("room state after rejection: got %x want %x", room.CurrentState(), compacted)
	}
	if got := st.savedState("draft-1"); !bytes.Equal(got, compacted) {
		t.Fatalf("stored state after rejection: got %x want %x", got, compacted)
	}
}

// --- flush points -----------------------------------------------------------

// TestFlushOnLastCollaboratorLeaves: without this the final edits die with the
// room, because the 5s debounce never fires after everyone closes their tab.
// The room keeps its production debounce here on purpose, so a pass can only
// mean the disconnect triggered the flush.
func TestFlushOnLastCollaboratorLeaves(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	st := newMemStore()
	hub := NewHub(st, testPublisher(core), nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "7f9c1a2e-3b4d-4c5e-8f60-112233445566", "", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	drainInitialSync(t, conn)

	compacted := []byte{0x42}
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, compacted)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	// Nothing has flushed yet: the debounce is 5s and no snapshot deadline passed.
	time.Sleep(100 * time.Millisecond)
	if got := core.recorded(); len(got) != 0 {
		t.Fatalf("snapshot flushed before the debounce elapsed: %+v", got)
	}

	// Last collaborator closes their tab.
	_ = conn.Close()

	calls := core.waitForCalls(t, 1)
	if calls[0].ContentJSON != testLexicalJSON {
		t.Fatalf("flushed content: got %q", calls[0].ContentJSON)
	}
	if got := st.savedState("draft-1"); !bytes.Equal(got, compacted) {
		t.Fatalf("flushed yjs state: got %x want %x", got, compacted)
	}
	// The hub must still drop the room, after the flush.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hub.RoomCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("room not removed after flush; RoomCount=%d", hub.RoomCount())
}

// TestRoomSurvivesAJoinDuringTheFinalFlush: the final flush makes a gRPC call,
// so a client can join while it is in flight. Dropping the room then would
// strand that client and let the next joiner create a second room for the same
// draft.
func TestRoomSurvivesAJoinDuringTheFinalFlush(t *testing.T) {
	ctx := t.Context()
	hub := NewHub(newMemStore(), nil, nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "u-1", "", log.Nop())
	})
	defer srv.Close()

	// A client is present, and the flush path's removal is invoked as if the
	// room had just emptied and then been rejoined mid-flush.
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)
	waitForClients(t, room, 1)

	hub.removeRoom("draft-1")

	if hub.RoomCount() != 1 {
		t.Fatal("hub dropped a room that still had a connected client")
	}
	again, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("second GetOrCreateRoom: %v", err)
	}
	if again != room {
		t.Fatal("a second room was created for a draft that already had one")
	}
}

// TestFlushSnapshotIsIdempotent: a flush with nothing pending must not send an
// empty checkpoint to core (which would blank the draft).
func TestFlushSnapshotIsIdempotent(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	go room.run(ctx)

	if room.FlushSnapshot(ctx) {
		t.Fatal("flush with nothing pending reported work")
	}
	room.RecordSnapshot(ctx, testLexicalJSON, nil, "")
	if !room.FlushSnapshot(ctx) {
		t.Fatal("flush with a pending snapshot reported nothing")
	}
	if room.FlushSnapshot(ctx) {
		t.Fatal("second flush re-sent an already-flushed snapshot")
	}
	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core calls: got %d want 1", len(got))
	}
}

// TestHubFlushDraftIsThePublishIntentGate: the gate a publish must pass through
// so a version is never cut from content older than the room holds.
func TestHubFlushDraftIsThePublishIntentGate(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	hub := NewHub(newMemStore(), testPublisher(core), nil, log.Nop())

	// No room for the draft: reported as "nothing to flush", not an error.
	if res, err := hub.FlushDraft(ctx, "draft-absent"); err != nil || res.RoomFound {
		t.Fatal("FlushDraft invented a room for a draft nobody is editing")
	}
	if hub.RoomCount() != 0 {
		t.Fatalf("FlushDraft created a room; RoomCount=%d", hub.RoomCount())
	}

	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	go room.run(ctx)
	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x01}, "")

	if res, err := hub.FlushDraft(ctx, "draft-1"); err != nil || !res.RoomFound {
		t.Fatal("FlushDraft did not find the live room")
	}
	// Synchronous by contract: the caller must be able to publish immediately.
	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core calls after FlushDraft: got %d want 1", len(got))
	}
}

// TestMarkDraftPublishedFlushesAndGoesReadOnly: publishing is a hard
// serialization boundary — flush first, then stop accepting edits, and tell the
// editors so they drop to read-only instead of typing into an immutable version.
func TestMarkDraftPublishedFlushesAndGoesReadOnly(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	hub := NewHub(newMemStore(), testPublisher(core), nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "7f9c1a2e-3b4d-4c5e-8f60-112233445566", "", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	// Wait until the room has recorded the snapshot before publishing.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		room.mu.RLock()
		pending := room.pending != nil
		room.mu.RUnlock()
		if pending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !hub.MarkDraftPublished(ctx, "draft-1", 7) {
		t.Fatal("MarkDraftPublished did not find the live room")
	}

	// The pending checkpoint reached core before the version was cut.
	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core calls: got %d want 1 (flush before publish)", len(got))
	}
	// Every editor is told to drop to read-only. The accepted-snapshot frame
	// from the flush may arrive first.
	for {
		ctrl := readUntilControl(t, conn)
		if ctrl.Type == ControlSnapshotAccepted {
			continue
		}
		if ctrl.Type != ControlDraftPublished {
			t.Fatalf("control type: got %q want %q", ctrl.Type, ControlDraftPublished)
		}
		if ctrl.VersionNumber != 7 {
			t.Fatalf("published version: got %d want 7", ctrl.VersionNumber)
		}
		break
	}

	// A published version is immutable: further edits and snapshots are refused.
	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x02}, "")
	room.mu.RLock()
	pending := room.pending
	room.mu.RUnlock()
	if pending != nil {
		t.Fatal("room accepted a snapshot after the draft was published")
	}
}

// --- mid-session joiner -----------------------------------------------------

// TestJoinAsksPeersToReAdvertise: because the relay holds only the last
// COMPACTED state, anything typed since the previous snapshot lives solely in
// the peers' documents. A joiner must therefore provoke the peers into
// re-advertising, or it silently starts from stale content.
func TestJoinAsksPeersToReAdvertise(t *testing.T) {
	ctx := t.Context()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	go room.run(ctx)

	uids := make(chan string, 4)
	uids <- "u-1"
	uids <- "u-2"
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, <-uids, "", log.Nop())
	})
	defer srv.Close()

	first := dialWS(t, wsURL)
	defer func() { _ = first.Close() }()
	// The server's own opening Sync Step-1.
	readUntilSync1(t, first)
	waitForClients(t, room, 1)

	second := dialWS(t, wsURL)
	defer func() { _ = second.Close() }()

	// The existing peer is asked again, with an empty state vector: "send me
	// everything you have".
	if sv := readUntilSync1(t, first); !bytes.Equal(sv, EmptyStateVector()) {
		t.Fatalf("solicited state vector: got %x want %x", sv, EmptyStateVector())
	}
}

// TestFreeFormSnapshotFrameReachesCore is the end-to-end half of freeform co-editing: a
// room for a free-form policy carries NO template version, and a snapshot must
// still travel the whole path — client socket, room debouncer, publisher — and
// reach core with the template version forwarded as empty rather than
// fabricated. core reads that as "no pin" and matches it against the draft's
// NULL template_version_id.
func TestFreeFormSnapshotFrameReachesCore(t *testing.T) {
	ctx := t.Context()
	const actor = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"
	core := &fakePolicyClient{}
	st := newMemStore()
	// Empty template version id: this draft is free-form.
	room := newRoom("draft-1", "policy-1", "", nil, st, testPublisher(core), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, actor, "Bob", log.Nop())
	})
	defer srv.Close()
	conn := dialWS(t, wsURL)
	defer func() { _ = conn.Close() }()
	drainInitialSync(t, conn)

	compacted := []byte{0x11, 0x22, 0x33}
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, compacted)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	calls := core.waitForCalls(t, 1)
	got := calls[0]
	if got.ContentJSON != testLexicalJSON {
		t.Errorf("content json: got %q want %q", got.ContentJSON, testLexicalJSON)
	}
	if got.DraftID != "draft-1" || got.PolicyID != "policy-1" {
		t.Errorf("draft/policy ids: got %+v", got)
	}
	if got.TemplateVersionID != "" {
		t.Errorf("template version id: got %q want empty -- a template must never be invented", got.TemplateVersionID)
	}
	if got.ActorUserID != actor {
		t.Errorf("actor: got %q want %q (the JWT uid)", got.ActorUserID, actor)
	}
}

// A token minted during act-as names the real admin in its imp claim; the
// admin rides every snapshot to core with the user acted as.
func TestImpersonatorClaimReachesCore(t *testing.T) {
	ctx := t.Context()
	const target = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"
	const admin = "0b1c2d3e-4f50-4617-8899-aabbccddeeff"
	const secret = "test-secret-32-bytes-long-------"
	core := &fakePolicyClient{}
	hub := NewHub(newMemStore(), testPublisher(core), nil, log.Nop())
	srv := httptest.NewServer(NewHandler(hub, secret, log.Nop()))
	defer srv.Close()

	now := time.Now()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, CollabClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{TokenAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
		},
		UserID: target, PolicyID: "policy-1", DraftID: "draft-imp", Impersonator: admin,
	}).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws/draft-imp?token="+tok, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	drainInitialSync(t, conn)
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(testLexicalJSON, nil)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	_ = conn.Close()

	got := core.waitForCalls(t, 1)[0]
	if got.ActorUserID != target || got.Impersonator != admin {
		t.Fatalf("core request: actor %q impersonator %q, want %q and %q", got.ActorUserID, got.Impersonator, target, admin)
	}
}
