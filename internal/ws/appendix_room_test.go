// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

// These tests hold an appendix room to the same answers as a policy room: the
// cases of steward-mock-server's src/product/appendix-collab.test.ts that are
// about the room. The cases that are about core's draft (it becomes the draft,
// it updates the draft, nothing when equal to the published row) are core's;
// here they are the request core receives.

const (
	appendixKey = "appendix:appendix-1"
	appendixDoc = `{"root":{"type":"root","children":[{"type":"paragraph","children":[{"type":"text","text":"Written in the room"}]}]}}`
)

// fakeAppendixCore records what an appendix room sent to core. A non-nil hold
// keeps each call open until it is closed.
type fakeAppendixCore struct {
	mu         sync.Mutex
	calls      []snapshot.AppendixRequest
	err        error
	rejectWith string
	hold       chan struct{}
}

func (f *fakeAppendixCore) CheckpointAppendixDraft(_ context.Context, req snapshot.AppendixRequest) (bool, string, error) {
	if f.hold != nil {
		<-f.hold
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.err != nil {
		return false, "", f.err
	}
	if f.rejectWith != "" {
		return false, f.rejectWith, nil
	}
	return true, "", nil
}

func (f *fakeAppendixCore) recorded() []snapshot.AppendixRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]snapshot.AppendixRequest(nil), f.calls...)
}

func (f *fakeAppendixCore) waitForCalls(t *testing.T, n int) []snapshot.AppendixRequest {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := f.recorded(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("core received %d CheckpointAppendixDraft call(s), want %d", len(f.recorded()), n)
	return nil
}

func appendixPublisher(policy snapshot.PolicyClient, appendix snapshot.AppendixClient) *snapshot.Publisher {
	return snapshot.NewPublisherWithClient(policy, audit.New(discardAuditPublisher{}), log.Nop()).WithAppendixClient(appendix)
}

// appendixRoom is a running appendix room on a fast debounce.
func appendixRoom(t *testing.T, policy *fakePolicyClient, appendix *fakeAppendixCore) *Room {
	t.Helper()
	room := newRoom(appendixKey, "policy-1", "", nil, newMemStore(), appendixPublisher(policy, appendix), nil, log.Nop())
	fastDebounce(room)
	go room.run(t.Context())
	return room
}

// Mock "becomes the appendix's draft": the room hands core the appendix, not
// the room key, with the policy, the content and the token's person.
func TestAppendixRoomCheckpointReachesTheAppendixCallAsAccepted(t *testing.T) {
	policy, core := &fakePolicyClient{}, &fakeAppendixCore{}
	room := appendixRoom(t, policy, core)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(appendixDoc, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ctrl := readUntilControl(t, conn)
	if ctrl.Type != ControlSnapshotAccepted || ctrl.DraftID != appendixKey {
		t.Fatalf("control: got %+v want snapshot.accepted for %s", ctrl, appendixKey)
	}
	got := core.waitForCalls(t, 1)[0]
	want := snapshot.AppendixRequest{AppendixID: "appendix-1", PolicyID: "policy-1", ContentJSON: appendixDoc, ActorUserID: testUser}
	if got != want {
		t.Fatalf("request: got %+v want %+v", got, want)
	}
	if n := len(policy.recorded()); n != 0 {
		t.Fatalf("an appendix checkpoint reached the policy draft call %d time(s)", n)
	}
}

// Mock "updates the draft that is already there": each flush is one call with
// the newest content, so core's one-draft rule is what the second one meets.
func TestAppendixRoomSecondCheckpointIsAnotherCallWithTheNewestContent(t *testing.T) {
	core := &fakeAppendixCore{}
	room := appendixRoom(t, &fakePolicyClient{}, core)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	for _, words := range []string{"First", "Second"} {
		doc := `{"root":{"type":"root","children":[{"text":"` + words + `"}]}}`
		if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(doc, []byte{0x01})); err != nil {
			t.Fatalf("write snapshot: %v", err)
		}
		if ctrl := readUntilControl(t, conn); ctrl.Type != ControlSnapshotAccepted {
			t.Fatalf("control: %+v", ctrl)
		}
	}
	calls := core.waitForCalls(t, 2)
	if calls[1].ContentJSON != `{"root":{"type":"root","children":[{"text":"Second"}]}}` || calls[1].AppendixID != "appendix-1" {
		t.Fatalf("second call: %+v", calls[1])
	}
}

// Mock "is refused when the content is not a JSON document": at once, with the
// policy room's reason, and nothing is held, stored or sent.
func TestAppendixRoomRefusesNonJSONAtOnce(t *testing.T) {
	ctx := t.Context()
	core, st := &fakeAppendixCore{}, newMemStore()
	// The production debounce stays: an answer inside readUntilControl's budget
	// can only be an immediate one.
	room := newRoom(appendixKey, "policy-1", "", nil, st, appendixPublisher(&fakePolicyClient{}, core), nil, log.Nop())
	go room.run(ctx)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot("not json", []byte{0x09})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ctrl := readUntilControl(t, conn)
	if ctrl.Type != ControlSnapshotRejected || ctrl.Reason != snapshot.ReasonPreCheckFailed || ctrl.DraftID != appendixKey || ctrl.Detail == "" {
		t.Fatalf("control: got %+v", ctrl)
	}
	room.FlushSnapshot(ctx)
	if got := core.recorded(); len(got) != 0 {
		t.Errorf("a refused checkpoint reached core: %+v", got)
	}
	if st.saveCount() != 0 || len(room.CurrentState()) != 0 {
		t.Errorf("a refused checkpoint was kept (%d saves, state %x)", st.saveCount(), room.CurrentState())
	}
}

// Core's refusal (a retired appendix, content it will not take) is the editors'
// snapshot.rejected with server_rejected and core's reason.
func TestAppendixRoomTellsEditorsWhenCoreRefuses(t *testing.T) {
	core := &fakeAppendixCore{rejectWith: "appendix is retired"}
	room := appendixRoom(t, &fakePolicyClient{}, core)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(appendixDoc, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ctrl := readUntilControl(t, conn)
	if ctrl.Type != ControlSnapshotRejected || ctrl.Reason != snapshot.ReasonServerRejected || ctrl.Detail != "appendix is retired" {
		t.Fatalf("control: got %+v", ctrl)
	}
}

func TestAppendixRoomTellsEditorsWhenTheCallFails(t *testing.T) {
	core := &fakeAppendixCore{err: status.Error(codes.InvalidArgument, "content validation: bad node")}
	room := appendixRoom(t, &fakePolicyClient{}, core)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(appendixDoc, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ctrl := readUntilControl(t, conn)
	if ctrl.Type != ControlSnapshotRejected || ctrl.Reason != snapshot.ReasonGRPCError {
		t.Fatalf("control: got %+v", ctrl)
	}
}

// Late ack: snapshot.accepted waits for core, as in a policy room.
func TestAppendixRoomAcknowledgesOnlyAfterCoreAnswers(t *testing.T) {
	core := &fakeAppendixCore{hold: make(chan struct{})}
	room := appendixRoom(t, &fakePolicyClient{}, core)
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(appendixDoc, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	waitForPending(t, room)

	// One reader owns the socket: a read deadline that expires poisons the
	// connection, so "nothing yet" is a timer on a channel, not on the read.
	controls := make(chan ControlMessage, 4)
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if typ, payload, derr := DecodeMessage(msg); derr == nil && typ == MsgControl {
				if ctrl, cerr := DecodeControlMessage(payload); cerr == nil {
					controls <- ctrl
				}
			}
		}
	}()
	select {
	case ctrl := <-controls:
		t.Fatalf("%s was sent before core answered", ctrl.Type)
	case <-time.After(300 * time.Millisecond): // well past the 20ms test debounce, with core still holding
	}

	close(core.hold)
	select {
	case ctrl := <-controls:
		if ctrl.Type != ControlSnapshotAccepted {
			t.Fatalf("control: got %+v want snapshot.accepted once core answered", ctrl)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no answer after core replied")
	}
}

// A checkpoint that carries only the Yjs state is stored; nothing goes to core
// and no control frame comes back.
func TestAppendixRoomYjsOnlyCheckpointGetsNoReply(t *testing.T) {
	core, st := &fakeAppendixCore{}, newMemStore()
	room := newRoom(appendixKey, "policy-1", "", nil, st, appendixPublisher(&fakePolicyClient{}, core), nil, log.Nop())
	fastDebounce(room)
	go room.run(t.Context())
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)

	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot("", []byte{0x05, 0x06})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if got := core.recorded(); len(got) != 0 {
		t.Fatalf("a Yjs-only checkpoint reached core: %+v", got)
	}
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if typ, _, derr := DecodeMessage(msg); derr == nil && typ == MsgControl {
			t.Fatal("a Yjs-only checkpoint got a control frame")
		}
	}
	if doc, _ := st.Load(t.Context(), appendixKey); doc == nil || len(doc.YjsState) != 2 {
		t.Fatalf("the Yjs state was not stored under the room key: %+v", doc)
	}
}

// Mock "tells the room who a person is by name, whatever their browser sent".
func TestAppendixRoomPresenceCarriesTheTokenName(t *testing.T) {
	ctx := t.Context()
	room := newRoom(appendixKey, "policy-1", "", nil, newMemStore(), nil, nil, log.Nop())
	go room.run(ctx)
	uids := make(chan string, 2)
	uids <- "user-bob"
	uids <- "user-alice"
	names := map[string]string{"user-bob": "Bob Author", "user-alice": "Alice"}
	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		uid := <-uids
		NewClient(ctx, room, conn, uid, names[uid], log.Nop())
	})
	defer srv.Close()
	bob := dialWS(t, wsURL)
	defer func() { _ = bob.Close() }()
	drainInitialSync(t, bob)
	alice := dialWS(t, wsURL)
	defer func() { _ = alice.Close() }()
	drainInitialSync(t, alice)
	waitForClients(t, room, 2)

	if err := bob.WriteMessage(websocket.BinaryMessage,
		awarenessFrameFor(7, 3, `{"activity":"typing","user":{"name":"Owl"}}`)); err != nil {
		t.Fatalf("write awareness: %v", err)
	}

	entries := readAwarenessStates(t, alice)
	var state struct {
		Activity string        `json:"activity"`
		User     awarenessUser `json:"user"`
	}
	if err := json.Unmarshal(entries[0].State, &state); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if state.User.Name != "Bob Author" || state.User.UID != "user-bob" || state.Activity != "typing" {
		t.Fatalf("presence: %+v", state)
	}
}

// Flush parity: the gateway's FlushDraft for an appendix reaches core's
// CheckpointAppendixDraft and returns once core has answered.
func TestAppendixHubFlushIsThePublishIntentGate(t *testing.T) {
	ctx := t.Context()
	policy, core := &fakePolicyClient{}, &fakeAppendixCore{}
	hub := NewHub(newMemStore(), appendixPublisher(policy, core), nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, appendixKey, "policy-1", "")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	go room.run(ctx)
	room.RecordSnapshot(ctx, appendixDoc, []byte{0x01}, "")

	res, err := hub.FlushDraft(ctx, appendixKey)
	if err != nil || !res.RoomFound || !res.ContentFlushed {
		t.Fatalf("FlushDraft: %+v %v", res, err)
	}
	if got := core.recorded(); len(got) != 1 || got[0].AppendixID != "appendix-1" {
		t.Fatalf("core calls after FlushDraft: %+v", got)
	}
	if len(policy.recorded()) != 0 {
		t.Fatal("the flush reached the policy draft call")
	}
}

func TestAppendixHubFlushReportsACoreRefusal(t *testing.T) {
	ctx := t.Context()
	core := &fakeAppendixCore{rejectWith: "appendix is retired"}
	hub := NewHub(newMemStore(), appendixPublisher(&fakePolicyClient{}, core), nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, appendixKey, "policy-1", "")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	go room.run(ctx)
	room.RecordSnapshot(ctx, appendixDoc, []byte{0x01}, "")

	_, err = hub.FlushDraft(ctx, appendixKey)
	fe, ok := err.(*FlushError)
	if !ok || fe.Reason != snapshot.ReasonServerRejected || fe.Detail != "appendix is retired" {
		t.Fatalf("FlushDraft error: %#v", err)
	}
}

// Notify parity: publishing the appendix flushes, then freezes the room.
func TestAppendixHubMarkPublishedFlushesAndFreezes(t *testing.T) {
	ctx := t.Context()
	core := &fakeAppendixCore{}
	hub := NewHub(newMemStore(), appendixPublisher(&fakePolicyClient{}, core), nil, log.Nop())
	room, err := hub.GetOrCreateRoom(ctx, appendixKey, "policy-1", "")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	conn := roomClient(t, room, testUser)
	waitForClients(t, room, 1)
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(appendixDoc, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	waitForPending(t, room)

	if !hub.MarkDraftPublished(ctx, appendixKey, 4) {
		t.Fatal("MarkDraftPublished did not find the appendix room")
	}

	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core calls: got %d want 1 (flush before publish)", len(got))
	}
	for {
		ctrl := readUntilControl(t, conn)
		if ctrl.Type == ControlSnapshotAccepted {
			continue
		}
		if ctrl.Type != ControlDraftPublished || ctrl.VersionNumber != 4 || ctrl.DraftID != appendixKey {
			t.Fatalf("control: %+v", ctrl)
		}
		break
	}
	// A checkpoint on the frozen room is refused, as in a policy room.
	if err := conn.WriteMessage(websocket.BinaryMessage, EncodeSnapshot(appendixDoc, []byte{0x02})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if ctrl := readUntilControl(t, conn); ctrl.Type != ControlSnapshotRejected || ctrl.Reason != snapshot.ReasonServerRejected {
		t.Fatalf("control: %+v", ctrl)
	}
}
