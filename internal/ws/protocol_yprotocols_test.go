// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	log "github.com/Bugs5382/go-log"
)

// This file is the wire-compatibility contract with the real Yjs stack. Every
// fixture below was produced by the ACTUAL JavaScript packages a browser client
// uses — yjs 13.6.32, y-protocols 1.0.7, lib0 0.2.117 — not by this package.
// Regenerate with test/wire/gen.mjs (see test/wire/README.md).
//
// This matters because a Go↔Go round-trip test cannot detect a framing bug:
// a self-consistent encoder/decoder pair passes it happily while being
// unreadable to every real client. The pre-existing prefix-less framing was
// exactly that, and it failed here in both directions:
//
//   - decode: real SyncStep1 `00000501d6e8480c` yielded the data
//     `0501d6e8480c` — the varUint8Array length byte 0x05 leaked into the
//     payload and would have been merged into the document as content.
//   - decode: real awareness frames were REJECTED outright
//     ("awareness: rebind state: invalid character 'a'"), because the length
//     prefix was read as the client count.
//   - encode: feeding the Go frames to y-protocols' readSyncMessage /
//     applyAwarenessUpdate threw "Unexpected end of array" for sync step 1/2
//     and awareness, and for update frames Yjs SWALLOWED the error
//     ("Caught error while handling a Yjs update") and silently dropped the
//     edit.
//
// Fixture doc: a Y.Doc with clientID 1193046 whose Y.Text "root" is
// "hello collab" (so its state vector is client 1193046 @ clock 12).
const (
	// Y.encodeStateVector(doc)
	realStateVector = "01d6e8480c"
	// Y.encodeStateVector(new Y.Doc()) — zero clients.
	realEmptyStateVector = "00"
	// Y.encodeStateAsUpdate(doc)
	realFullUpdate = "0101d6e84800040104726f6f740c68656c6c6f20636f6c6c616200"

	// messageSync + syncProtocol.writeSyncStep1(encoder, doc)
	realSyncStep1 = "00000501d6e8480c"
	// messageSync + syncProtocol.writeSyncStep1(encoder, new Y.Doc())
	realSyncStep1Empty = "00000100"
	// messageSync + syncProtocol.writeSyncStep2(encoder, doc, emptyStateVector)
	realSyncStep2 = "00011b0101d6e84800040104726f6f740c68656c6c6f20636f6c6c616200"
	// messageSync + syncProtocol.writeUpdate(encoder, update) for a one-char insert
	realSyncUpdate      = "00020e01010700040104726f6f74017800"
	realSyncUpdateInner = "01010700040104726f6f74017800"

	// messageAwareness + writeVarUint8Array(encodeAwarenessUpdate(awareness, [clientID]))
	// for a peer asserting user {uid: spoofed-uid, name: Impostor, color: #000000}
	// plus a cursor. Awareness clock is 2.
	realAwareness      = "016701d6e84802617b2275736572223a7b22756964223a2273706f6f6665642d756964222c226e616d65223a22496d706f73746f72222c22636f6c6f72223a2223303030303030227d2c22637572736f72223a7b22616e63686f72223a312c2268656164223a347d7d"
	realAwarenessInner = "01d6e84802617b2275736572223a7b22756964223a2273706f6f6665642d756964222c226e616d65223a22496d706f73746f72222c22636f6c6f72223a2223303030303030227d2c22637572736f72223a7b22616e63686f72223a312c2268656164223a347d7d"

	// messageQueryAwareness — a bare message type, no payload.
	realQueryAwareness = "03"

	// realFixtureClientID / realFixtureAwarenessClock describe the fixture peer.
	realFixtureClientID       = 1193046
	realFixtureAwarenessClock = 2
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad fixture hex %q: %v", s, err)
	}
	return b
}

// TestEncodeMatchesRealYProtocolsBytes is the strongest available assertion:
// each Encode* helper must produce byte-for-byte the frame that the real
// y-protocols encoder produced for the same payload.
func TestEncodeMatchesRealYProtocolsBytes(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"sync step 1 (state vector)", EncodeSync1(mustHex(t, realStateVector)), realSyncStep1},
		{"sync step 1 (empty state vector)", EncodeSync1(EmptyStateVector()), realSyncStep1Empty},
		{"sync step 2 (full update)", EncodeSync2(mustHex(t, realFullUpdate)), realSyncStep2},
		{"sync update", EncodeUpdate(mustHex(t, realSyncUpdateInner)), realSyncUpdate},
		{"awareness", EncodeAwareness(mustHex(t, realAwarenessInner)), realAwareness},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hex.EncodeToString(tc.got); got != tc.want {
				t.Fatalf("frame is not what y-protocols emits\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestEmptyStateVectorMatchesYjs pins EmptyStateVector() to the bytes
// Y.encodeStateVector(new Y.Doc()) actually produces.
func TestEmptyStateVectorMatchesYjs(t *testing.T) {
	if got := hex.EncodeToString(EmptyStateVector()); got != realEmptyStateVector {
		t.Fatalf("EmptyStateVector: got %s want %s", got, realEmptyStateVector)
	}
}

// TestDecodeRealYProtocolsSyncFrames runs frames built by y-protocols through
// the decoder and requires the payload to come out clean — no leaked length
// prefix.
func TestDecodeRealYProtocolsSyncFrames(t *testing.T) {
	cases := []struct {
		name         string
		frame        string
		wantSyncType uint32
		wantData     string
	}{
		{"step 1", realSyncStep1, SyncStep1, realStateVector},
		{"step 1 empty", realSyncStep1Empty, SyncStep1, realEmptyStateVector},
		{"step 2", realSyncStep2, SyncStep2, realFullUpdate},
		{"update", realSyncUpdate, SyncUpdate, realSyncUpdateInner},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgType, payload, err := DecodeMessage(mustHex(t, tc.frame))
			if err != nil {
				t.Fatalf("DecodeMessage: %v", err)
			}
			if msgType != MsgSync {
				t.Fatalf("msgType: got %d want %d", msgType, MsgSync)
			}
			syncType, data, err := DecodeSyncMessage(payload)
			if err != nil {
				t.Fatalf("DecodeSyncMessage: %v", err)
			}
			if syncType != tc.wantSyncType {
				t.Fatalf("syncType: got %d want %d", syncType, tc.wantSyncType)
			}
			if got := hex.EncodeToString(data); got != tc.wantData {
				t.Fatalf("payload leaked framing bytes\n got: %s\nwant: %s", got, tc.wantData)
			}
		})
	}
}

// TestDecodeRealYProtocolsAwareness decodes a real awareness frame and confirms
// the server still overwrites the spoofed identity while preserving the cursor.
func TestDecodeRealYProtocolsAwareness(t *testing.T) {
	msgType, payload, err := DecodeMessage(mustHex(t, realAwareness))
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
	if got := hex.EncodeToString(body); got != realAwarenessInner {
		t.Fatalf("awareness body\n got: %s\nwant: %s", got, realAwarenessInner)
	}

	const authUID = "real-user-7"
	bound, entries, err := bindAwarenessIdentity(body, newAwarenessUser(authUID, "Bob"))
	if err != nil {
		t.Fatalf("bindAwarenessIdentity on real bytes: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries: got %d want 1", len(entries))
	}
	if entries[0].ClientID != realFixtureClientID || entries[0].Clock != realFixtureAwarenessClock {
		t.Fatalf("client id/clock: got %d/%d want %d/%d",
			entries[0].ClientID, entries[0].Clock, realFixtureClientID, realFixtureAwarenessClock)
	}

	var state struct {
		User   awarenessUser   `json:"user"`
		Cursor json.RawMessage `json:"cursor"`
	}
	if err := json.Unmarshal(entries[0].State, &state); err != nil {
		t.Fatalf("unmarshal bound state: %v", err)
	}
	if state.User.UID != authUID {
		t.Fatalf("uid: got %q want %q (spoofed uid survived)", state.User.UID, authUID)
	}
	if state.User.Name != "Bob" {
		t.Fatalf("name: got %q want the token display name", state.User.Name)
	}
	if string(state.Cursor) != `{"anchor":1,"head":4}` {
		t.Fatalf("cursor not preserved: got %s", state.Cursor)
	}

	// The re-encoded update must round-trip back through our own decoder.
	if _, err := decodeAwarenessUpdate(bound); err != nil {
		t.Fatalf("re-encoded awareness update does not decode: %v", err)
	}
}

// TestDecodeRealQueryAwareness covers the payload-free MsgQueryAwareness frame a
// y-websocket client sends on connect.
func TestDecodeRealQueryAwareness(t *testing.T) {
	msgType, payload, err := DecodeMessage(mustHex(t, realQueryAwareness))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != MsgQueryAwareness {
		t.Fatalf("msgType: got %d want %d", msgType, MsgQueryAwareness)
	}
	if len(payload) != 0 {
		t.Fatalf("MsgQueryAwareness should carry no payload, got %x", payload)
	}
}

// TestSync1PayloadIsAStateVectorNotADocument guards the connect handshake: the
// server's opening Sync Step-1 must be y-protocols' empty-state-vector frame,
// never the stored document blob.
//
// Yjs does not reject a document blob offered as a state vector — it MIS-READS
// it. Y.decodeStateVector on the realFullUpdate fixture yields {client 1: clock
// 1193046} (verified against yjs 13.6.32), so the server would claim to already
// hold 1.19M ops from client 1 and that peer would send nothing at all.
func TestSync1PayloadIsAStateVectorNotADocument(t *testing.T) {
	frame := hex.EncodeToString(EncodeSync1(EmptyStateVector()))
	if frame != realSyncStep1Empty {
		t.Fatalf("connect Sync1 frame: got %s want %s", frame, realSyncStep1Empty)
	}
	if docFrame := hex.EncodeToString(EncodeSync1(mustHex(t, realFullUpdate))); docFrame == frame {
		t.Fatal("document blob and empty state vector produced the same Sync1 frame")
	}
}

// TestEmptyDocumentUpdateMatchesYjs pins EmptyDocumentUpdate() to the bytes
// Y.encodeStateAsUpdate(new Y.Doc()) produces, and guards the reason it exists:
// a zero-length payload is correctly framed but NOT valid Yjs —
// Y.applyUpdate throws "Unexpected end of array" on it, which Yjs catches and
// logs while dropping the frame. Verified against yjs 13.6.32.
func TestEmptyDocumentUpdateMatchesYjs(t *testing.T) {
	const realEmptyDocUpdate = "0000"
	if got := hex.EncodeToString(EmptyDocumentUpdate()); got != realEmptyDocUpdate {
		t.Fatalf("EmptyDocumentUpdate: got %s want %s", got, realEmptyDocUpdate)
	}
	// The framed step 2 a joiner on a brand-new draft receives.
	if got := hex.EncodeToString(EncodeSync2(EmptyDocumentUpdate())); got != "0001020000" {
		t.Fatalf("empty-doc Sync2 frame: got %s want 0001020000", got)
	}
}

// TestNewRoomNeverSendsAZeroLengthUpdate covers a brand-new draft: the room has
// no stored state, and the Sync Step-2 payload must still be a valid update.
func TestNewRoomNeverSendsAZeroLengthUpdate(t *testing.T) {
	room := newRoom("draft-new", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	if got := hex.EncodeToString(room.DocumentUpdate()); got != "0000" {
		t.Fatalf("new room DocumentUpdate: got %s want 0000", got)
	}
	// Once there is real state, it is passed through untouched.
	stored := newRoom("draft-1", "policy-1", "tpl-1", mustHex(t, realFullUpdate), newMemStore(), nil, nil, log.Nop())
	if got := hex.EncodeToString(stored.DocumentUpdate()); got != realFullUpdate {
		t.Fatalf("stored DocumentUpdate: got %s want %s", got, realFullUpdate)
	}
}

// TestApplyUpdateIgnoresZeroLengthUpdate keeps an empty client update from being
// fanned out, which would make every peer's Y.applyUpdate throw.
func TestApplyUpdateIgnoresZeroLengthUpdate(t *testing.T) {
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), nil, nil, log.Nop())
	room.ApplyUpdate(t.Context(), nil)
	room.ApplyUpdate(t.Context(), []byte{})
	if len(room.CurrentState()) != 0 {
		t.Fatalf("zero-length update mutated room state: %x", room.CurrentState())
	}
	select {
	case frame := <-room.broadcast:
		t.Fatalf("zero-length update was queued for fan-out: %x", frame)
	default:
	}
}
