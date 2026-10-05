// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package ws implements the y-websocket binary message framing.
//
// Protocol reference:
//   - message envelope: https://github.com/yjs/y-websocket/blob/master/bin/utils.js
//   - sync sub-protocol: https://github.com/yjs/y-protocols/blob/master/sync.js
//   - awareness:         https://github.com/yjs/y-protocols/blob/master/awareness.js
//
// Message layout:
//
//	[messageType varUint] [payload …]
//
// For MsgSync the payload is:
//
//	[syncType varUint] [varUint8Array data]
//
// For MsgAwareness the payload is:
//
//	[varUint8Array awarenessUpdate]
//
// The varUint8Array framing (a varUint byte-length prefix followed by the
// bytes) is NOT optional. y-protocols writes it with lib0's
// `encoding.writeVarUint8Array` and reads it back with
// `decoding.readVarUint8Array`; a stock Yjs client throws
// "Unexpected end of array" on a prefix-less sync frame and silently swallows
// the failure on an update frame (the edit is simply lost). Every Encode*
// helper below therefore writes the prefix and every Decode* helper strips it.
//
// # Collab extension frames
//
// Two message types are ours, not y-websocket's: MsgSnapshot (client→server)
// and MsgControl (server→client). They ride the same envelope and the same
// varUint8Array field framing, so a browser can write them with plain lib0
// encoders and no custom binary code.
//
// MsgSnapshot — client→server checkpoint, the ONLY path by which live edits
// reach durable storage:
//
//	[varUint 100]
//	[varUint8Array contentJSON]   // Lexical EditorState JSON, UTF-8
//	[varUint8Array yjsState]      // Y.encodeStateAsUpdate(doc), may be empty
//
// contentJSON is the AUTHORITATIVE content: the server forwards it to core
// PolicyService.UpdateDraftContent, which validates it (draft status, pinned
// template_version_id, template-section shape) and persists it as the draft's
// ContentJSON. yjsState is the client's own compacted full document state; the
// server stores it VERBATIM and never decodes or merges it (the relay runs no
// Y.Doc — see Room). It exists only so a room can be rehydrated after a
// restart, which is why storing an unvalidated client blob is safe: the source
// of truth is the core-validated Lexical JSON, not the Yjs bytes.
//
// Both fields are length-prefixed, so either may be empty (send only the JSON,
// or only a compacted state) and later fields can be appended without breaking
// older servers — a decoder ignores trailing bytes it does not know. The frame
// deliberately carries NO actor: the acting user is the socket's authenticated
// JWT `uid`, which the server already holds and a client must not be able to
// assert.
//
// MsgControl — server→client control channel:
//
//	[varUint 101]
//	[varUint8Array controlJSON]   // JSON-encoded ControlMessage
//
// It tells the editor about things it cannot observe from the document stream:
// a snapshot core accepted or refused, or the draft being published (drop to
// read-only). See ControlMessage.
package ws

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
)

// Top-level message type bytes (y-websocket constants).
const (
	MsgSync           = 0
	MsgAwareness      = 1
	MsgAuth           = 2
	MsgQueryAwareness = 3
)

// Collab extension message type bytes. y-websocket allocates 0-3; 100+ is
// deliberately far above that range so upstream can add message types without
// colliding with ours.
const (
	// MsgSnapshot is a client→server Lexical-JSON + compacted-Yjs-state
	// checkpoint. See the package doc for the frame layout.
	MsgSnapshot = 100
	// MsgControl is a server→client control frame. See ControlMessage.
	MsgControl = 101
)

// ControlMessage is the JSON payload of a MsgControl frame.
//
// Type is always set; the remaining fields are populated per type:
//
//   - ControlSnapshotAccepted — core persisted the checkpoint. The editor can
//     surface a "saved" indicator. No Reason/Detail.
//   - ControlSnapshotRejected — the checkpoint did NOT persist. Reason is a
//     stable machine token (pre_check_failed | grpc_error | server_rejected)
//     and Detail is the human-readable cause (e.g. core's template-validation
//     message). The room keeps running: the editor must surface the failure and
//     reconcile, because its edits are NOT durable.
//   - ControlDraftPublished — the draft was published as VersionNumber and is
//     now immutable. The editor must drop to read-only; the room stops
//     accepting updates and snapshots.
type ControlMessage struct {
	Type          string `json:"type"`
	DraftID       string `json:"draft_id"`
	Reason        string `json:"reason,omitempty"`
	Detail        string `json:"detail,omitempty"`
	VersionNumber int32  `json:"version_number,omitempty"`
}

// ControlMessage Type values.
const (
	ControlSnapshotAccepted = "snapshot.accepted"
	ControlSnapshotRejected = "snapshot.rejected"
	ControlDraftPublished   = "draft.published"
)

// Sync sub-message type bytes.
const (
	SyncStep1  = 0
	SyncStep2  = 1
	SyncUpdate = 2
)

// EmptyStateVector returns the y-protocols encoding of a state vector with zero
// clients — a single varUint 0, exactly what `Y.encodeStateVector(new Y.Doc())`
// produces.
//
// The relay does not maintain a Y.Doc (see the room's relay-mode contract), so
// an empty state vector is the only *truthful* Sync Step-1 it can offer: "I know
// of no client clocks, send me everything you have". A Yjs peer answers it with
// a Sync Step-2 carrying its full document. Advertising the stored document
// blob here instead would be read by `readStateVector` as an arbitrary set of
// client clocks — see TestSync1PayloadIsAStateVectorNotADocument — which makes
// the peer withhold updates the room actually needs.
func EmptyStateVector() []byte { return []byte{0x00} }

// EmptyDocumentUpdate returns the y-protocols encoding of an update for a
// document with no content — what `Y.encodeStateAsUpdate(new Y.Doc())` produces
// (zero structs, empty delete set).
//
// It exists because a ZERO-LENGTH update payload is not valid Yjs. A correctly
// framed but empty Sync Step-2 makes `Y.applyUpdate` throw "Unexpected end of
// array", which Yjs catches and logs while discarding the frame. A brand-new
// draft has no stored state, so without this substitution every joiner on a new
// draft would provoke that error — see Room.DocumentUpdate.
func EmptyDocumentUpdate() []byte { return []byte{0x00, 0x00} }

// encodeVarUint encodes a uint32 as a variable-length unsigned integer.
func encodeVarUint(n uint32) []byte {
	buf := make([]byte, binary.MaxVarintLen32)
	written := binary.PutUvarint(buf, uint64(n))
	return buf[:written]
}

// decodeVarUint reads a varint from the front of b; returns the value and remaining bytes.
func decodeVarUint(b []byte) (uint32, []byte, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, fmt.Errorf("invalid varint")
	}
	// The wire value is untrusted; the protocol only ever encodes uint32
	// (see encodeVarUint), so reject anything that would overflow rather
	// than silently wrapping.
	if v > math.MaxUint32 {
		return 0, nil, fmt.Errorf("varint %d overflows uint32", v)
	}
	return uint32(v), b[n:], nil
}

// encodeVarUint8Array writes b with a varUint byte-length prefix — lib0's
// `encoding.writeVarUint8Array`. lib0's `writeVarString` has the identical byte
// layout, so this also encodes the awareness state strings.
func encodeVarUint8Array(b []byte) []byte {
	return append(encodeVarUint(clampUint32(len(b))), b...)
}

// decodeVarUint8Array reads a length-prefixed byte array from the front of b and
// returns it plus the remainder — lib0's `decoding.readVarUint8Array`.
func decodeVarUint8Array(b []byte) (data, rest []byte, err error) {
	n, rest, err := decodeVarUint(b)
	if err != nil {
		return nil, nil, err
	}
	if int(n) > len(rest) {
		return nil, nil, fmt.Errorf("length %d exceeds %d remaining bytes", n, len(rest))
	}
	return rest[:n], rest[n:], nil
}

// DecodeMessage splits a raw websocket message into (messageType, payload, error).
func DecodeMessage(msg []byte) (uint32, []byte, error) {
	msgType, rest, err := decodeVarUint(msg)
	if err != nil {
		return 0, nil, fmt.Errorf("decode message type: %w", err)
	}
	return msgType, rest, nil
}

// DecodeSyncMessage splits the payload of a MsgSync message into
// (syncType, data, error), stripping the varUint8Array length prefix that
// y-protocols wrote around data.
func DecodeSyncMessage(payload []byte) (uint32, []byte, error) {
	syncType, rest, err := decodeVarUint(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("decode sync type: %w", err)
	}
	data, _, err := decodeVarUint8Array(rest)
	if err != nil {
		return 0, nil, fmt.Errorf("decode sync payload: %w", err)
	}
	return syncType, data, nil
}

// DecodeAwarenessMessage strips the varUint8Array length prefix from the payload
// of a MsgAwareness message, yielding the raw y-protocols awareness update.
func DecodeAwarenessMessage(payload []byte) ([]byte, error) {
	body, _, err := decodeVarUint8Array(payload)
	if err != nil {
		return nil, fmt.Errorf("decode awareness payload: %w", err)
	}
	return body, nil
}

// EncodeSync1 builds a sync step-1 message containing a Yjs state vector.
func EncodeSync1(stateVector []byte) []byte {
	return concat(encodeVarUint(MsgSync), encodeVarUint(SyncStep1), encodeVarUint8Array(stateVector))
}

// EncodeSync2 builds a sync step-2 message containing a Yjs update diff.
func EncodeSync2(update []byte) []byte {
	return concat(encodeVarUint(MsgSync), encodeVarUint(SyncStep2), encodeVarUint8Array(update))
}

// EncodeUpdate builds a sync update message.
func EncodeUpdate(update []byte) []byte {
	return concat(encodeVarUint(MsgSync), encodeVarUint(SyncUpdate), encodeVarUint8Array(update))
}

// EncodeAwareness wraps a y-protocols awareness update in a MsgAwareness frame.
func EncodeAwareness(update []byte) []byte {
	return concat(encodeVarUint(MsgAwareness), encodeVarUint8Array(update))
}

// EncodeSnapshot builds a MsgSnapshot frame. The browser builds this frame with
// lib0 (writeVarUint / writeVarString / writeVarUint8Array); this helper exists
// so the Go side has one canonical encoder to test the decoder against.
func EncodeSnapshot(contentJSON string, yjsState []byte) []byte {
	return concat(
		encodeVarUint(MsgSnapshot),
		encodeVarUint8Array([]byte(contentJSON)),
		encodeVarUint8Array(yjsState),
	)
}

// DecodeSnapshotMessage parses the payload of a MsgSnapshot frame into the
// client's Lexical EditorState JSON and its compacted Yjs state.
//
// The Yjs-state field is optional: a payload that ends after contentJSON yields
// an empty state, which the room reads as "keep the state you already have".
// Trailing bytes beyond the two known fields are ignored so a newer client can
// append fields without breaking an older server.
func DecodeSnapshotMessage(payload []byte) (contentJSON string, yjsState []byte, err error) {
	content, rest, err := decodeVarUint8Array(payload)
	if err != nil {
		return "", nil, fmt.Errorf("decode snapshot content: %w", err)
	}
	if len(rest) == 0 {
		return string(content), nil, nil
	}
	state, _, err := decodeVarUint8Array(rest)
	if err != nil {
		return "", nil, fmt.Errorf("decode snapshot yjs state: %w", err)
	}
	return string(content), state, nil
}

// EncodeControl builds a MsgControl frame carrying msg as JSON. The error is
// returned rather than swallowed so callers log an encoding bug instead of
// silently dropping a control notification.
func EncodeControl(msg ControlMessage) ([]byte, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode control message: %w", err)
	}
	return concat(encodeVarUint(MsgControl), encodeVarUint8Array(body)), nil
}

// DecodeControlMessage parses the payload of a MsgControl frame. The service
// never receives one (MsgControl is server→client only); this exists for tests
// and for any Go client of the relay.
func DecodeControlMessage(payload []byte) (ControlMessage, error) {
	body, _, err := decodeVarUint8Array(payload)
	if err != nil {
		return ControlMessage{}, fmt.Errorf("decode control payload: %w", err)
	}
	var msg ControlMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return ControlMessage{}, fmt.Errorf("unmarshal control message: %w", err)
	}
	return msg, nil
}

func concat(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// clampUint32 narrows a length to the uint32 the wire varints carry. Frame
// lengths are capped far below the limit by maxMessageSize, so the clamp never
// changes a real value.
func clampUint32(n int) uint32 {
	switch {
	case n < 0:
		return 0
	case n > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(n) //nolint:gosec // bounded by the guards above
	}
}
