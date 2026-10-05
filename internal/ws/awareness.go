// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"cmp"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"slices"
)

// awarenessCursorPalette is the design's eight collaborator colours in the
// light theme. The server picks one from the authenticated user id, so a user
// always renders in the same colour and a client can't pick another user's.
// Each theme has its own eight, so the web reads colorIndex and uses its
// theme's colour; color serves stock y-websocket clients.
var awarenessCursorPalette = []string{
	"#8E3B8A",
	"#9A5A00",
	"#2F5FD0",
	"#C0392B",
	"#2E7D32",
	"#B0306A",
	"#5B4BC4",
	"#B4531A",
}

// awarenessStateCleared is the literal awareness state a Yjs client writes when
// it drops its presence (y-protocols encodes the state as JSON, and a cleared
// entry is the JSON null).
const awarenessStateCleared = "null"

// colorIndexForUser derives a stable palette index from a user id, so presence
// colours are consistent across sessions with no stored state.
func colorIndexForUser(uid string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(uid))
	return int(h.Sum32() % clampUint32(len(awarenessCursorPalette)))
}

// colorForUser is the light-theme colour at the user's palette index.
func colorForUser(uid string) string {
	return awarenessCursorPalette[colorIndexForUser(uid)]
}

// awarenessUser is the server-authoritative identity block the server writes
// into every awareness state entry. Clients (the presence UI) read the remote
// peer's identity from this `user` field; because the server always overwrites
// it with the connection's token identity, a client cannot spoof another user's
// cursor, name, or color.
type awarenessUser struct {
	UID   string `json:"uid"`
	Name  string `json:"name"`
	Color string `json:"color"`
	// ColorIndex is the position in the design's collaborator palette.
	ColorIndex int `json:"colorIndex"`
}

// newAwarenessUser builds the authoritative identity block for a connection.
// displayName is the `name` claim of the collab token, which the Gateway
// resolves from the caller's identity record before minting. It falls back to
// the uid when the token carries no name, so an older token (or an identity
// lookup the Gateway could not complete) still yields a usable — if ugly —
// presence label instead of an empty one.
func newAwarenessUser(uid, displayName string) awarenessUser {
	if displayName == "" {
		displayName = uid
	}
	return awarenessUser{UID: uid, Name: displayName, Color: colorForUser(uid), ColorIndex: colorIndexForUser(uid)}
}

// awarenessEntry is one decoded client-state entry of a y-protocols awareness
// update. ClientID is the Yjs client id (NOT our user id — one user may have
// several, e.g. two tabs), Clock is that entry's monotonic awareness clock, and
// State is the raw state JSON (awarenessStateCleared when the peer left).
type awarenessEntry struct {
	ClientID uint32
	Clock    uint32
	State    []byte
}

// cleared reports whether this entry is a peer dropping its presence.
func (e awarenessEntry) cleared() bool { return string(e.State) == awarenessStateCleared }

// decodeAwarenessUpdate parses a y-protocols awareness update body.
//
// Wire format (y-protocols/awareness.js encodeAwarenessUpdate):
//
//	varUint(numClients)
//	repeat numClients:
//	  varUint(clientID)
//	  varUint(clock)
//	  varString(stateJSON)   // "null" when the client clears its state
//
// The body is what DecodeAwarenessMessage returns — i.e. the varUint8Array
// length prefix has already been stripped.
func decodeAwarenessUpdate(body []byte) ([]awarenessEntry, error) {
	numClients, rest, err := decodeVarUint(body)
	if err != nil {
		return nil, fmt.Errorf("awareness: decode client count: %w", err)
	}
	entries := make([]awarenessEntry, 0, min(int(numClients), 64))
	for range numClients {
		clientID, r2, err := decodeVarUint(rest)
		if err != nil {
			return nil, fmt.Errorf("awareness: decode client id: %w", err)
		}
		clock, r3, err := decodeVarUint(r2)
		if err != nil {
			return nil, fmt.Errorf("awareness: decode clock: %w", err)
		}
		state, r4, err := decodeVarUint8Array(r3)
		if err != nil {
			return nil, fmt.Errorf("awareness: decode state: %w", err)
		}
		rest = r4
		entries = append(entries, awarenessEntry{ClientID: clientID, Clock: clock, State: state})
	}
	return entries, nil
}

// encodeAwarenessUpdate serialises entries back into a y-protocols awareness
// update body, ready to be wrapped by EncodeAwareness.
func encodeAwarenessUpdate(entries []awarenessEntry) []byte {
	out := encodeVarUint(clampUint32(len(entries)))
	for _, e := range entries {
		out = append(out, encodeVarUint(e.ClientID)...)
		out = append(out, encodeVarUint(e.Clock)...)
		out = append(out, encodeVarUint8Array(e.State)...)
	}
	return out
}

// sortAwarenessEntries orders entries by client id so an encoded awareness
// frame is byte-stable (map iteration order must not leak into the wire).
func sortAwarenessEntries(entries []awarenessEntry) {
	slices.SortFunc(entries, func(a, b awarenessEntry) int { return cmp.Compare(a.ClientID, b.ClientID) })
}

// bindAwarenessIdentity rewrites the identity of every client-state entry in a
// y-protocols awareness update so it carries the connection's authenticated
// identity instead of anything the client asserted: the server is
// authoritative for the `user` field, never trusting a client-supplied
// uid/name/color.
//
// Non-identity fields (cursor anchor/focus, selection, focusing flag, …) are
// preserved byte-for-byte so live-follow cursors keep working; only the `user`
// key is replaced. A cleared state is passed through untouched — it carries no
// identity to spoof.
//
// It returns both the re-encoded update (for fan-out) and the decoded entries,
// so the room can fold them into its presence map without parsing twice.
func bindAwarenessIdentity(body []byte, user awarenessUser) ([]byte, []awarenessEntry, error) {
	entries, err := decodeAwarenessUpdate(body)
	if err != nil {
		return nil, nil, err
	}
	for i, e := range entries {
		newState, err := rebindStateUser(e.State, user)
		if err != nil {
			return nil, nil, fmt.Errorf("awareness: rebind state: %w", err)
		}
		entries[i].State = newState
	}
	return encodeAwarenessUpdate(entries), entries, nil
}

// rebindStateUser replaces the `user` key of a single awareness state JSON with
// the server-authoritative identity, preserving every other key. A cleared
// state (a client dropping its presence) is returned unchanged.
func rebindStateUser(stateJSON []byte, user awarenessUser) ([]byte, error) {
	if string(stateJSON) == awarenessStateCleared {
		return stateJSON, nil
	}
	// Preserve unknown fields (cursor/selection/etc.) by decoding into raw
	// messages and only overwriting `user`.
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(stateJSON, &fields); err != nil {
		return nil, err
	}
	userJSON, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}
	fields["user"] = userJSON
	return json.Marshal(fields)
}
