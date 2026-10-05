// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// buildAwarenessUpdate encodes a single-client y-protocols awareness update
// carrying the given state JSON, mirroring the client wire format the server
// receives.
func buildAwarenessUpdate(clientID, clock uint32, stateJSON string) []byte {
	out := encodeVarUint(1)
	out = append(out, encodeVarUint(clientID)...)
	out = append(out, encodeVarUint(clock)...)
	out = append(out, encodeVarUint8Array([]byte(stateJSON))...)
	return out
}

// decodeSingleAwarenessState decodes the first client-state entry back out of an
// awareness update payload for assertions.
func decodeSingleAwarenessState(t *testing.T, payload []byte) (clientID, clock uint32, state []byte) {
	t.Helper()
	n, rest, err := decodeVarUint(payload)
	if err != nil {
		t.Fatalf("decode client count: %v", err)
	}
	if n != 1 {
		t.Fatalf("client count: got %d want 1", n)
	}
	clientID, rest, err = decodeVarUint(rest)
	if err != nil {
		t.Fatalf("decode client id: %v", err)
	}
	clock, rest, err = decodeVarUint(rest)
	if err != nil {
		t.Fatalf("decode clock: %v", err)
	}
	state, _, err = decodeVarUint8Array(rest)
	if err != nil {
		t.Fatalf("decode state: %v", err)
	}
	return clientID, clock, state
}

// TestBindAwarenessIdentity_IgnoresSpoofedUser is the core anti-spoofing guard
// : a client asserts another user's uid/name/color, and the
// server MUST overwrite the whole `user` block with the connection's
// authenticated identity while preserving all cursor/selection fields.
func TestBindAwarenessIdentity_IgnoresSpoofedUser(t *testing.T) {
	const authUID = "real-user-7"
	spoofed := `{"user":{"uid":"victim-9","name":"Victim","color":"#000000"},"anchorPos":{"key":"a","offset":3},"focusing":true}`
	payload := buildAwarenessUpdate(42, 7, spoofed)

	bound, _, err := bindAwarenessIdentity(payload, newAwarenessUser(authUID, ""))
	if err != nil {
		t.Fatalf("bindAwarenessIdentity: %v", err)
	}

	clientID, clock, state := decodeSingleAwarenessState(t, bound)
	if clientID != 42 || clock != 7 {
		t.Fatalf("client id/clock not preserved: got %d/%d want 42/7", clientID, clock)
	}

	var got struct {
		User      awarenessUser   `json:"user"`
		AnchorPos json.RawMessage `json:"anchorPos"`
		Focusing  bool            `json:"focusing"`
	}
	if err := json.Unmarshal(state, &got); err != nil {
		t.Fatalf("unmarshal bound state: %v", err)
	}

	// Identity is server-authoritative — the spoofed values must be gone.
	if got.User.UID != authUID {
		t.Fatalf("uid: got %q want %q (spoofed uid was not ignored)", got.User.UID, authUID)
	}
	if got.User.Name != authUID {
		t.Fatalf("name: got %q want %q", got.User.Name, authUID)
	}
	if got.User.Color != colorForUser(authUID) {
		t.Fatalf("color: got %q want deterministic %q", got.User.Color, colorForUser(authUID))
	}

	// Non-identity presence fields (cursor/selection) must survive untouched.
	if string(got.AnchorPos) != `{"key":"a","offset":3}` {
		t.Fatalf("anchorPos not preserved: got %s", got.AnchorPos)
	}
	if !got.Focusing {
		t.Fatalf("focusing flag not preserved")
	}
}

// TestBindAwarenessIdentity_PassesThroughClearedState confirms a cleared state
// ("null" — a client dropping its presence) is forwarded unchanged: there is no
// identity to bind and rewriting it would corrupt the disconnect signal.
func TestBindAwarenessIdentity_PassesThroughClearedState(t *testing.T) {
	payload := buildAwarenessUpdate(42, 8, "null")
	bound, _, err := bindAwarenessIdentity(payload, newAwarenessUser("real-user-7", ""))
	if err != nil {
		t.Fatalf("bindAwarenessIdentity: %v", err)
	}
	_, _, state := decodeSingleAwarenessState(t, bound)
	if string(state) != "null" {
		t.Fatalf("cleared state not passed through: got %s", state)
	}
}

// TestColorForUser_Deterministic guards that the derived cursor color is stable
// for a uid (so presence color never changes between sessions) and comes from
// the fixed palette.
func TestColorForUser_Deterministic(t *testing.T) {
	a := colorForUser("user-x")
	b := colorForUser("user-x")
	if a != b {
		t.Fatalf("colorForUser not deterministic: %q vs %q", a, b)
	}
	inPalette := slices.Contains(awarenessCursorPalette, a)
	if !inPalette {
		t.Fatalf("color %q not from palette", a)
	}
}

// The web picks the colour for its own theme, so the bound identity carries
// the palette index beside the light-theme colour, and the two agree.
func TestNewAwarenessUser_CarriesThePaletteIndex(t *testing.T) {
	for _, uid := range []string{"user-alice", "user-bob", "user-carol", "user-dave"} {
		u := newAwarenessUser(uid, "")
		if u.ColorIndex < 0 || u.ColorIndex >= len(awarenessCursorPalette) {
			t.Fatalf("%s: colorIndex %d out of range", uid, u.ColorIndex)
		}
		if awarenessCursorPalette[u.ColorIndex] != u.Color {
			t.Fatalf("%s: colorIndex %d names %q, color is %q", uid, u.ColorIndex, awarenessCursorPalette[u.ColorIndex], u.Color)
		}
	}
	raw, err := json.Marshal(newAwarenessUser("user-bob", "Bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"colorIndex":`) {
		t.Fatalf("colorIndex missing from the wire: %s", raw)
	}
}
