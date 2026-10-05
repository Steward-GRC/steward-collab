// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws_test

import (
	"testing"

	"github.com/Steward-GRC/steward-collab/internal/ws"
)

func TestEncodeDecodeSync1(t *testing.T) {
	stateVector := []byte{0x01, 0x02, 0x03}
	msg := ws.EncodeSync1(stateVector)
	msgType, payload, err := ws.DecodeMessage(msg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != ws.MsgSync {
		t.Fatalf("expected MsgSync, got %d", msgType)
	}
	syncType, data, err := ws.DecodeSyncMessage(payload)
	if err != nil {
		t.Fatalf("decode sync: %v", err)
	}
	if syncType != ws.SyncStep1 {
		t.Fatalf("expected SyncStep1, got %d", syncType)
	}
	if string(data) != string(stateVector) {
		t.Fatalf("state vector mismatch: got %v want %v", data, stateVector)
	}
}

func TestEncodeDecodeUpdate(t *testing.T) {
	update := []byte{0xAB, 0xCD}
	msg := ws.EncodeUpdate(update)
	msgType, payload, err := ws.DecodeMessage(msg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != ws.MsgSync {
		t.Fatalf("expected MsgSync, got %d", msgType)
	}
	syncType, data, err := ws.DecodeSyncMessage(payload)
	if err != nil {
		t.Fatalf("decode sync: %v", err)
	}
	if syncType != ws.SyncUpdate {
		t.Fatalf("expected SyncUpdate, got %d", syncType)
	}
	if string(data) != string(update) {
		t.Fatalf("update mismatch: got %v want %v", data, update)
	}
}

func TestEncodeAwareness(t *testing.T) {
	payload := []byte{0x10, 0x20}
	msg := ws.EncodeAwareness(payload)
	msgType, framed, err := ws.DecodeMessage(msg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msgType != ws.MsgAwareness {
		t.Fatalf("expected MsgAwareness, got %d", msgType)
	}
	// The awareness payload is a varUint8Array, so the length prefix has to be
	// stripped before the body is usable. Reading DecodeMessage's remainder
	// directly (as this test used to) is how the missing-prefix bug hid.
	data, err := ws.DecodeAwarenessMessage(framed)
	if err != nil {
		t.Fatalf("decode awareness: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("awareness data mismatch")
	}
}
