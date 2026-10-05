// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/gorilla/websocket"
)

// fakeAuditor records emitted audit events for assertions.
type fakeAuditor struct {
	mu     sync.Mutex
	events []audit.Event
}

func (f *fakeAuditor) Emit(_ context.Context, ev audit.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}

// waitForAction polls until an event with the given action is recorded, or the
// deadline expires, and returns it. Emission is asynchronous (off the room
// goroutine), so tests poll rather than assume synchronous delivery.
func (f *fakeAuditor) waitForAction(t *testing.T, action string) audit.Event {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		for _, ev := range f.events {
			if ev.Action == action {
				f.mu.Unlock()
				return ev
			}
		}
		f.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("audit event %q was not emitted within timeout", action)
	return audit.Event{}
}

// TestRoom_EmitsSessionJoinedAndLeft verifies the session audit events:
// a client joining a room emits collab.session.joined and leaving
// emits collab.session.left, both with actor = JWT uid and subject = policy.
func TestRoom_EmitsSessionJoinedAndLeft(t *testing.T) {
	ctx := t.Context()

	fa := &fakeAuditor{}
	room := newRoom("draft-9", "policy-9", "tpl-9", nil, newMemStore(), nil, fa, log.Nop())
	go room.run(ctx)

	srv, wsURL := wsTestServer(t, func(conn *websocket.Conn) {
		NewClient(ctx, room, conn, "u-audit", "", log.Nop())
	})
	defer srv.Close()

	conn := dialWS(t, wsURL)
	drainInitialSync(t, conn)

	joined := fa.waitForAction(t, "collab.session.joined")
	if joined.ActorUserID != "u-audit" {
		t.Fatalf("joined actor: got %q want u-audit", joined.ActorUserID)
	}
	if joined.Subject != "policy:policy-9" {
		t.Fatalf("joined subject: got %q want policy:policy-9", joined.Subject)
	}
	if joined.Tier != audit.TierAudit {
		t.Fatalf("joined tier: got %q want %q", joined.Tier, audit.TierAudit)
	}
	if joined.Attributes["draft_id"] != "draft-9" {
		t.Fatalf("joined draft_id attr: got %q want draft-9", joined.Attributes["draft_id"])
	}

	// Disconnect; readPump should unregister the client and emit session.left.
	_ = conn.Close()

	left := fa.waitForAction(t, "collab.session.left")
	if left.ActorUserID != "u-audit" {
		t.Fatalf("left actor: got %q want u-audit", left.ActorUserID)
	}
	if left.Subject != "policy:policy-9" {
		t.Fatalf("left subject: got %q want policy:policy-9", left.Subject)
	}
}
