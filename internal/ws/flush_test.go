// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gatedPolicyClient is an in-memory core whose UpdateDraftContent can be held
// open, so a test can observe whether a flush waits for core's answer.
type gatedPolicyClient struct {
	mu      sync.Mutex
	calls   []snapshot.SnapshotRequest
	entered chan struct{}
	release chan struct{}
}

func newGatedPolicyClient() *gatedPolicyClient {
	return &gatedPolicyClient{entered: make(chan struct{}, 8), release: make(chan struct{})}
}

func (g *gatedPolicyClient) UpdateDraftContent(ctx context.Context, req snapshot.SnapshotRequest) (bool, string, error) {
	g.entered <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return false, "", status.FromContextError(ctx.Err()).Err()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, req)
	return true, "", nil
}

func (g *gatedPolicyClient) recorded() []snapshot.SnapshotRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]snapshot.SnapshotRequest(nil), g.calls...)
}

func TestRoomFlushPersistsTheNewestCheckpoint(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	go room.run(ctx)

	older := `{"root":{"type":"root","children":[{"type":"paragraph","text":"old"}]}}`
	room.RecordSnapshot(ctx, older, []byte{0x01}, "")
	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x02}, "")

	flushed, err := room.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !flushed {
		t.Fatal("Flush reported nothing flushed with a checkpoint pending")
	}
	got := core.recorded()
	if len(got) != 1 {
		t.Fatalf("core calls: got %d want 1", len(got))
	}
	if got[0].ContentJSON != testLexicalJSON {
		t.Fatalf("core got %q, want the newest checkpoint", got[0].ContentJSON)
	}
}

// A debounced flush can already be talking to core when a publish asks for a
// flush. Reporting success then, with nothing left pending, would let the
// version be cut before core has the content.
func TestRoomFlushWaitsForAnInFlightCheckpoint(t *testing.T) {
	ctx := t.Context()
	core := newGatedPolicyClient()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	fastDebounce(room)
	go room.run(ctx)

	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x01}, "")
	select {
	case <-core.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("debounced flush never reached core")
	}

	done := make(chan error, 1)
	go func() {
		_, err := room.Flush(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Flush returned (err=%v) while core had not answered the in-flight checkpoint", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(core.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Flush: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Flush never returned after core answered")
	}
	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core calls: got %d want 1", len(got))
	}
}

func TestRoomFlushSurfacesCoreRejection(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{err: status.Error(codes.InvalidArgument, "content validation: boilerplate edited")}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	go room.run(ctx)

	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x01}, "")
	_, err := room.Flush(ctx)
	var fe *FlushError
	if !errors.As(err, &fe) {
		t.Fatalf("Flush error = %v, want a *FlushError", err)
	}
	if fe.Code != codes.InvalidArgument {
		t.Errorf("code: got %v want InvalidArgument", fe.Code)
	}
	if !strings.Contains(fe.Detail, "boilerplate edited") {
		t.Errorf("detail: got %q, want core's message", fe.Detail)
	}

	// Nothing new has arrived, so the room's newest content is still not in
	// core. A second flush must say so rather than report success.
	if _, err := room.Flush(ctx); !errors.As(err, &fe) {
		t.Fatalf("second Flush error = %v, want the standing rejection", err)
	}
	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core calls: got %d want 1 (a rejected checkpoint is not resent)", len(got))
	}

	core.mu.Lock()
	core.err = nil
	core.mu.Unlock()
	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x02}, "")
	if _, err := room.Flush(ctx); err != nil {
		t.Fatalf("Flush after an accepted checkpoint: %v", err)
	}
}

// core being unreachable says nothing about the content, so the checkpoint
// must survive for the caller's retry instead of being dropped.
func TestRoomFlushKeepsTheCheckpointAfterATransientFailure(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{err: status.Error(codes.Unavailable, "connection refused")}
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	go room.run(ctx)

	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x01}, "")
	_, err := room.Flush(ctx)
	var fe *FlushError
	if !errors.As(err, &fe) || fe.Code != codes.Unavailable {
		t.Fatalf("Flush error = %v, want a *FlushError with Unavailable", err)
	}

	core.mu.Lock()
	core.err = nil
	core.mu.Unlock()
	flushed, err := room.Flush(ctx)
	if err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	if !flushed {
		t.Fatal("retry Flush found nothing to send; the checkpoint was dropped")
	}
	got := core.recorded()
	if len(got) != 2 || got[1].ContentJSON != testLexicalJSON {
		t.Fatalf("core calls: got %+v, want the same checkpoint sent twice", got)
	}
}

func TestRoomFlushGivesUpWhenTheCallerDoes(t *testing.T) {
	core := newGatedPolicyClient()
	room := newRoom("draft-1", "policy-1", "tpl-1", nil, newMemStore(), testPublisher(core), nil, log.Nop())
	go room.run(t.Context())

	room.RecordSnapshot(t.Context(), testLexicalJSON, []byte{0x01}, "")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := room.Flush(ctx)
	var fe *FlushError
	if !errors.As(err, &fe) || fe.Code != codes.DeadlineExceeded {
		t.Fatalf("Flush error = %v, want a *FlushError with DeadlineExceeded", err)
	}
}

func TestHubFlushDraftReportsWhatItFound(t *testing.T) {
	ctx := t.Context()
	core := &fakePolicyClient{}
	hub := NewHub(newMemStore(), testPublisher(core), nil, log.Nop())

	res, err := hub.FlushDraft(ctx, "draft-absent")
	if err != nil {
		t.Fatalf("FlushDraft with no room: %v", err)
	}
	if res.RoomFound || res.ContentFlushed {
		t.Fatalf("FlushDraft with no room: got %+v, want zero", res)
	}

	room, err := hub.GetOrCreateRoom(ctx, "draft-1", "policy-1", "tpl-1")
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	go room.run(ctx)

	res, err = hub.FlushDraft(ctx, "draft-1")
	if err != nil || !res.RoomFound || res.ContentFlushed {
		t.Fatalf("FlushDraft on an idle room: got %+v, %v; want found, nothing flushed", res, err)
	}

	room.RecordSnapshot(ctx, testLexicalJSON, []byte{0x01}, "")
	res, err = hub.FlushDraft(ctx, "draft-1")
	if err != nil || !res.RoomFound || !res.ContentFlushed {
		t.Fatalf("FlushDraft with a pending checkpoint: got %+v, %v; want found and flushed", res, err)
	}
}
