// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot_test

import (
	"context"
	"testing"

	log "github.com/Bugs5382/go-log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

// fakeAppendixClient stands in for core's
// PolicyAppendixService.CheckpointAppendixDraft.
type fakeAppendixClient struct {
	calls      []snapshot.AppendixRequest
	rejectWith string
	errWith    error
}

func (f *fakeAppendixClient) CheckpointAppendixDraft(_ context.Context, req snapshot.AppendixRequest) (bool, string, error) {
	f.calls = append(f.calls, req)
	if f.errWith != nil {
		return false, "", f.errWith
	}
	if f.rejectWith != "" {
		return false, f.rejectWith, nil
	}
	return true, "", nil
}

const (
	appendixActor = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"
	lexicalDoc    = `{"root":{"type":"root","children":[]}}`
)

func TestAppendixRoomKey(t *testing.T) {
	key := snapshot.AppendixRoomKey("appendix-1")
	if key != "appendix:appendix-1" {
		t.Fatalf("room key: got %q", key)
	}
	if id, ok := snapshot.AppendixIDFromRoomKey(key); !ok || id != "appendix-1" {
		t.Fatalf("round trip: got %q %v", id, ok)
	}
	for _, key := range []string{"draft-1", "", "appendix:", "xappendix:a"} {
		if id, ok := snapshot.AppendixIDFromRoomKey(key); ok {
			t.Errorf("%q read as an appendix room (id %q)", key, id)
		}
	}
}

// Mock "becomes the appendix's draft": collab hands core the appendix id, the
// policy, the content and the person; core makes the draft.
func TestPublisher_AppendixRoomCheckpointGoesToTheAppendixCall(t *testing.T) {
	policy, appendix := &fakePolicyClient{}, &fakeAppendixClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(policy, em, log.Nop()).WithAppendixClient(appendix)

	out := p.SnapshotJSON(t.Context(), snapshot.Checkpoint{
		DraftID: "appendix:appendix-1", PolicyID: "policy-1", ContentJSON: lexicalDoc,
		ActorUserID: appendixActor, Impersonator: "admin-1",
	})

	if !out.Accepted {
		t.Fatalf("outcome: %+v", out)
	}
	if policy.callCount != 0 {
		t.Fatal("an appendix checkpoint reached the policy draft call")
	}
	if len(appendix.calls) != 1 {
		t.Fatalf("appendix calls: %d", len(appendix.calls))
	}
	want := snapshot.AppendixRequest{
		AppendixID: "appendix-1", PolicyID: "policy-1", ContentJSON: lexicalDoc,
		ActorUserID: appendixActor, Impersonator: "admin-1",
	}
	if appendix.calls[0] != want {
		t.Fatalf("request: got %+v want %+v", appendix.calls[0], want)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 || msgs[0].Event.Action != "collab.snapshot.accepted" {
		t.Fatalf("audit: %+v", msgs)
	}
	ev := msgs[0].Event
	if ev.Subject != "appendix:appendix-1" || ev.Attributes["appendix_id"] != "appendix-1" || ev.Attributes["policy_id"] != "policy-1" {
		t.Fatalf("audit subject/attributes: %q %v", ev.Subject, ev.Attributes)
	}
}

func TestPublisher_PolicyRoomStillUsesThePolicyCall(t *testing.T) {
	policy, appendix := &fakePolicyClient{}, &fakeAppendixClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(policy, em, log.Nop()).WithAppendixClient(appendix)

	p.SnapshotJSON(t.Context(), snapshot.Checkpoint{DraftID: "draft-1", PolicyID: "policy-1", ContentJSON: lexicalDoc, ActorUserID: appendixActor})

	if policy.callCount != 1 || len(appendix.calls) != 0 {
		t.Fatalf("policy calls %d, appendix calls %d", policy.callCount, len(appendix.calls))
	}
}

// Mock "refused when the content is not a JSON document".
func TestPublisher_AppendixRoomRefusesNonJSONBeforeCore(t *testing.T) {
	appendix := &fakeAppendixClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(&fakePolicyClient{}, em, log.Nop()).WithAppendixClient(appendix)

	out := p.SnapshotJSON(t.Context(), snapshot.Checkpoint{DraftID: "appendix:appendix-1", PolicyID: "policy-1", ContentJSON: "not json"})

	if out.Accepted || out.Reason != snapshot.ReasonPreCheckFailed {
		t.Fatalf("outcome: %+v", out)
	}
	if len(appendix.calls) != 0 {
		t.Fatal("a non-JSON checkpoint reached core")
	}
	if msgs := fap.snapshot(); len(msgs) != 1 || msgs[0].Event.Action != "collab.snapshot.rejected" || msgs[0].Event.Subject != "appendix:appendix-1" {
		t.Fatalf("audit: %+v", msgs)
	}
}

func TestPublisher_AppendixRoomRelaysCoresRefusal(t *testing.T) {
	appendix := &fakeAppendixClient{rejectWith: "appendix is retired"}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(&fakePolicyClient{}, em, log.Nop()).WithAppendixClient(appendix)

	out := p.SnapshotJSON(t.Context(), snapshot.Checkpoint{DraftID: "appendix:appendix-1", PolicyID: "policy-1", ContentJSON: lexicalDoc})

	if out.Accepted || out.Reason != snapshot.ReasonServerRejected || out.Detail != "appendix is retired" {
		t.Fatalf("outcome: %+v", out)
	}
}

func TestPublisher_AppendixRoomReportsACallThatFailed(t *testing.T) {
	appendix := &fakeAppendixClient{errWith: status.Error(codes.Unavailable, "core down")}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(&fakePolicyClient{}, em, log.Nop()).WithAppendixClient(appendix)

	out := p.SnapshotJSON(t.Context(), snapshot.Checkpoint{DraftID: "appendix:appendix-1", PolicyID: "policy-1", ContentJSON: lexicalDoc})

	if out.Accepted || out.Reason != snapshot.ReasonGRPCError || out.Code != codes.Unavailable {
		t.Fatalf("outcome: %+v", out)
	}
}

// A publisher built without the appendix call answers an appendix checkpoint
// with a refusal rather than sending it down the policy path.
func TestPublisher_AppendixRoomWithoutAClientIsRefused(t *testing.T) {
	policy := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(policy, em, log.Nop())

	out := p.SnapshotJSON(t.Context(), snapshot.Checkpoint{DraftID: "appendix:appendix-1", PolicyID: "policy-1", ContentJSON: lexicalDoc})

	if out.Accepted || out.Reason != snapshot.ReasonGRPCError || out.Code != codes.Unimplemented {
		t.Fatalf("outcome: %+v", out)
	}
	if policy.callCount != 0 {
		t.Fatal("the checkpoint fell through to the policy call")
	}
}
