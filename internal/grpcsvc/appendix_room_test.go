// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"context"
	"sync"
	"testing"

	log "github.com/Bugs5382/go-log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

// fakeAppendixCore stands in for core's CheckpointAppendixDraft.
type fakeAppendixCore struct {
	mu         sync.Mutex
	calls      []snapshot.AppendixRequest
	rejectWith string
}

func (f *fakeAppendixCore) CheckpointAppendixDraft(_ context.Context, req snapshot.AppendixRequest) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	return f.rejectWith == "", f.rejectWith, nil
}

func (f *fakeAppendixCore) recorded() []snapshot.AppendixRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]snapshot.AppendixRequest(nil), f.calls...)
}

// policyCore must stay unused by an appendix room.
type policyCore struct{ calls int }

func (p *policyCore) UpdateDraftContent(context.Context, snapshot.SnapshotRequest) (bool, string, error) {
	p.calls++
	return true, "", nil
}

func newAppendixTestbed(t *testing.T, core *fakeAppendixCore, policy *policyCore) *collabTestbed {
	t.Helper()
	emitter, _ := newTestEmitter()
	pub := snapshot.NewPublisherWithClient(policy, emitter, log.Nop()).WithAppendixClient(core)
	return newCollabTestbedWithPublisher(t, pub)
}

const appendixRoom = "appendix:appendix-1"

// Flush parity: the gateway flushes an appendix room with the room key as the
// draft id, and core's CheckpointAppendixDraft is what answers.
func TestFlushDraft_AppendixRoomPersistsThroughCheckpointAppendixDraft(t *testing.T) {
	core, policy := &fakeAppendixCore{}, &policyCore{}
	tb := newAppendixTestbed(t, core, policy)
	conn := tb.join(t, appendixRoom, "policy-2", "u-bob")
	tb.waitForEditors(t, appendixRoom, 1)
	sendCheckpoint(t, conn, lexical("latest"))

	resp, err := tb.client.FlushDraft(actorCtx(t, publisher()), &collabv1.FlushDraftRequest{DraftId: appendixRoom, PolicyId: "policy-2"})
	if err != nil {
		t.Fatalf("FlushDraft: %v", err)
	}
	if !resp.GetRoomFound() || !resp.GetContentFlushed() {
		t.Fatalf("response: %+v", resp)
	}
	calls := core.recorded()
	if len(calls) != 1 || calls[0].AppendixID != "appendix-1" || calls[0].PolicyID != "policy-2" || calls[0].ContentJSON != lexical("latest") {
		t.Fatalf("core calls: %+v", calls)
	}
	if policy.calls != 0 {
		t.Fatal("the flush reached the policy draft call")
	}
}

func TestFlushDraft_AppendixRoomStopsThePublishWhenCoreRefuses(t *testing.T) {
	core := &fakeAppendixCore{rejectWith: "appendix is retired"}
	tb := newAppendixTestbed(t, core, &policyCore{})
	conn := tb.join(t, appendixRoom, "policy-2", "u-bob")
	tb.waitForEditors(t, appendixRoom, 1)
	sendCheckpoint(t, conn, lexical("latest"))

	_, err := tb.client.FlushDraft(actorCtx(t, publisher()), &collabv1.FlushDraftRequest{DraftId: appendixRoom, PolicyId: "policy-2"})
	if status.Code(err) != codes.FailedPrecondition || symbolOf(err) != "FLUSH_REJECTED" {
		t.Fatalf("got %v (%s)", status.Code(err), symbolOf(err))
	}
}

func TestFlushDraft_AppendixRoomOfAnotherPolicyIsLeftAlone(t *testing.T) {
	core := &fakeAppendixCore{}
	tb := newAppendixTestbed(t, core, &policyCore{})
	conn := tb.join(t, appendixRoom, "policy-2", "u-bob")
	tb.waitForEditors(t, appendixRoom, 1)
	sendCheckpoint(t, conn, lexical("latest"))

	_, err := tb.client.FlushDraft(actorCtx(t, publisher()), &collabv1.FlushDraftRequest{DraftId: appendixRoom, PolicyId: "policy-9"})
	if symbolOf(err) != "ROOM_POLICY_MISMATCH" {
		t.Fatalf("got %s", symbolOf(err))
	}
	if len(core.recorded()) != 0 {
		t.Fatal("the room was flushed for another policy")
	}
}

// Notify parity: publishing the appendix freezes its room and tells the editors
// which version went live.
func TestNotifyDraftPublished_FreezesTheAppendixRoom(t *testing.T) {
	tb := newAppendixTestbed(t, &fakeAppendixCore{}, &policyCore{})
	conn := tb.join(t, appendixRoom, "policy-2", "u-bob")
	tb.waitForEditors(t, appendixRoom, 1)

	resp, err := tb.client.NotifyDraftPublished(actorCtx(t, publisher()),
		&collabv1.NotifyDraftPublishedRequest{DraftId: appendixRoom, PolicyId: "policy-2", VersionNumber: 3})
	if err != nil || !resp.GetRoomNotified() {
		t.Fatalf("NotifyDraftPublished: %+v %v", resp, err)
	}
	ctrl := readControl(t, conn)
	if ctrl.Type != "draft.published" || ctrl.VersionNumber != 3 || ctrl.DraftID != appendixRoom {
		t.Fatalf("control: %+v", ctrl)
	}
}
