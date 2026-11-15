// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Bugs5382/go-log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

// Core's UpdateDraftContent guard (steward-core internal/grpcsvc/policy_handler.go):
// a version that holds the draft slot (draft or in_review) takes a checkpoint, as the
// mock does while approvers review; a rejected version is final and a late checkpoint
// is refused with FailedPrecondition, "policy version <id> is not a draft (status=rejected)".
// Collab keeps no status of its own: it forwards the checkpoint and reports core's answer.

// A checkpoint for a rejected version is refused by core. The publisher passes core's
// reason through to the editor and audits the refusal.
func TestPublisher_RejectedVersionCheckpointCarriesCoresReason(t *testing.T) {
	fake := &fakePolicyClient{
		errWith: status.Error(codes.FailedPrecondition, "policy version draft-9 is not a draft (status=rejected)"),
	}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-9", PolicyID: "policy-9", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	if out.Accepted {
		t.Fatal("a checkpoint for a rejected version was reported accepted")
	}
	if out.Reason != snapshot.ReasonGRPCError {
		t.Errorf("reason: got %q want %q", out.Reason, snapshot.ReasonGRPCError)
	}
	if !strings.Contains(out.Detail, "FailedPrecondition") || !strings.Contains(out.Detail, "status=rejected") {
		t.Errorf("detail: got %q, want core's FailedPrecondition and status=rejected", out.Detail)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 || msgs[0].Event.Action != "collab.snapshot.rejected" {
		t.Fatalf("audit: got %+v want one collab.snapshot.rejected", msgs)
	}
	if msgs[0].Event.Attributes["detail"] != out.Detail {
		t.Errorf("audit detail: got %q want %q", msgs[0].Event.Attributes["detail"], out.Detail)
	}
}

// A version in review keeps taking checkpoints (the author edits while approvers
// review). Core accepts it; the publisher must not gate on status itself.
func TestPublisher_InReviewVersionCheckpointIsAccepted(t *testing.T) {
	fake := &fakePolicyClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-10", PolicyID: "policy-10", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	if !out.Accepted {
		t.Fatalf("outcome: got %+v want accepted", out)
	}
	if fake.callCount != 1 {
		t.Fatalf("core calls: got %d want 1", fake.callCount)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 || msgs[0].Event.Action != "collab.snapshot.accepted" {
		t.Fatalf("audit: got %+v want one collab.snapshot.accepted", msgs)
	}
}
