// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeAuditPublisher records every audit message routed through it so tests
// can assert what the publisher emitted. Bodies are decoded AuditEvents.
type fakeAuditPublisher struct {
	mu       sync.Mutex
	messages []fakeAuditMessage
}

type fakeAuditMessage struct {
	RoutingKey string
	Event      audit.Event
}

func (f *fakeAuditPublisher) Publish(_ context.Context, routingKey string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ev, err := audit.Decode(body)
	if err != nil {
		return err
	}
	f.messages = append(f.messages, fakeAuditMessage{RoutingKey: routingKey, Event: ev})
	return nil
}

func (f *fakeAuditPublisher) snapshot() []fakeAuditMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeAuditMessage, len(f.messages))
	copy(out, f.messages)
	return out
}

// newTestEmitter returns an Emitter wired to a fresh fakeAuditPublisher so
// each test owns its own audit stream.
func newTestEmitter() (*audit.Emitter, *fakeAuditPublisher) {
	fap := &fakeAuditPublisher{}
	return audit.New(fap), fap
}

// fakePolicyClient is an in-memory PolicyClient used to assert what the
// publisher forwards to the gRPC layer. Setting rejectWith returns
// (accepted=false, reason=rejectWith). Setting errWith returns the supplied
// error verbatim so tests can verify gRPC status codes propagate.
type fakePolicyClient struct {
	calledWith *snapshot.SnapshotRequest
	callCount  int
	rejectWith string
	errWith    error
}

func (f *fakePolicyClient) UpdateDraftContent(_ context.Context, req snapshot.SnapshotRequest) (bool, string, error) {
	f.callCount++
	f.calledWith = &req
	if f.errWith != nil {
		return false, "", f.errWith
	}
	if f.rejectWith != "" {
		return false, f.rejectWith, nil
	}
	return true, "", nil
}

func TestPublisher_CallsPolicyClientWithLexicalJSON(t *testing.T) {
	fake := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	// Client supplies Lexical JSON directly — no Yjs decoding on the server.
	lexJSON := `{"root":{"type":"root","children":[]}}`

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: lexJSON,
	})

	if fake.calledWith == nil {
		t.Fatal("expected policy client to be called")
	}
	if fake.calledWith.DraftID != "draft-1" {
		t.Fatalf("draft id: got %q", fake.calledWith.DraftID)
	}
	if fake.calledWith.PolicyID != "policy-1" {
		t.Fatalf("policy id: got %q", fake.calledWith.PolicyID)
	}
	if fake.calledWith.ContentJSON != lexJSON {
		t.Fatalf("content json: got %q want %q", fake.calledWith.ContentJSON, lexJSON)
	}
	if fake.calledWith.TemplateVersionID != "tmpl-v1" {
		t.Fatalf("template version id: got %q", fake.calledWith.TemplateVersionID)
	}
	// Core's PolicyService.UpdateDraftContent rejects requests with an empty
	// actor_user_id (codes.Unauthenticated). A Checkpoint with no actor must
	// therefore still stamp the well-known SystemActorID so production
	// snapshots are never rejected on that ground.
	if fake.calledWith.ActorUserID != snapshot.SystemActorID {
		t.Fatalf("actor user id: got %q want %q", fake.calledWith.ActorUserID, snapshot.SystemActorID)
	}
}

// The snapshot arrives on an
// authenticated socket, so core's audit row must name the human who was
// editing instead of the synthetic collab-service actor.
func TestPublisher_ForwardsRealActorToCore(t *testing.T) {
	fake := &fakePolicyClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	const actor = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"
	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
		ActorUserID: actor,
	})

	if !out.Accepted {
		t.Fatalf("outcome: got %+v want accepted", out)
	}
	if fake.calledWith.ActorUserID != actor {
		t.Fatalf("actor user id: got %q want %q", fake.calledWith.ActorUserID, actor)
	}
	// The collab-side audit event must name the same human.
	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	if msgs[0].Event.ActorUserID != actor {
		t.Fatalf("audit actor: got %q want %q", msgs[0].Event.ActorUserID, actor)
	}
}

// TestPublisher_FallsBackWhenActorIsNotAUUID guards the whole checkpoint: core
// refuses a non-UUID actor_user_id with InvalidArgument, which would discard
// the edits entirely. Degrading attribution beats losing content.
func TestPublisher_FallsBackWhenActorIsNotAUUID(t *testing.T) {
	fake := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
		ActorUserID: "not-a-uuid",
	})

	if !out.Accepted {
		t.Fatalf("outcome: got %+v want accepted", out)
	}
	if fake.calledWith.ActorUserID != snapshot.SystemActorID {
		t.Fatalf("actor user id: got %q want the system actor", fake.calledWith.ActorUserID)
	}
}

// TestPublisher_OutcomeReportsRejection is what makes a rejected snapshot
// visible: the Outcome is the only thing the room can put on the wire, so it
// must carry both the machine reason and core's human-readable message.
func TestPublisher_OutcomeReportsRejection(t *testing.T) {
	fake := &fakePolicyClient{
		errWith: status.Error(codes.InvalidArgument, "content validation: section count mismatch"),
	}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	if out.Accepted {
		t.Fatal("outcome reported accepted for a rejected snapshot")
	}
	if out.Reason != snapshot.ReasonGRPCError {
		t.Errorf("reason: got %q want %q", out.Reason, snapshot.ReasonGRPCError)
	}
	// Core signals every content rejection as a gRPC error, so the message is
	// the only place the author's actual problem appears.
	if !strings.Contains(out.Detail, "section count mismatch") {
		t.Errorf("detail: got %q, want it to carry core's message", out.Detail)
	}
}

// TestPublisher_OutcomeReportsPreCheckFailure covers the local branch: a
// malformed payload never reaches core but must still be reported so the
// editor is not left believing its edits are safe.
func TestPublisher_OutcomeReportsPreCheckFailure(t *testing.T) {
	fake := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: "{not json}",
	})

	if out.Accepted || out.Reason != snapshot.ReasonPreCheckFailed {
		t.Fatalf("outcome: got %+v want pre_check_failed", out)
	}
	if fake.callCount != 0 {
		t.Fatalf("pre-check failure still called core %d time(s)", fake.callCount)
	}
}

func TestPublisher_LogsRejection(t *testing.T) {
	fake := &fakePolicyClient{rejectWith: "section count mismatch"}
	logs := &logCapture{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, logs.Logger())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	if !logs.Contains("section count mismatch") {
		t.Fatal("expected rejection reason in log")
	}
}

func TestPublisher_SkipsInvalidJSON(t *testing.T) {
	fake := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	// Pre-check rejects malformed JSON before calling the policy client.
	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: "{not json}",
	})

	if fake.calledWith != nil {
		t.Fatal("expected policy client NOT to be called for invalid JSON")
	}
	if fake.callCount != 0 {
		t.Fatalf("expected zero gRPC calls, got %d", fake.callCount)
	}
}

func TestPublisher_SkipsEmptyJSON(t *testing.T) {
	fake := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: "",
	})

	if fake.callCount != 0 {
		t.Fatalf("expected zero gRPC calls for empty input, got %d", fake.callCount)
	}
}

func TestPublisher_PropagatesGRPCErrorCode(t *testing.T) {
	// status.Code(err) must remain inspectable downstream (logged or surfaced
	// to operators) — the publisher logs but does not swallow the code.
	sentinel := status.Error(codes.Unavailable, "policy core unreachable")
	fake := &fakePolicyClient{errWith: sentinel}
	logs := &logCapture{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, logs.Logger())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	if fake.callCount != 1 {
		t.Fatalf("expected exactly one gRPC call, got %d", fake.callCount)
	}
	// The logged error should preserve the status code information so that
	// log scraping / metrics can distinguish transport failures from rejections.
	if !logs.Contains("Unavailable") && !logs.Contains("policy core unreachable") {
		t.Fatalf("expected gRPC error detail in logs, got: %v", logs.lines)
	}
}

// TestPublisher_GRPCErrorCodePreservedForCaller asserts that when the
// underlying client returns a gRPC status error, status.Code recovers the
// original code on the same error value the fake returned. This guards
// against future refactors that wrap the error in a way that loses code info.
func TestPublisher_GRPCErrorCodePreservedForCaller(t *testing.T) {
	sentinel := status.Error(codes.DeadlineExceeded, "deadline")
	fake := &fakePolicyClient{errWith: sentinel}

	// Direct check on the underlying interface — confirms our test fake
	// preserves the error verbatim. The Publisher itself does not re-throw,
	// but the contract is that the client returns errors unwrapped.
	_, _, err := fake.UpdateDraftContent(context.Background(), snapshot.SnapshotRequest{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("status code: got %v want DeadlineExceeded", got)
	}
}

// TestPublisher_EmitsAcceptedAuditEvent asserts that a successful snapshot
// publishes collab.snapshot.accepted with the policy_version subject and the
// expected attribute set. This is the happy path of the audit contract.
func TestPublisher_EmitsAcceptedAuditEvent(t *testing.T) {
	fake := &fakePolicyClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-7", PolicyID: "policy-7", TemplateVersionID: "tmpl-v4",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	got := msgs[0]
	if got.RoutingKey != "audit.audit" {
		t.Errorf("routing key: got %q want %q", got.RoutingKey, "audit.audit")
	}
	if got.Event.Action != "collab.snapshot.accepted" {
		t.Errorf("action: got %q want collab.snapshot.accepted", got.Event.Action)
	}
	if got.Event.Tier != audit.TierAudit {
		t.Errorf("tier: got %q want %q", got.Event.Tier, audit.TierAudit)
	}
	if got.Event.Subject != "policy_version:draft-7" {
		t.Errorf("subject: got %q want policy_version:draft-7", got.Event.Subject)
	}
	if got.Event.Attributes["policy_id"] != "policy-7" {
		t.Errorf("policy_id attr: got %q", got.Event.Attributes["policy_id"])
	}
	if got.Event.Attributes["draft_id"] != "draft-7" {
		t.Errorf("draft_id attr: got %q", got.Event.Attributes["draft_id"])
	}
	if got.Event.Attributes["template_version_id"] != "tmpl-v4" {
		t.Errorf("template_version_id attr: got %q", got.Event.Attributes["template_version_id"])
	}
}

// TestPublisher_EmitsRejectedAuditOnServerReject covers the case where the
// policy core returns accepted=false with a human reason — the audit row must
// carry reason=server_rejected and surface the server's detail string.
func TestPublisher_EmitsRejectedAuditOnServerReject(t *testing.T) {
	fake := &fakePolicyClient{rejectWith: "section count mismatch"}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-8", PolicyID: "policy-8", TemplateVersionID: "tmpl-v5",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	got := msgs[0]
	if got.Event.Action != "collab.snapshot.rejected" {
		t.Errorf("action: got %q want collab.snapshot.rejected", got.Event.Action)
	}
	if got.Event.Subject != "policy_version:draft-8" {
		t.Errorf("subject: got %q want policy_version:draft-8", got.Event.Subject)
	}
	if got.Event.Attributes["reason"] != "server_rejected" {
		t.Errorf("reason: got %q want server_rejected", got.Event.Attributes["reason"])
	}
	if got.Event.Attributes["detail"] != "section count mismatch" {
		t.Errorf("detail: got %q", got.Event.Attributes["detail"])
	}
}

// TestPublisher_EmitsRejectedAuditOnGRPCError exercises the transport-error
// branch. The reason attribute must distinguish this from a server-rejection
// so log-side dashboards can separate "policy core said no" from "policy core
// was unreachable" without parsing detail text.
func TestPublisher_EmitsRejectedAuditOnGRPCError(t *testing.T) {
	fake := &fakePolicyClient{errWith: status.Error(codes.Unavailable, "unreachable")}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-9", PolicyID: "policy-9", TemplateVersionID: "tmpl-v6",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})

	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	got := msgs[0]
	if got.Event.Action != "collab.snapshot.rejected" {
		t.Errorf("action: got %q want collab.snapshot.rejected", got.Event.Action)
	}
	if got.Event.Attributes["reason"] != "grpc_error" {
		t.Errorf("reason: got %q want grpc_error", got.Event.Attributes["reason"])
	}
	// detail carries the code AND core's message: core reports content
	// rejections as gRPC errors, so the message is where the real cause is.
	wantDetail := codes.Unavailable.String() + ": unreachable"
	if got.Event.Attributes["detail"] != wantDetail {
		t.Errorf("detail: got %q want %q", got.Event.Attributes["detail"], wantDetail)
	}
}

// TestPublisher_EmitsRejectedAuditOnPreCheckFail covers the local pre-check:
// malformed JSON must still produce an audit row (reason=pre_check_failed) so
// the audit trail records that a client tried to push bad content, even
// though no gRPC call was made.
func TestPublisher_EmitsRejectedAuditOnPreCheckFail(t *testing.T) {
	fake := &fakePolicyClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-10", PolicyID: "policy-10", TemplateVersionID: "tmpl-v7",
		ContentJSON: "{not json}",
	})

	if fake.callCount != 0 {
		t.Fatalf("expected zero gRPC calls when pre-check fails, got %d", fake.callCount)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	got := msgs[0]
	if got.Event.Action != "collab.snapshot.rejected" {
		t.Errorf("action: got %q want collab.snapshot.rejected", got.Event.Action)
	}
	if got.Event.Attributes["reason"] != "pre_check_failed" {
		t.Errorf("reason: got %q want pre_check_failed", got.Event.Attributes["reason"])
	}
	if got.Event.Subject != "policy_version:draft-10" {
		t.Errorf("subject: got %q want policy_version:draft-10", got.Event.Subject)
	}
}

// logCapture is a minimal zerolog sink for testing.
type logCapture struct{ lines []string }

func (l *logCapture) Write(p []byte) (int, error) {
	l.lines = append(l.lines, string(p))
	return len(p), nil
}

func (l *logCapture) Contains(s string) bool {
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			return true
		}
	}
	return false
}

func (l *logCapture) Logger() log.Logger {
	return log.NewLoggerWithOptions("collab", log.WithOutput(l), log.WithDefaultLevel(log.LevelTrace))
}

// A caller deciding whether to retry needs core's status code, not just the
// rendered detail: Unavailable is worth retrying, a content rejection is not.
func TestPublisher_OutcomeCarriesCoreStatusCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"content rejection", status.Error(codes.InvalidArgument, "content validation: section count mismatch"), codes.InvalidArgument},
		{"core unreachable", status.Error(codes.Unavailable, "connection refused"), codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			em, _ := newTestEmitter()
			p := snapshot.NewPublisherWithClient(&fakePolicyClient{errWith: tc.err}, em, log.Nop())
			out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
				DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
				ContentJSON: `{"root":{"type":"root","children":[]}}`,
			})
			if out.Code != tc.want {
				t.Fatalf("code: got %v want %v", out.Code, tc.want)
			}
		})
	}

	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(&fakePolicyClient{}, em, log.Nop())
	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
	})
	if out.Code != codes.OK {
		t.Fatalf("accepted outcome code: got %v want OK", out.Code)
	}
}

// During act-as the snapshot is made as the user acted as, and the real admin
// travels with it to core and is credited in collab's own audit event.
func TestPublisher_ActAsCarriesTheRealAdmin(t *testing.T) {
	fake := &fakePolicyClient{}
	em, fap := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())

	const target = "7f9c1a2e-3b4d-4c5e-8f60-112233445566"
	const admin = "0b1c2d3e-4f50-4617-8899-aabbccddeeff"
	out := p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1", TemplateVersionID: "tmpl-v1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
		ActorUserID: target, Impersonator: admin,
	})
	if !out.Accepted {
		t.Fatalf("outcome: got %+v want accepted", out)
	}
	if fake.calledWith.ActorUserID != target || fake.calledWith.Impersonator != admin {
		t.Fatalf("core request: actor %q impersonator %q", fake.calledWith.ActorUserID, fake.calledWith.Impersonator)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	if msgs[0].Event.ActorUserID != admin || msgs[0].Event.Attributes["impersonated_user_id"] != target {
		t.Fatalf("audit: actor %q impersonated %q", msgs[0].Event.ActorUserID, msgs[0].Event.Attributes["impersonated_user_id"])
	}
}

// The system fallback acts as nobody, so an impersonator is dropped with it.
func TestPublisher_FallbackActorDropsTheImpersonator(t *testing.T) {
	fake := &fakePolicyClient{}
	em, _ := newTestEmitter()
	p := snapshot.NewPublisherWithClient(fake, em, log.Nop())
	p.SnapshotJSON(context.Background(), snapshot.Checkpoint{
		DraftID: "draft-1", PolicyID: "policy-1",
		ContentJSON: `{"root":{"type":"root","children":[]}}`,
		ActorUserID: "not-a-uuid", Impersonator: "0b1c2d3e-4f50-4617-8899-aabbccddeeff",
	})
	if fake.calledWith.ActorUserID != snapshot.SystemActorID || fake.calledWith.Impersonator != "" {
		t.Fatalf("core request: actor %q impersonator %q", fake.calledWith.ActorUserID, fake.calledWith.Impersonator)
	}
}
