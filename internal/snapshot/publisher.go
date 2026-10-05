// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package snapshot sends the Lexical EditorState JSON a client exported from a
// live room to core's PolicyService.UpdateDraftContent, which validates it and
// keeps it as the draft's content. The relay decodes no Yjs: it forwards the
// client's JSON after a cheap structural pre-check.
//
// A failed snapshot never breaks the room. It comes back as an Outcome that the
// room pushes to its editors, so an edit that didn't persist is never silent.
package snapshot

import (
	"context"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-collab/internal/audit"
)

// SystemActorID is the actor stamped on a snapshot that can't name a person.
// Core refuses UpdateDraftContent without an actor, and losing the edits is
// worse than losing the attribution. It is fixed, so audit searches can find
// the snapshots collab couldn't attribute.
const SystemActorID = "00000000-0000-0000-0000-0000000c011a"

// SnapshotRequest is what the Publisher sends to core.
type SnapshotRequest struct {
	PolicyID          string
	DraftID           string
	ContentJSON       string
	TemplateVersionID string
	// ActorUserID is the person the snapshot is attributed to; during act-as,
	// the user acted as.
	ActorUserID string
	// Impersonator is the real admin during act-as, otherwise empty.
	Impersonator string
}

// PolicyClient is the part of core the Publisher needs.
type PolicyClient interface {
	UpdateDraftContent(ctx context.Context, req SnapshotRequest) (accepted bool, rejectReason string, err error)
}

type grpcPolicyClient struct {
	client corev1.PolicyServiceClient
}

// UpdateDraftContent puts the actor on the call, so go-grpc-actor's client
// interceptor carries it (and during act-as the real admin) to core. The
// system actor is a fallback, not a person, so it travels in the request only.
func (g *grpcPolicyClient) UpdateDraftContent(ctx context.Context, req SnapshotRequest) (bool, string, error) {
	if req.ActorUserID != SystemActorID {
		ctx = grpcactor.WithActor(ctx, grpcactor.Actor{Subject: req.ActorUserID, Impersonator: req.Impersonator})
	}
	resp, err := g.client.UpdateDraftContent(ctx, &corev1.UpdateDraftContentRequest{
		PolicyId:          req.PolicyID,
		DraftId:           req.DraftID,
		ContentJson:       req.ContentJSON,
		TemplateVersionId: req.TemplateVersionID,
		ActorUserId:       req.ActorUserID,
	})
	if err != nil {
		return false, "", err
	}
	return resp.GetAccepted(), resp.GetRejectReason(), nil
}

// Publisher forwards checkpoints to core and audits each attempt:
// collab.snapshot.accepted, or collab.snapshot.rejected for a pre-check
// failure, a failed call or a refusal. Both name policy_version:<draft_id>, the
// subject core uses for draft writes.
type Publisher struct {
	client  PolicyClient
	auditor *audit.Emitter
	logger  log.Logger
}

// NewPublisher returns a Publisher on a connection to core.
func NewPublisher(conn grpc.ClientConnInterface, auditor *audit.Emitter, logger log.Logger) *Publisher {
	return NewPublisherWithClient(&grpcPolicyClient{client: corev1.NewPolicyServiceClient(conn)}, auditor, logger)
}

// NewPublisherWithClient returns a Publisher on client.
func NewPublisherWithClient(client PolicyClient, auditor *audit.Emitter, logger log.Logger) *Publisher {
	return &Publisher{client: client, auditor: auditor, logger: logger}
}

// Checkpoint is one client snapshot of a draft.
type Checkpoint struct {
	DraftID           string
	PolicyID          string
	TemplateVersionID string
	// ContentJSON is the Lexical EditorState JSON, the draft's content.
	ContentJSON string
	// ActorUserID is the token user of the connection that sent it. Empty
	// falls back to SystemActorID.
	ActorUserID string
	// Impersonator is the token's real admin during act-as.
	Impersonator string
}

// Outcome is what happened to a Checkpoint. A refusal is a normal outcome, not
// an error.
//
// Reason, when Accepted is false:
//   - pre_check_failed: the JSON never left the process; Detail is why.
//   - grpc_error: the call to core failed; Detail is "Code: message". Core
//     reports content refusals this way, so Detail holds the author's problem.
//   - server_rejected: core answered accepted=false with Detail as its reason.
//
// Code is core's status code for grpc_error and OK otherwise, so a caller can
// tell "core was unreachable" from "core refused the content".
type Outcome struct {
	Accepted bool
	Reason   string
	Detail   string
	Code     codes.Code
}

// Outcome.Reason values.
const (
	ReasonPreCheckFailed = "pre_check_failed"
	ReasonGRPCError      = "grpc_error"
	ReasonServerRejected = "server_rejected"
)

// SnapshotJSON pre-checks the checkpoint's JSON, sends it to core and reports
// the result. It never returns an error: the caller relays the Outcome to the
// editors instead.
func (p *Publisher) SnapshotJSON(ctx context.Context, cp Checkpoint) Outcome {
	actor := p.actorFor(cp)
	if err := ValidateLexicalJSON(cp.ContentJSON); err != nil {
		p.logger.Warn("snapshot pre-check rejected content", log.F("error", err.Error()), log.F("draft_id", cp.DraftID), log.F("policy_id", cp.PolicyID))
		return p.rejected(ctx, cp, actor, ReasonPreCheckFailed, err.Error())
	}

	accepted, reason, err := p.client.UpdateDraftContent(ctx, SnapshotRequest{
		PolicyID:          cp.PolicyID,
		DraftID:           cp.DraftID,
		ContentJSON:       cp.ContentJSON,
		TemplateVersionID: cp.TemplateVersionID,
		ActorUserID:       actor,
		Impersonator:      p.impersonatorFor(cp, actor),
	})
	if err != nil {
		p.logger.Error(err, "policy update draft content", log.F("draft_id", cp.DraftID), log.F("policy_id", cp.PolicyID), log.F("grpc_code", status.Code(err).String()))
		out := p.rejected(ctx, cp, actor, ReasonGRPCError, grpcDetail(err))
		out.Code = status.Code(err)
		return out
	}
	if !accepted {
		p.logger.Warn("snapshot rejected by policy service", log.F("draft_id", cp.DraftID), log.F("policy_id", cp.PolicyID), log.F("reason", reason))
		return p.rejected(ctx, cp, actor, ReasonServerRejected, reason)
	}
	p.logger.Debug("snapshot accepted", log.F("draft_id", cp.DraftID), log.F("policy_id", cp.PolicyID))
	p.emit(ctx, cp, actor, "collab.snapshot.accepted", nil)
	return Outcome{Accepted: true}
}

// actorFor falls back to SystemActorID for an absent or non-UUID actor: core
// refuses both, which would lose the checkpoint. A non-UUID means the token
// path changed, so it is logged.
func (p *Publisher) actorFor(cp Checkpoint) string {
	if cp.ActorUserID == "" {
		return SystemActorID
	}
	if err := uuid.Validate(cp.ActorUserID); err != nil {
		p.logger.Warn("snapshot actor is not a UUID; attributing to the collab service actor",
			log.F("draft_id", cp.DraftID), log.F("actor_user_id", cp.ActorUserID))
		return SystemActorID
	}
	return cp.ActorUserID
}

// impersonatorFor drops the impersonator once the actor fell back: the system
// actor acts as nobody.
func (p *Publisher) impersonatorFor(cp Checkpoint, actor string) string {
	if actor == SystemActorID {
		return ""
	}
	return cp.Impersonator
}

func grpcDetail(err error) string {
	st := status.Convert(err)
	if msg := st.Message(); msg != "" {
		return st.Code().String() + ": " + msg
	}
	return st.Code().String()
}

func (p *Publisher) rejected(ctx context.Context, cp Checkpoint, actor, reason, detail string) Outcome {
	p.emit(ctx, cp, actor, "collab.snapshot.rejected", map[string]string{"reason": reason, "detail": detail})
	return Outcome{Reason: reason, Detail: detail}
}

// emit audits one attempt. During act-as the event names the real admin and
// keeps the user acted as in impersonated_user_id. An audit failure is logged
// and never fails the snapshot.
func (p *Publisher) emit(ctx context.Context, cp Checkpoint, actor, action string, extra map[string]string) {
	attrs := map[string]string{
		"policy_id":           cp.PolicyID,
		"draft_id":            cp.DraftID,
		"template_version_id": cp.TemplateVersionID,
	}
	for k, v := range extra {
		attrs[k] = v
	}
	ev := audit.Event{
		Tier:        audit.TierAudit,
		Action:      action,
		ActorUserID: actor,
		Subject:     "policy_version:" + cp.DraftID,
		Attributes:  attrs,
	}
	if imp := p.impersonatorFor(cp, actor); imp != "" {
		ev = ev.ActedAs(imp)
	}
	if err := p.auditor.Emit(ctx, ev); err != nil {
		p.logger.Warn("emit "+action, log.F("error", err.Error()), log.F("draft_id", cp.DraftID))
	}
}
