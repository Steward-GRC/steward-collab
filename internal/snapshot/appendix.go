// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"strings"

	log "github.com/Bugs5382/go-log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AppendixRoomPrefix starts the room key of an appendix room: "appendix:<id>".
// A policy room is keyed by its draft version's id, a UUID, so the two key
// spaces cannot meet; IssueToken refuses a draft id with this prefix so a
// policy token cannot name an appendix room either.
const AppendixRoomPrefix = "appendix:"

// Room kinds, as logged. Ids are opaque; content is never logged.
const (
	kindPolicy   = "policy"
	kindAppendix = "appendix"
)

// AppendixRoomKey is the room key of appendix id.
func AppendixRoomKey(id string) string { return AppendixRoomPrefix + id }

// AppendixIDFromRoomKey returns the appendix id of an appendix room key. ok is
// false for any other key, and for an appendix key with no id.
func AppendixIDFromRoomKey(key string) (id string, ok bool) {
	id, ok = strings.CutPrefix(key, AppendixRoomPrefix)
	if !ok || id == "" {
		return "", false
	}
	return id, true
}

// AppendixRequest is what the Publisher sends to core for an appendix room.
type AppendixRequest struct {
	AppendixID  string
	PolicyID    string
	ContentJSON string
	// ActorUserID is the person the checkpoint is attributed to; during
	// act-as, the user acted as.
	ActorUserID string
	// Impersonator is the real admin during act-as, otherwise empty.
	Impersonator string
}

// AppendixClient is the part of core the Publisher needs for an appendix room:
// PolicyAppendixService.CheckpointAppendixDraft. Core validates the content,
// keeps the appendix's title, makes no draft when the content equals the
// published row, updates the open draft or starts the next, refuses a retired
// appendix and records the actor as a contributor.
type AppendixClient interface {
	CheckpointAppendixDraft(ctx context.Context, req AppendixRequest) (accepted bool, rejectReason string, err error)
}

// WithAppendixClient sets the core call appendix rooms use and returns p. A
// Publisher without one answers an appendix checkpoint with a grpc_error
// (Unimplemented) and never sends it down the policy path.
func (p *Publisher) WithAppendixClient(c AppendixClient) *Publisher {
	p.appendix = c
	return p
}

// checkpointToCore sends cp to the call for its room kind.
func (p *Publisher) checkpointToCore(ctx context.Context, cp Checkpoint, actor string) (accepted bool, reason string, err error) {
	if appendixID, ok := AppendixIDFromRoomKey(cp.DraftID); ok {
		if p.appendix == nil {
			p.logger.Error(nil, "appendix checkpoint with no appendix client wired", log.F("room_kind", kindAppendix), log.F("appendix_id", appendixID))
			return false, "", status.Error(codes.Unimplemented, "appendix checkpoints are not available")
		}
		p.logger.Debug("sending appendix checkpoint to core", log.F("room_kind", kindAppendix), log.F("appendix_id", appendixID), log.F("policy_id", cp.PolicyID))
		return p.appendix.CheckpointAppendixDraft(ctx, AppendixRequest{
			AppendixID:   appendixID,
			PolicyID:     cp.PolicyID,
			ContentJSON:  cp.ContentJSON,
			ActorUserID:  actor,
			Impersonator: p.impersonatorFor(cp, actor),
		})
	}
	p.logger.Debug("sending policy checkpoint to core", log.F("room_kind", kindPolicy), log.F("draft_id", cp.DraftID), log.F("policy_id", cp.PolicyID))
	return p.client.UpdateDraftContent(ctx, SnapshotRequest{
		PolicyID:          cp.PolicyID,
		DraftID:           cp.DraftID,
		ContentJSON:       cp.ContentJSON,
		TemplateVersionID: cp.TemplateVersionID,
		ActorUserID:       actor,
		Impersonator:      p.impersonatorFor(cp, actor),
	})
}

// roomKind names the kind of room cp belongs to, for logs.
func roomKind(cp Checkpoint) string {
	if _, ok := AppendixIDFromRoomKey(cp.DraftID); ok {
		return kindAppendix
	}
	return kindPolicy
}
