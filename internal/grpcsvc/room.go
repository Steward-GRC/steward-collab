// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"errors"
	"strconv"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/errcodes"
	"github.com/Steward-GRC/steward-collab/internal/ws"
)

// RoomRegistry is the part of *ws.Hub the room service needs. It can look a
// live room up, flush it and freeze it, but never create one or touch its
// document.
type RoomRegistry interface {
	LookupRoom(draftID string) (ws.RoomInfo, bool)
	FlushDraft(ctx context.Context, draftID string) (ws.FlushResult, error)
	MarkDraftPublished(ctx context.Context, draftID string, versionNumber int32) bool
}

var _ RoomRegistry = (*ws.Hub)(nil)

type roomService struct {
	collabv1.UnimplementedCollabRoomServiceServer
	rooms   RoomRegistry
	auditor *audit.Emitter
	logger  log.Logger
}

// NewRoomService returns the CollabRoomService handler over rooms, which must
// be the same hub the websocket handler serves, or a freeze reaches no editor.
func NewRoomService(rooms RoomRegistry, auditor *audit.Emitter, logger log.Logger) collabv1.CollabRoomServiceServer {
	return &roomService{rooms: rooms, auditor: auditor, logger: logger}
}

// RegisterRoomService registers the handler on s.
func RegisterRoomService(s grpc.ServiceRegistrar, svc collabv1.CollabRoomServiceServer) {
	collabv1.RegisterCollabRoomServiceServer(s, svc)
}

// checkRequest requires an actor and both ids.
func checkRequest(ctx context.Context, draftID, policyID string) (grpcactor.Actor, error) {
	actor, err := grpcactor.Require(ctx, errcodes.CodeActorRequired)
	if err != nil {
		return actor, errcodes.Error(ctx, err)
	}
	if draftID == "" {
		return actor, errcodes.New(ctx, errcodes.CodeDraftIDRequired)
	}
	if policyID == "" {
		return actor, errcodes.New(ctx, errcodes.CodePolicyIDRequired)
	}
	return actor, nil
}

// liveRoom returns the live room, or false when there is none (the normal
// case). A room of another policy is refused, so a wrong id can't flush or
// freeze an unrelated draft.
func (svc *roomService) liveRoom(ctx context.Context, draftID, policyID string) (ws.RoomInfo, bool, error) {
	info, live := svc.rooms.LookupRoom(draftID)
	if !live {
		return info, false, nil
	}
	if info.PolicyID != policyID {
		svc.logger.Ctx(ctx).Error(nil, "policy_id does not match the live room; refusing it",
			log.F("draft_id", draftID), log.F("request_policy_id", policyID), log.F("room_policy_id", info.PolicyID))
		return info, false, errcodes.New(ctx, errcodes.CodeRoomPolicyMismatch, "draft_id", draftID)
	}
	return info, true, nil
}

// FlushDraft sends the room's newest checkpoint to core and returns once core
// has answered. Any error must stop the publish, or the version is cut from
// content older than the room holds. It authorizes the same way as
// NotifyDraftPublished: it only persists content an editor already sent.
func (svc *roomService) FlushDraft(ctx context.Context, req *collabv1.FlushDraftRequest) (*collabv1.FlushDraftResponse, error) {
	if _, err := checkRequest(ctx, req.GetDraftId(), req.GetPolicyId()); err != nil {
		return nil, err
	}
	_, live, err := svc.liveRoom(ctx, req.GetDraftId(), req.GetPolicyId())
	if err != nil {
		return nil, err
	}
	if !live {
		return &collabv1.FlushDraftResponse{}, nil
	}
	l := svc.logger.Ctx(ctx)
	res, err := svc.rooms.FlushDraft(ctx, req.GetDraftId())
	if err != nil {
		l.Warn("collab room flush not accepted by core", log.F("error", err.Error()),
			log.F("draft_id", req.GetDraftId()), log.F("policy_id", req.GetPolicyId()))
		return nil, flushError(ctx, err)
	}
	l.Debug("collab room flushed before publish", log.F("draft_id", req.GetDraftId()),
		log.F("room_found", res.RoomFound), log.F("content_flushed", res.ContentFlushed))
	return &collabv1.FlushDraftResponse{RoomFound: res.RoomFound, ContentFlushed: res.ContentFlushed}, nil
}

func flushError(ctx context.Context, err error) error {
	var fe *ws.FlushError
	if !errors.As(err, &fe) {
		return errcodes.Error(ctx, err)
	}
	switch fe.Code {
	case codes.Unavailable, codes.Canceled:
		return errcodes.New(ctx, errcodes.CodeFlushUnavailable, "detail", fe.Detail)
	case codes.DeadlineExceeded:
		return errcodes.New(ctx, errcodes.CodeFlushTimeout, "detail", fe.Detail)
	}
	return errcodes.New(ctx, errcodes.CodeFlushRejected, "reason", fe.Reason, "detail", fe.Detail)
}

// NotifyDraftPublished latches the room read-only, flushes anything still
// pending and pushes draft.published to every editor. The gateway calls it
// after core confirms the publish, so a failed publish never leaves a room
// stuck read-only.
//
// It checks the actor only, not the edit check IssueToken makes: the gateway's
// publish check is the authority, and a stricter check here could only skip
// the freeze after a publish that already happened. The frame takes editing
// away and grants nothing, and the policy id must match the room's.
func (svc *roomService) NotifyDraftPublished(ctx context.Context, req *collabv1.NotifyDraftPublishedRequest) (*collabv1.NotifyDraftPublishedResponse, error) {
	actor, err := checkRequest(ctx, req.GetDraftId(), req.GetPolicyId())
	if err != nil {
		return nil, err
	}
	if req.GetVersionNumber() <= 0 {
		return nil, errcodes.New(ctx, errcodes.CodeVersionNumberInvalid, "version_number", strconv.Itoa(int(req.GetVersionNumber())))
	}
	info, live, err := svc.liveRoom(ctx, req.GetDraftId(), req.GetPolicyId())
	if err != nil {
		return nil, err
	}
	l := svc.logger.Ctx(ctx)
	if !live || !svc.rooms.MarkDraftPublished(ctx, req.GetDraftId(), req.GetVersionNumber()) {
		l.Debug("draft published with no live collab room; nothing to notify", log.F("draft_id", req.GetDraftId()))
		return &collabv1.NotifyDraftPublishedResponse{}, nil
	}
	l.Info("draft published; collab room frozen read-only", log.F("draft_id", req.GetDraftId()),
		log.F("policy_id", req.GetPolicyId()), log.F("version_number", req.GetVersionNumber()), log.F("editors", info.Editors))

	// The event is about the room; core already audits the publish itself.
	ev := audit.Event{
		Tier:        audit.TierAudit,
		Action:      "collab.room.frozen",
		ActorUserID: actor.Subject,
		Subject:     "policy:" + req.GetPolicyId(),
		Attributes: map[string]string{
			"policy_id":      req.GetPolicyId(),
			"draft_id":       req.GetDraftId(),
			"version_number": strconv.Itoa(int(req.GetVersionNumber())),
			"editors":        strconv.Itoa(info.Editors),
		},
	}
	if actor.Impersonated() {
		ev = ev.ActedAs(actor.Impersonator)
	}
	if err := svc.auditor.Emit(ctx, ev); err != nil {
		l.Warn("emit collab.room.frozen", log.F("error", err.Error()))
	}
	return &collabv1.NotifyDraftPublishedResponse{RoomNotified: true}, nil
}
