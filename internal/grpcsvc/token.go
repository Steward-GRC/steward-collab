// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package grpcsvc implements the collab service's gRPC handlers.
//
// IssueToken mints the short-lived HS256 token a browser opens a room with;
// ws.Handler verifies it with the same secret. The claims, audience and
// algorithm must stay in step with ws.CollabClaims and ws.TokenAudience.
package grpcsvc

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/errcodes"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/Steward-GRC/steward-collab/internal/ws"
)

// DefaultTokenTTL is long enough to ride out a network blip or a tab regaining
// focus, and short enough that a leaked token is mostly useless by the time it
// surfaces.
const DefaultTokenTTL = 5 * time.Minute

// WsPathPrefix is the path the gateway serves its collab websocket proxy on;
// it forwards the upgrade to collab's own /ws/{draftID}. It must match the
// gateway's route, or every editor session fails at the edge.
const WsPathPrefix = "/collab/ws"

// CollabTokenClaims are the claims of a minted token. The JSON names must match
// ws.CollabClaims exactly.
type CollabTokenClaims struct {
	jwt.RegisteredClaims
	UserID            string `json:"uid"`
	PolicyID          string `json:"pid"`
	DraftID           string `json:"did"`
	TemplateVersionID string `json:"tvid"`
	DisplayName       string `json:"name,omitempty"`
	Impersonator      string `json:"imp,omitempty"`
}

// EditChecker decides whether a user may co-edit a policy's draft.
// *access.Checker implements it.
type EditChecker interface {
	CanEdit(ctx context.Context, userID, policyID string) (access.Result, error)
}

// TokenOptions configure the token service.
type TokenOptions struct {
	Secret []byte
	// TTL is DefaultTokenTTL when zero.
	TTL time.Duration
	// WsBase is an origin put in front of the websocket path, for a web app
	// not served from the gateway's origin. Empty gives a same-origin path.
	WsBase string
	// Appendices looks an appendix up for IssueAppendixToken.
	Appendices access.AppendixReader
}

type tokenService struct {
	collabv1.UnimplementedCollabTokenServiceServer
	opts    TokenOptions
	now     func() time.Time
	edit    EditChecker
	auditor *audit.Emitter
	logger  log.Logger
}

// NewTokenService returns the CollabTokenService handler.
func NewTokenService(opts TokenOptions, edit EditChecker, auditor *audit.Emitter, logger log.Logger) collabv1.CollabTokenServiceServer {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTokenTTL
	}
	return &tokenService{opts: opts, now: time.Now, edit: edit, auditor: auditor, logger: logger}
}

// RegisterTokenService registers the handler on s.
func RegisterTokenService(s grpc.ServiceRegistrar, svc collabv1.CollabTokenServiceServer) {
	collabv1.RegisterCollabTokenServiceServer(s, svc)
}

// IssueToken checks that the caller may edit the draft and mints a token for
// the caller's go-grpc-actor actor. During act-as that is the user acted as,
// who must be allowed to edit, and the token carries the real admin in imp.
// The template version is optional: a freeform draft has none, and an empty
// value is carried to core unchanged.
//
// A draft id in the appendix room space ("appendix:...") is refused: the room
// key decides which core call a checkpoint goes to, so a policy token must not
// be able to name an appendix room.
func (svc *tokenService) IssueToken(ctx context.Context, req *collabv1.IssueTokenRequest) (*collabv1.IssueTokenResponse, error) {
	actor, err := grpcactor.Require(ctx, errcodes.CodeActorRequired)
	if err != nil {
		return nil, errcodes.Error(ctx, err)
	}
	if req.GetPolicyId() == "" {
		return nil, errcodes.New(ctx, errcodes.CodePolicyIDRequired)
	}
	if req.GetDraftId() == "" {
		return nil, errcodes.New(ctx, errcodes.CodeDraftIDRequired)
	}
	l := svc.logger.Ctx(ctx)
	if strings.HasPrefix(req.GetDraftId(), snapshot.AppendixRoomPrefix) {
		l.Warn("policy token refused: draft id is in the appendix room space", log.F("policy_id", req.GetPolicyId()), log.F("user_id", actor.Subject))
		return nil, errcodes.New(ctx, errcodes.CodeDraftIDReserved)
	}

	decision, err := svc.authorize(ctx, actor, req.GetPolicyId())
	if err != nil {
		return nil, err
	}

	signed, exp, err := svc.mint(ctx, actor, decision, CollabTokenClaims{
		PolicyID:          req.GetPolicyId(),
		DraftID:           req.GetDraftId(),
		TemplateVersionID: req.GetTemplateVersionId(),
	})
	if err != nil {
		return nil, err
	}

	// The token itself is never audited, only that one was issued.
	svc.auditIssued(ctx, actor, "policy:"+req.GetPolicyId(), map[string]string{
		"policy_id":           req.GetPolicyId(),
		"draft_id":            req.GetDraftId(),
		"template_version_id": req.GetTemplateVersionId(),
		"expires_at":          exp.Format(time.RFC3339),
	})
	l.Debug("collab token issued", log.F("room_kind", "policy"), log.F("policy_id", req.GetPolicyId()), log.F("draft_id", req.GetDraftId()))

	return &collabv1.IssueTokenResponse{
		Token:     signed,
		WsUrl:     wsURL(svc.opts.WsBase, req.GetDraftId()),
		ExpiresAt: timestamppb.New(exp),
	}, nil
}

// IssueAppendixToken mints the token for an appendix's own room,
// "appendix:<appendix_id>". It checks what IssueToken does, against the policy
// the appendix belongs to (read from core, so the caller can't pair an appendix
// with a policy it may edit): the caller must own the policy or hold an author
// grant for it. Order matters for what a reader learns: an unknown appendix is
// NotFound, then the edit check, then a retired appendix is FailedPrecondition.
func (svc *tokenService) IssueAppendixToken(ctx context.Context, req *collabv1.IssueAppendixTokenRequest) (*collabv1.IssueAppendixTokenResponse, error) {
	actor, err := grpcactor.Require(ctx, errcodes.CodeActorRequired)
	if err != nil {
		return nil, errcodes.Error(ctx, err)
	}
	appendixID := req.GetAppendixId()
	if appendixID == "" {
		return nil, errcodes.New(ctx, errcodes.CodeAppendixIDRequired)
	}
	l := svc.logger.Ctx(ctx)
	if svc.opts.Appendices == nil {
		l.Error(nil, "appendix token requested but no appendix lookup is wired", log.F("appendix_id", appendixID))
		return nil, errcodes.New(ctx, errcodes.CodeEditCheckUnavailable)
	}

	info, err := svc.opts.Appendices.GetAppendix(ctx, appendixID)
	switch {
	case errors.Is(err, access.ErrAppendixNotFound):
		l.Info("appendix token refused: no such appendix", log.F("appendix_id", appendixID))
		return nil, errcodes.New(ctx, errcodes.CodeAppendixNotFound, "appendix_id", appendixID)
	case errors.Is(err, access.ErrUnavailable):
		l.Warn("appendix lookup could not reach core", log.F("error", err.Error()), log.F("appendix_id", appendixID))
		return nil, errcodes.New(ctx, errcodes.CodeEditCheckUnavailable)
	case err != nil:
		l.Error(err, "appendix lookup failed", log.F("appendix_id", appendixID))
		return nil, errcodes.Error(ctx, err)
	}
	l.Debug("appendix found for token", log.F("appendix_id", appendixID), log.F("policy_id", info.PolicyID), log.F("retired", info.Retired))

	decision, err := svc.authorize(ctx, actor, info.PolicyID)
	if err != nil {
		return nil, err
	}
	if info.Retired {
		l.Info("appendix token refused: appendix is retired", log.F("appendix_id", appendixID), log.F("user_id", actor.Subject))
		return nil, errcodes.New(ctx, errcodes.CodeAppendixRetired, "appendix_id", appendixID)
	}

	roomKey := snapshot.AppendixRoomKey(appendixID)
	signed, exp, err := svc.mint(ctx, actor, decision, CollabTokenClaims{PolicyID: info.PolicyID, DraftID: roomKey})
	if err != nil {
		return nil, err
	}

	svc.auditIssued(ctx, actor, roomKey, map[string]string{
		"policy_id":   info.PolicyID,
		"appendix_id": appendixID,
		"room_key":    roomKey,
		"expires_at":  exp.Format(time.RFC3339),
	})
	l.Debug("collab token issued", log.F("room_kind", "appendix"), log.F("policy_id", info.PolicyID), log.F("appendix_id", appendixID))

	return &collabv1.IssueAppendixTokenResponse{
		Token:     signed,
		WsUrl:     wsURL(svc.opts.WsBase, roomKey),
		ExpiresAt: timestamppb.New(exp),
		RoomKey:   roomKey,
		PolicyId:  info.PolicyID,
	}, nil
}

// authorize runs the edit check for actor on policyID and maps its refusals to
// coded errors.
func (svc *tokenService) authorize(ctx context.Context, actor grpcactor.Actor, policyID string) (access.Result, error) {
	l := svc.logger.Ctx(ctx)
	decision, err := svc.edit.CanEdit(ctx, actor.Subject, policyID)
	switch {
	case errors.Is(err, access.ErrPolicyNotFound):
		return access.Result{}, errcodes.New(ctx, errcodes.CodePolicyNotFound, "policy_id", policyID)
	case errors.Is(err, access.ErrUnavailable):
		l.Warn("edit check could not reach a peer", log.F("error", err.Error()), log.F("policy_id", policyID))
		return access.Result{}, errcodes.New(ctx, errcodes.CodeEditCheckUnavailable)
	case err != nil:
		l.Error(err, "edit check failed", log.F("policy_id", policyID))
		return access.Result{}, errcodes.Error(ctx, err)
	case !decision.Allowed:
		l.Info("co-editing refused", log.F("policy_id", policyID), log.F("user_id", actor.Subject))
		return access.Result{}, errcodes.New(ctx, errcodes.CodeDraftEditForbidden, "policy_id", policyID)
	}
	return decision, nil
}

// mint signs the token for claims, adding the actor, the audience and the
// times.
func (svc *tokenService) mint(ctx context.Context, actor grpcactor.Actor, decision access.Result, claims CollabTokenClaims) (string, time.Time, error) {
	now := svc.now().UTC()
	exp := now.Add(svc.opts.TTL)
	claims.RegisteredClaims = jwt.RegisteredClaims{
		Audience:  jwt.ClaimStrings{ws.TokenAudience},
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
	}
	claims.UserID = actor.Subject
	claims.DisplayName = decision.Name
	claims.Impersonator = actor.Impersonator
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(svc.opts.Secret)
	if err != nil {
		svc.logger.Ctx(ctx).Error(err, "sign collab token")
		return "", time.Time{}, errcodes.Error(ctx, err)
	}
	return signed, exp, nil
}

// auditIssued records that a token was issued; an audit failure is logged and
// never fails the call.
func (svc *tokenService) auditIssued(ctx context.Context, actor grpcactor.Actor, subject string, attrs map[string]string) {
	ev := audit.Event{
		Tier:        audit.TierAudit,
		Action:      "collab.token.issued",
		ActorUserID: actor.Subject,
		Subject:     subject,
		Attributes:  attrs,
	}
	if actor.Impersonated() {
		ev = ev.ActedAs(actor.Impersonator)
	}
	if err := svc.auditor.Emit(ctx, ev); err != nil {
		svc.logger.Ctx(ctx).Warn("emit collab.token.issued", log.F("error", err.Error()))
	}
}

// wsURL is "<base><WsPathPrefix>/<roomKey>". The key is path-escaped, so an odd
// id can't add a path segment or query to the URL a browser dials, and its
// colon is escaped too (an appendix room is "appendix%3A<id>", as the mock's
// encodeURIComponent gives it). The handler reads the decoded path.
func wsURL(base, roomKey string) string {
	return strings.TrimSuffix(base, "/") + WsPathPrefix + "/" + strings.ReplaceAll(url.PathEscape(roomKey), ":", "%3A")
}
