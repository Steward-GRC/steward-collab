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

	decision, err := svc.edit.CanEdit(ctx, actor.Subject, req.GetPolicyId())
	switch {
	case errors.Is(err, access.ErrPolicyNotFound):
		return nil, errcodes.New(ctx, errcodes.CodePolicyNotFound, "policy_id", req.GetPolicyId())
	case errors.Is(err, access.ErrUnavailable):
		l.Warn("edit check could not reach a peer", log.F("error", err.Error()), log.F("policy_id", req.GetPolicyId()))
		return nil, errcodes.New(ctx, errcodes.CodeEditCheckUnavailable)
	case err != nil:
		l.Error(err, "edit check failed", log.F("policy_id", req.GetPolicyId()))
		return nil, errcodes.Error(ctx, err)
	case !decision.Allowed:
		l.Info("co-editing refused", log.F("policy_id", req.GetPolicyId()), log.F("user_id", actor.Subject))
		return nil, errcodes.New(ctx, errcodes.CodeDraftEditForbidden, "policy_id", req.GetPolicyId())
	}

	now := svc.now().UTC()
	exp := now.Add(svc.opts.TTL)
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, CollabTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{ws.TokenAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
		UserID:            actor.Subject,
		PolicyID:          req.GetPolicyId(),
		DraftID:           req.GetDraftId(),
		TemplateVersionID: req.GetTemplateVersionId(),
		DisplayName:       decision.Name,
		Impersonator:      actor.Impersonator,
	}).SignedString(svc.opts.Secret)
	if err != nil {
		l.Error(err, "sign collab token")
		return nil, errcodes.Error(ctx, err)
	}

	// The token itself is never audited, only that one was issued.
	ev := audit.Event{
		Tier:        audit.TierAudit,
		Action:      "collab.token.issued",
		ActorUserID: actor.Subject,
		Subject:     "policy:" + req.GetPolicyId(),
		Attributes: map[string]string{
			"policy_id":           req.GetPolicyId(),
			"draft_id":            req.GetDraftId(),
			"template_version_id": req.GetTemplateVersionId(),
			"expires_at":          exp.Format(time.RFC3339),
		},
	}
	if actor.Impersonated() {
		ev = ev.ActedAs(actor.Impersonator)
	}
	if err := svc.auditor.Emit(ctx, ev); err != nil {
		l.Warn("emit collab.token.issued", log.F("error", err.Error()))
	}
	l.Debug("collab token issued", log.F("policy_id", req.GetPolicyId()), log.F("draft_id", req.GetDraftId()))

	return &collabv1.IssueTokenResponse{
		Token:     signed,
		WsUrl:     wsURL(svc.opts.WsBase, req.GetDraftId()),
		ExpiresAt: timestamppb.New(exp),
	}, nil
}

// wsURL is "<base><WsPathPrefix>/<draftID>". The draft id is path-escaped, so
// an odd id can't add a path segment or query to the URL a browser dials.
func wsURL(base, draftID string) string {
	return strings.TrimSuffix(base, "/") + WsPathPrefix + "/" + url.PathEscape(draftID)
}
