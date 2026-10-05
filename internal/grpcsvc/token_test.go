// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/grpcsvc"
	"github.com/Steward-GRC/steward-collab/internal/ws"
)

func tokenService(edit grpcsvc.EditChecker, em *audit.Emitter) collabv1.CollabTokenServiceServer {
	return grpcsvc.NewTokenService(grpcsvc.TokenOptions{Secret: []byte(testSecret), WsBase: "wss://collab.example.org"}, edit, em, log.Nop())
}

// parse verifies a token the way ws.Handler does, so a token that parses here
// opens a room.
func parse(t *testing.T, token string) *grpcsvc.CollabTokenClaims {
	t.Helper()
	claims := &grpcsvc.CollabTokenClaims{}
	_, err := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithAudience(ws.TokenAudience),
		jwt.WithExpirationRequired(),
	).ParseWithClaims(token, claims, func(*jwt.Token) (any, error) { return []byte(testSecret), nil })
	if err != nil {
		t.Fatalf("parse token with the ws verifier: %v", err)
	}
	return claims
}

func validRequest() *collabv1.IssueTokenRequest {
	return &collabv1.IssueTokenRequest{PolicyId: "policy-1", DraftId: "draft-1", TemplateVersionId: "tmpl-v2"}
}

func TestIssueToken_ValidRequest_MintsVerifiableJWT(t *testing.T) {
	svc := tokenService(allowAll("u-bob"), noopEmitter())
	resp, err := svc.IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), validRequest())
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	// ws_url is the gateway's reverse-proxy path, never collab's own listener:
	// a browser can't resolve collab's in-cluster name.
	if got, want := resp.GetWsUrl(), "wss://collab.example.org/collab/ws/draft-1"; got != want {
		t.Fatalf("ws url: got %q want %q", got, want)
	}
	if resp.GetExpiresAt() == nil {
		t.Fatal("ExpiresAt is nil")
	}

	claims := parse(t, resp.GetToken())
	if claims.UserID != "u-bob" || claims.PolicyID != "policy-1" || claims.DraftID != "draft-1" || claims.TemplateVersionID != "tmpl-v2" {
		t.Fatalf("claims: %+v", claims)
	}
	if claims.Impersonator != "" {
		t.Errorf("imp: got %q, want empty outside act-as", claims.Impersonator)
	}
	if claims.IssuedAt == nil || claims.NotBefore == nil || claims.ExpiresAt == nil {
		t.Fatalf("missing time claims: iat=%v nbf=%v exp=%v", claims.IssuedAt, claims.NotBefore, claims.ExpiresAt)
	}
	exp := claims.ExpiresAt.Time
	if exp.Before(time.Now()) || exp.After(time.Now().Add(grpcsvc.DefaultTokenTTL+30*time.Second)) {
		t.Errorf("exp not within the default TTL: %v", exp)
	}
}

// The presence label comes from the caller's identity record, which the edit
// check reads anyway; the request can't set it.
func TestIssueToken_CarriesDisplayNameClaim(t *testing.T) {
	edit := allowAll("u-bob")
	edit.names["u-bob"] = "Bob"
	resp, err := tokenService(edit, noopEmitter()).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), validRequest())
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	if got := parse(t, resp.GetToken()).DisplayName; got != "Bob" {
		t.Fatalf("name claim: got %q want Bob", got)
	}
}

func TestIssueToken_TTLFromOptions(t *testing.T) {
	svc := grpcsvc.NewTokenService(grpcsvc.TokenOptions{Secret: []byte(testSecret), TTL: 30 * time.Second}, allowAll("u-bob"), noopEmitter(), log.Nop())
	before := time.Now()
	resp, err := svc.IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), validRequest())
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	exp := parse(t, resp.GetToken()).ExpiresAt.Time
	if exp.Before(before.Add(25*time.Second)) || exp.After(before.Add(35*time.Second)) {
		t.Fatalf("exp not in the 30s window: got %v (before=%v)", exp, before)
	}
}

func TestIssueToken_RejectsMissingActor(t *testing.T) {
	_, err := tokenService(allowAll("u-bob"), noopEmitter()).IssueToken(t.Context(), validRequest())
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated (err: %v)", got, err)
	}
	if info, _ := apperrgrpc.FromError(err); info.Symbol != "ACTOR_REQUIRED" {
		t.Fatalf("symbol: got %q want ACTOR_REQUIRED", info.Symbol)
	}
}

func TestIssueToken_RejectsEmptyFields(t *testing.T) {
	cases := map[string]struct {
		req    *collabv1.IssueTokenRequest
		symbol string
	}{
		"missing policy_id": {&collabv1.IssueTokenRequest{DraftId: "draft-1"}, "POLICY_ID_REQUIRED"},
		"missing draft_id":  {&collabv1.IssueTokenRequest{PolicyId: "policy-1"}, "DRAFT_ID_REQUIRED"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			edit := allowAll("u-bob")
			_, err := tokenService(edit, noopEmitter()).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), tc.req)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("status code = %v, want InvalidArgument", got)
			}
			if info, _ := apperrgrpc.FromError(err); info.Symbol != tc.symbol {
				t.Fatalf("symbol: got %q want %q", info.Symbol, tc.symbol)
			}
			if len(edit.calls) != 0 {
				t.Fatal("the edit check ran for an unusable request")
			}
		})
	}
}

// The decision is made for the caller's own actor, so a caller can't get a
// token for edit access it lacks.
func TestIssueToken_ChecksTheCallersOwnAccess(t *testing.T) {
	edit := allowAll()
	_, err := tokenService(edit, noopEmitter()).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-erin"}), validRequest())
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("status code = %v, want PermissionDenied", got)
	}
	if info, _ := apperrgrpc.FromError(err); info.Symbol != "DRAFT_EDIT_FORBIDDEN" {
		t.Fatalf("symbol: got %q", info.Symbol)
	}
	if len(edit.calls) != 1 || edit.calls[0] != "u-erin@policy-1" {
		t.Fatalf("edit check calls: %v", edit.calls)
	}
}

func TestIssueToken_EditCheckFailures(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		code   codes.Code
		symbol string
	}{
		{"unknown policy", access.ErrPolicyNotFound, codes.NotFound, "POLICY_NOT_FOUND"},
		{"a peer is down", errors.Join(access.ErrUnavailable, errors.New("connection refused")), codes.Unavailable, "EDIT_CHECK_UNAVAILABLE"},
		{"anything else", errors.New("boom"), codes.Internal, "INTERNAL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			edit := &fakeEditChecker{err: tc.err}
			_, err := tokenService(edit, noopEmitter()).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), validRequest())
			if got := status.Code(err); got != tc.code {
				t.Fatalf("status code = %v, want %v", got, tc.code)
			}
			if info, _ := apperrgrpc.FromError(err); info.Symbol != tc.symbol {
				t.Fatalf("symbol: got %q want %q", info.Symbol, tc.symbol)
			}
		})
	}
}

// During act-as the token is for the user acted as, who must be allowed to
// edit, and carries the real admin; the audit event credits the admin.
func TestIssueToken_ActAs(t *testing.T) {
	em, fap := newTestEmitter()
	edit := allowAll("u-bob")
	resp, err := tokenService(edit, em).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob", Impersonator: "u-alice"}), validRequest())
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	claims := parse(t, resp.GetToken())
	if claims.UserID != "u-bob" || claims.Impersonator != "u-alice" {
		t.Fatalf("claims: uid %q imp %q", claims.UserID, claims.Impersonator)
	}
	if edit.calls[0] != "u-bob@policy-1" {
		t.Fatalf("edit check ran for %q, want the user acted as", edit.calls[0])
	}
	ev := fap.snapshot()[0].Event
	if ev.ActorUserID != "u-alice" || ev.Attributes["impersonated_user_id"] != "u-bob" {
		t.Fatalf("audit: actor %q impersonated %q", ev.ActorUserID, ev.Attributes["impersonated_user_id"])
	}
}

// A successful mint emits collab.token.issued on the policy subject. The token
// itself is never in the event.
func TestIssueToken_EmitsAuditEvent(t *testing.T) {
	em, fap := newTestEmitter()
	resp, err := tokenService(allowAll("u-bob"), em).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}),
		&collabv1.IssueTokenRequest{PolicyId: "policy-77", DraftId: "draft-88", TemplateVersionId: "tmpl-v9"})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(msgs))
	}
	got := msgs[0]
	if got.RoutingKey != "audit.audit" || got.Event.Action != "collab.token.issued" || got.Event.Tier != audit.TierAudit {
		t.Fatalf("event: key %q action %q tier %q", got.RoutingKey, got.Event.Action, got.Event.Tier)
	}
	if got.Event.Subject != "policy:policy-77" || got.Event.ActorUserID != "u-bob" {
		t.Fatalf("subject %q actor %q", got.Event.Subject, got.Event.ActorUserID)
	}
	for k, want := range map[string]string{"policy_id": "policy-77", "draft_id": "draft-88", "template_version_id": "tmpl-v9"} {
		if got.Event.Attributes[k] != want {
			t.Errorf("%s attr: got %q want %q", k, got.Event.Attributes[k], want)
		}
	}
	if got.Event.Attributes["expires_at"] == "" {
		t.Error("expires_at attr is empty")
	}
	for k, v := range got.Event.Attributes {
		if v == resp.GetToken() {
			t.Fatalf("the token is in the %s attribute", k)
		}
	}
}

// Failed mints aren't audited: the gRPC error is the record, and auditing every
// refusal would flood the trail with caller bugs.
func TestIssueToken_NoAuditOnInvalidRequest(t *testing.T) {
	em, fap := newTestEmitter()
	_, err := tokenService(allowAll("u-bob"), em).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}),
		&collabv1.IssueTokenRequest{PolicyId: "p", TemplateVersionId: "tv"})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := len(fap.snapshot()); got != 0 {
		t.Fatalf("expected zero audit events for an invalid request, got %d", got)
	}
}

// A freeform policy has no template version. The token carries an empty tvid,
// never a made-up one, and the audit records it empty.
func TestIssueToken_FreeFormPolicy_MintsTokenWithEmptyTvid(t *testing.T) {
	em, fap := newTestEmitter()
	resp, err := tokenService(allowAll("u-bob"), em).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}),
		&collabv1.IssueTokenRequest{PolicyId: "policy-ff", DraftId: "draft-ff"})
	if err != nil {
		t.Fatalf("issue token for a freeform draft: %v", err)
	}
	if got := parse(t, resp.GetToken()).TemplateVersionID; got != "" {
		t.Fatalf("tvid: got %q want empty", got)
	}
	if got, ok := fap.snapshot()[0].Event.Attributes["template_version_id"]; !ok || got != "" {
		t.Fatalf("template_version_id attr: got %q (present=%v), want present and empty", got, ok)
	}
}
