// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/grpcsvc"
)

// The cases of steward-mock-server's appendix-collab.test.ts "an appendix's
// room and token": issued to someone who can edit the policy, for the
// appendix's own path; refused to a reader and for an appendix that does not
// exist; refused for a retired appendix; a token names its own room only.

// fakeAppendixReader answers the appendix lookup from a fixed table.
type fakeAppendixReader struct {
	appendices map[string]access.AppendixInfo
	err        error
	calls      []string
}

func (f *fakeAppendixReader) GetAppendix(_ context.Context, id string) (access.AppendixInfo, error) {
	f.calls = append(f.calls, id)
	if f.err != nil {
		return access.AppendixInfo{}, f.err
	}
	info, ok := f.appendices[id]
	if !ok {
		return access.AppendixInfo{}, access.ErrAppendixNotFound
	}
	return info, nil
}

func seededAppendices() *fakeAppendixReader {
	return &fakeAppendixReader{appendices: map[string]access.AppendixInfo{
		"appendix-1": {PolicyID: "policy-2"},
		"retired-1":  {PolicyID: "policy-2", Retired: true},
	}}
}

func appendixTokenService(edit grpcsvc.EditChecker, appendices access.AppendixReader, em *audit.Emitter) collabv1.CollabTokenServiceServer {
	return grpcsvc.NewTokenService(grpcsvc.TokenOptions{Secret: []byte(testSecret), Appendices: appendices}, edit, em, log.Nop())
}

func symbolOf(err error) string {
	info, _ := apperrgrpc.FromError(err)
	return info.Symbol
}

func TestIssueAppendixToken_MintsATokenForTheAppendixRoom(t *testing.T) {
	edit := allowAll("u-bob")
	edit.names["u-bob"] = "Bob Author"
	em, fap := newTestEmitter()
	svc := appendixTokenService(edit, seededAppendices(), em)

	resp, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "appendix-1"})
	if err != nil {
		t.Fatalf("issue appendix token: %v", err)
	}

	if got, want := resp.GetWsUrl(), "/collab/ws/appendix%3Aappendix-1"; got != want {
		t.Fatalf("ws url: got %q want %q", got, want)
	}
	if resp.GetRoomKey() != "appendix:appendix-1" || resp.GetPolicyId() != "policy-2" || resp.GetExpiresAt() == nil {
		t.Fatalf("response: %+v", resp)
	}
	claims := parse(t, resp.GetToken())
	if claims.DraftID != "appendix:appendix-1" || claims.PolicyID != "policy-2" || claims.UserID != "u-bob" ||
		claims.DisplayName != "Bob Author" || claims.TemplateVersionID != "" {
		t.Fatalf("claims: %+v", claims)
	}
	// The edit check runs against the policy core says the appendix belongs to.
	if len(edit.calls) != 1 || edit.calls[0] != "u-bob@policy-2" {
		t.Fatalf("edit checks: %v", edit.calls)
	}
	msgs := fap.snapshot()
	if len(msgs) != 1 || msgs[0].Event.Action != "collab.token.issued" || msgs[0].Event.Subject != "appendix:appendix-1" {
		t.Fatalf("audit: %+v", msgs)
	}
	attrs := msgs[0].Event.Attributes
	if attrs["appendix_id"] != "appendix-1" || attrs["policy_id"] != "policy-2" {
		t.Fatalf("audit attributes: %v", attrs)
	}
	for k, v := range attrs {
		if v == resp.GetToken() {
			t.Fatalf("the token is in the audit attribute %q", k)
		}
	}
}

func TestIssueAppendixToken_ActAsTokenCarriesTheRealAdmin(t *testing.T) {
	svc := appendixTokenService(allowAll("u-bob"), seededAppendices(), noopEmitter())
	resp, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob", Impersonator: "u-admin"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "appendix-1"})
	if err != nil {
		t.Fatalf("issue appendix token: %v", err)
	}
	if got := parse(t, resp.GetToken()).Impersonator; got != "u-admin" {
		t.Fatalf("imp: got %q", got)
	}
}

func TestIssueAppendixToken_RefusesAReader(t *testing.T) {
	edit := allowAll("u-bob") // u-erin may not edit
	svc := appendixTokenService(edit, seededAppendices(), noopEmitter())
	_, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-erin"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "appendix-1"})
	if status.Code(err) != codes.PermissionDenied || symbolOf(err) != "DRAFT_EDIT_FORBIDDEN" {
		t.Fatalf("got %v (%s)", status.Code(err), symbolOf(err))
	}
}

func TestIssueAppendixToken_RefusesAnAppendixThatDoesNotExist(t *testing.T) {
	edit := allowAll("u-bob")
	svc := appendixTokenService(edit, seededAppendices(), noopEmitter())
	_, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "nope"})
	if status.Code(err) != codes.NotFound || symbolOf(err) != "APPENDIX_NOT_FOUND" {
		t.Fatalf("got %v (%s)", status.Code(err), symbolOf(err))
	}
	if len(edit.calls) != 0 {
		t.Fatal("the edit check ran for an appendix that does not exist")
	}
}

func TestIssueAppendixToken_RefusesARetiredAppendix(t *testing.T) {
	svc := appendixTokenService(allowAll("u-bob"), seededAppendices(), noopEmitter())
	_, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "retired-1"})
	if status.Code(err) != codes.FailedPrecondition || symbolOf(err) != "APPENDIX_RETIRED" {
		t.Fatalf("got %v (%s)", status.Code(err), symbolOf(err))
	}
}

// A reader learns nothing about a retired appendix: the edit check comes first.
func TestIssueAppendixToken_ReaderOfARetiredAppendixIsRefusedAsAReader(t *testing.T) {
	svc := appendixTokenService(allowAll("u-bob"), seededAppendices(), noopEmitter())
	_, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-erin"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "retired-1"})
	if symbolOf(err) != "DRAFT_EDIT_FORBIDDEN" {
		t.Fatalf("got %s", symbolOf(err))
	}
}

func TestIssueAppendixToken_RefusesAnEmptyIDAndAMissingActor(t *testing.T) {
	svc := appendixTokenService(allowAll("u-bob"), seededAppendices(), noopEmitter())
	_, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), &collabv1.IssueAppendixTokenRequest{})
	if status.Code(err) != codes.InvalidArgument || symbolOf(err) != "APPENDIX_ID_REQUIRED" {
		t.Fatalf("empty id: got %v (%s)", status.Code(err), symbolOf(err))
	}
	_, err = svc.IssueAppendixToken(t.Context(), &collabv1.IssueAppendixTokenRequest{AppendixId: "appendix-1"})
	if status.Code(err) != codes.Unauthenticated || symbolOf(err) != "ACTOR_REQUIRED" {
		t.Fatalf("no actor: got %v (%s)", status.Code(err), symbolOf(err))
	}
}

func TestIssueAppendixToken_WhenAPeerCannotAnswer(t *testing.T) {
	ctx := actorCtx(t, grpcactor.Actor{Subject: "u-bob"})
	req := &collabv1.IssueAppendixTokenRequest{AppendixId: "appendix-1"}

	lookup := seededAppendices()
	lookup.err = fmt.Errorf("core GetPolicyAppendix: %w", access.ErrUnavailable)
	_, err := appendixTokenService(allowAll("u-bob"), lookup, noopEmitter()).IssueAppendixToken(ctx, req)
	if status.Code(err) != codes.Unavailable || symbolOf(err) != "EDIT_CHECK_UNAVAILABLE" {
		t.Fatalf("lookup: got %v (%s)", status.Code(err), symbolOf(err))
	}

	edit := allowAll("u-bob")
	edit.err = access.ErrUnavailable
	_, err = appendixTokenService(edit, seededAppendices(), noopEmitter()).IssueAppendixToken(ctx, req)
	if status.Code(err) != codes.Unavailable || symbolOf(err) != "EDIT_CHECK_UNAVAILABLE" {
		t.Fatalf("edit check: got %v (%s)", status.Code(err), symbolOf(err))
	}

	edit = allowAll("u-bob")
	edit.err = access.ErrPolicyNotFound
	_, err = appendixTokenService(edit, seededAppendices(), noopEmitter()).IssueAppendixToken(ctx, req)
	if status.Code(err) != codes.NotFound || symbolOf(err) != "POLICY_NOT_FOUND" {
		t.Fatalf("policy gone: got %v (%s)", status.Code(err), symbolOf(err))
	}

	lookup = seededAppendices()
	lookup.err = errors.New("boom")
	_, err = appendixTokenService(allowAll("u-bob"), lookup, noopEmitter()).IssueAppendixToken(ctx, req)
	if status.Code(err) != codes.Internal {
		t.Fatalf("other lookup failure: got %v", status.Code(err))
	}
}

func TestIssueAppendixToken_WithoutALookupIsUnavailable(t *testing.T) {
	svc := appendixTokenService(allowAll("u-bob"), nil, noopEmitter())
	_, err := svc.IssueAppendixToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}), &collabv1.IssueAppendixTokenRequest{AppendixId: "appendix-1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v", status.Code(err))
	}
}

// A policy token may not name an appendix room: IssueToken signs whatever
// draft_id it is given, so without this a person who may edit one policy could
// join another policy's appendix room, and its checkpoints would reach core
// under the wrong policy.
func TestIssueToken_RefusesADraftIDInTheAppendixRoomSpace(t *testing.T) {
	edit := allowAll("u-bob")
	for _, draft := range []string{"appendix:appendix-9", "appendix:"} {
		_, err := tokenService(edit, noopEmitter()).IssueToken(actorCtx(t, grpcactor.Actor{Subject: "u-bob"}),
			&collabv1.IssueTokenRequest{PolicyId: "policy-1", DraftId: draft})
		if status.Code(err) != codes.InvalidArgument || symbolOf(err) != "DRAFT_ID_RESERVED" {
			t.Fatalf("%q: got %v (%s)", draft, status.Code(err), symbolOf(err))
		}
	}
	if len(edit.calls) != 0 {
		t.Fatal("the edit check ran for a reserved draft id")
	}
}
