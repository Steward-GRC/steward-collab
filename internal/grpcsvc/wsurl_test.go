// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"testing"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/grpcsvc"
)

// The ws_url in the IssueToken response is what the browser dials. It is the
// GATEWAY's reverse-proxy path, and the default (COLLAB_WS_BASE unset, an empty
// base) is a same-origin RELATIVE url so one image works unchanged anywhere.
func TestIssueToken_WsURLForms(t *testing.T) {
	tests := []struct {
		name    string
		wsBase  string
		draftID string
		want    string
	}{
		{
			// The normal deployment: relative, resolved by the browser against
			// the page origin (http->ws, https->wss).
			name:    "empty base is same-origin relative",
			wsBase:  "",
			draftID: "0f8e1d2c-3b4a-5967-8899-aabbccddeeff",
			want:    "/collab/ws/0f8e1d2c-3b4a-5967-8899-aabbccddeeff",
		},
		{
			name:    "explicit gateway origin",
			wsBase:  "wss://policies.example.org",
			draftID: "draft-7",
			want:    "wss://policies.example.org/collab/ws/draft-7",
		},
		{
			// Trailing slash on the override must not double up.
			name:    "trailing slash trimmed",
			wsBase:  "ws://localhost:8080/",
			draftID: "draft-7",
			want:    "ws://localhost:8080/collab/ws/draft-7",
		},
		{
			// A non-canonical draft id can never inject a path segment or a
			// query into the url the browser is handed.
			name:    "draft id is path-escaped",
			wsBase:  "",
			draftID: "a/../b?x=1",
			want:    "/collab/ws/a%2F..%2Fb%3Fx=1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := grpcsvc.NewTokenService(grpcsvc.TokenOptions{Secret: []byte(testSecret), WsBase: tc.wsBase},
				allowAll("u-1"), noopEmitter(), log.Nop())
			ctx := actorCtx(t, grpcactor.Actor{Subject: "u-1"})
			resp, err := svc.IssueToken(ctx, &collabv1.IssueTokenRequest{
				PolicyId:          "policy-1",
				DraftId:           tc.draftID,
				TemplateVersionId: "tmpl-v2",
			})
			if err != nil {
				t.Fatalf("issue token: %v", err)
			}
			if got := resp.GetWsUrl(); got != tc.want {
				t.Fatalf("ws_url: got %q want %q", got, tc.want)
			}
		})
	}
}

// The path prefix ws_url is built from is a shared contract with the gateway's
// "GET /collab/ws/{draftID}" route. Pin it so a rename here can never silently
// diverge from the route the edge actually serves.
func TestWsPathPrefix_MatchesGatewayRoute(t *testing.T) {
	if got, want := grpcsvc.WsPathPrefix, "/collab/ws"; got != want {
		t.Fatalf("WsPathPrefix: got %q want %q — the gateway route must change too", got, want)
	}
}
