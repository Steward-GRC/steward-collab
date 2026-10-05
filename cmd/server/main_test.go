// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Bugs5382/go-buildinfo/health"
	log "github.com/Bugs5382/go-log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// The HTTP listener carries the websocket upgrades and the probes and nothing
// else: no plain /health or /healthz.
func TestHTTPMuxServesWebsocketsAndProbesOnly(t *testing.T) {
	ws := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	mux, err := httpMux(ws, health.New())
	require.NoError(t, err)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for path, want := range map[string]int{
		"/ws/draft-1": http.StatusTeapot,
		"/livez":      http.StatusOK,
		"/readyz":     http.StatusOK,
		"/healthz":    http.StatusNotFound,
		"/health":     http.StatusNotFound,
	} {
		res, err := http.Get(srv.URL + path)
		require.NoError(t, err)
		_ = res.Body.Close()
		require.Equal(t, want, res.StatusCode, path)
	}
}

type recordingPublisher struct{ bodies [][]byte }

func (r *recordingPublisher) Publish(_ context.Context, _ string, body []byte) error {
	r.bodies = append(r.bodies, body)
	return nil
}

// A refused call is audited with who called what and why, never the request.
func TestAuditDenialRecordsTheRefusal(t *testing.T) {
	pub := &recordingPublisher{}
	auditDenial(audit.New(pub), log.Nop())(t.Context(), workloadauth.Denial{
		Method: "/steward.collab.v1.CollabTokenService/IssueToken",
		Caller: workloadauth.Caller{Name: "workflow", ServiceAccount: "steward/steward-workflow"},
		Code:   codes.PermissionDenied, Reason: workloadauth.ReasonMethodNotAllowed,
		Request: "secret request body",
	})
	require.Len(t, pub.bodies, 1)
	ev, err := audit.Decode(pub.bodies[0])
	require.NoError(t, err)
	require.Equal(t, "collab.call.refused", ev.Action)
	require.Equal(t, "method:/steward.collab.v1.CollabTokenService/IssueToken", ev.Subject)
	require.Equal(t, "workflow", ev.Attributes["caller"])
	require.Equal(t, "steward/steward-workflow", ev.Attributes["service_account"])
	require.Equal(t, "PermissionDenied", ev.Attributes["code"])
	for _, v := range ev.Attributes {
		require.NotContains(t, v, "secret request body")
	}
}
