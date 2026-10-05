// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// fakeVerifier maps a token to the service account it belongs to.
type fakeVerifier map[string]workloadauth.Caller

func (f fakeVerifier) Verify(token string) (workloadauth.Caller, error) {
	c, ok := f[token]
	if !ok {
		return workloadauth.Caller{}, workloadauth.ErrRejected
	}
	return c, nil
}

var verifier = fakeVerifier{
	"gateway-token":  {Name: "gateway", ServiceAccount: "steward/steward-gateway"},
	"workflow-token": {Name: "workflow", ServiceAccount: "steward/steward-workflow"},
	"ai-token":       {Name: "ai", ServiceAccount: "steward/steward-ai"},
}

var testPolicy = workloadauth.Policy{
	collabv1.CollabRoomService_FlushDraft_FullMethodName: {
		"gateway": workloadauth.OnBehalf,
		"ai":      workloadauth.Self,
	},
}

// actorProbe records the actor go-grpc-actor admitted.
type actorProbe struct {
	collabv1.UnimplementedCollabRoomServiceServer
	seen chan grpcactor.Actor
}

func (p actorProbe) FlushDraft(ctx context.Context, _ *collabv1.FlushDraftRequest) (*collabv1.FlushDraftResponse, error) {
	a, _ := grpcactor.FromContext(ctx)
	p.seen <- a
	return &collabv1.FlushDraftResponse{}, nil
}

func serveAuth(t *testing.T, opts Options, seen chan grpcactor.Actor) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, lis, log.Nop(), opts, func(s *grpc.Server) {
			collabv1.RegisterCollabRoomServiceServer(s, actorProbe{seen: seen})
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("Serve did not stop")
		}
	})
	return lis.Addr().String()
}

func clientFor(t *testing.T, addr, tokenFile string) collabv1.CollabRoomServiceClient {
	t.Helper()
	opts, err := DialOptions(tokenFile)
	require.NoError(t, err)
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return collabv1.NewCollabRoomServiceClient(conn)
}

func tokenFile(t *testing.T, token string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(p, []byte(token+"\n"), 0o600))
	return p
}

var acting = grpcactor.Actor{Subject: "user-bob", Impersonator: "user-alice"}

func TestAuthLetsAListedCallerPassAnActor(t *testing.T) {
	seen := make(chan grpcactor.Actor, 1)
	addr := serveAuth(t, Options{Verifier: verifier, Policy: testPolicy}, seen)
	_, err := clientFor(t, addr, tokenFile(t, "gateway-token")).FlushDraft(grpcactor.WithActor(t.Context(), acting), &collabv1.FlushDraftRequest{})
	require.NoError(t, err)
	require.Equal(t, acting, <-seen, "the gateway acts on a user's behalf")
}

func TestAuthDropsTheActorOfASelfCaller(t *testing.T) {
	seen := make(chan grpcactor.Actor, 1)
	addr := serveAuth(t, Options{Verifier: verifier, Policy: testPolicy}, seen)
	_, err := clientFor(t, addr, tokenFile(t, "ai-token")).FlushDraft(grpcactor.WithActor(t.Context(), acting), &collabv1.FlushDraftRequest{})
	require.NoError(t, err)
	require.Equal(t, grpcactor.Actor{}, <-seen, "a caller listed as Self acts only as itself")
}

// A valid token from a service account the method doesn't list is refused.
func TestAuthRefusesAnUnlistedServiceAccount(t *testing.T) {
	var denied []workloadauth.Denial
	addr := serveAuth(t, Options{Verifier: verifier, Policy: testPolicy, OnDeny: func(_ context.Context, d workloadauth.Denial) {
		denied = append(denied, d)
	}}, make(chan grpcactor.Actor, 1))
	_, err := clientFor(t, addr, tokenFile(t, "workflow-token")).FlushDraft(t.Context(), &collabv1.FlushDraftRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Len(t, denied, 1, "the refusal reaches the deny hook")
	require.Equal(t, "workflow", denied[0].Caller.Name)
}

func TestAuthRefusesAMissingOrBadToken(t *testing.T) {
	addr := serveAuth(t, Options{Verifier: verifier, Policy: testPolicy}, make(chan grpcactor.Actor, 1))
	_, err := clientFor(t, addr, "").FlushDraft(t.Context(), &collabv1.FlushDraftRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "no token")
	_, err = clientFor(t, addr, tokenFile(t, "forged")).FlushDraft(t.Context(), &collabv1.FlushDraftRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "a token the verifier rejects")
}

func TestAuthLeavesHealthOpen(t *testing.T) {
	addr := serveAuth(t, Options{Verifier: verifier, Policy: testPolicy}, make(chan grpcactor.Actor, 1))
	opts, err := DialOptions("")
	require.NoError(t, err)
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "probes need no token")
}

// With WORKLOAD_AUTH=disabled every caller is let in and its actor believed.
func TestAuthDisabledTrustsEveryCaller(t *testing.T) {
	seen := make(chan grpcactor.Actor, 1)
	addr := serveAuth(t, Options{}, seen)
	_, err := clientFor(t, addr, "").FlushDraft(grpcactor.WithActor(t.Context(), acting), &collabv1.FlushDraftRequest{})
	require.NoError(t, err)
	require.Equal(t, acting, <-seen)
}

// The token rides every outbound call, re-read each time, with the actor.
func TestDialOptionsSendTheTokenAndTheActor(t *testing.T) {
	got := make(chan metadata.MD, 2)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got <- md
		return &collabv1.FlushDraftResponse{}, nil
	}))
	collabv1.RegisterCollabRoomServiceServer(s, collabv1.UnimplementedCollabRoomServiceServer{})
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	file := tokenFile(t, "first")
	client := clientFor(t, lis.Addr().String(), file)
	_, err = client.FlushDraft(grpcactor.WithActor(t.Context(), acting), &collabv1.FlushDraftRequest{})
	require.NoError(t, err)
	md := <-got
	require.Equal(t, []string{"Bearer first"}, md.Get("authorization"))
	require.NotEmpty(t, md.Get(grpcactor.MetadataKey), "the actor travels")

	require.NoError(t, os.WriteFile(file, []byte("rotated"), 0o600))
	_, err = client.FlushDraft(t.Context(), &collabv1.FlushDraftRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"Bearer rotated"}, (<-got).Get("authorization"), "a rotated token is picked up")
}

// A token file that isn't there fails closed at start-up.
func TestDialOptionsFailClosedOnAMissingTokenFile(t *testing.T) {
	_, err := DialOptions(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}
