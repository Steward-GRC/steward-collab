// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot_test

import (
	"context"
	"testing"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

type fakeCoreAppendices struct {
	req   *corev1.CheckpointAppendixDraftRequest
	actor grpcactor.Actor
	resp  *corev1.CheckpointAppendixDraftResponse
	err   error
}

func (f *fakeCoreAppendices) CheckpointAppendixDraft(ctx context.Context, in *corev1.CheckpointAppendixDraftRequest, _ ...grpc.CallOption) (*corev1.CheckpointAppendixDraftResponse, error) {
	f.req = in
	f.actor, _ = grpcactor.FromContext(ctx)
	return f.resp, f.err
}

func TestAppendixGRPCClient_SendsTheCheckpointAsTheActor(t *testing.T) {
	f := &fakeCoreAppendices{resp: &corev1.CheckpointAppendixDraftResponse{Accepted: true}}
	ok, reason, err := snapshot.NewAppendixClient(f).CheckpointAppendixDraft(t.Context(), snapshot.AppendixRequest{
		AppendixID: "a-1", PolicyID: "p-1", ContentJSON: lexicalDoc, ActorUserID: appendixActor, Impersonator: "admin-1",
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, reason)
	require.Equal(t, "a-1", f.req.GetId())
	require.Equal(t, "p-1", f.req.GetPolicyId())
	require.Equal(t, lexicalDoc, f.req.GetContentJson())
	require.Equal(t, appendixActor, f.req.GetActorUserId())
	require.Equal(t, grpcactor.Actor{Subject: appendixActor, Impersonator: "admin-1"}, f.actor)
}

// The collab service actor is a fallback, not a person: it rides in the request
// only, as with the policy call.
func TestAppendixGRPCClient_SystemActorIsNotPutOnTheCall(t *testing.T) {
	f := &fakeCoreAppendices{resp: &corev1.CheckpointAppendixDraftResponse{Accepted: true}}
	_, _, err := snapshot.NewAppendixClient(f).CheckpointAppendixDraft(t.Context(), snapshot.AppendixRequest{
		AppendixID: "a-1", PolicyID: "p-1", ContentJSON: lexicalDoc, ActorUserID: snapshot.SystemActorID,
	})
	require.NoError(t, err)
	require.Equal(t, snapshot.SystemActorID, f.req.GetActorUserId())
	require.Empty(t, f.actor.Subject)
}

func TestAppendixGRPCClient_RelaysARefusalAndAnError(t *testing.T) {
	f := &fakeCoreAppendices{resp: &corev1.CheckpointAppendixDraftResponse{RejectReason: "appendix is retired"}}
	ok, reason, err := snapshot.NewAppendixClient(f).CheckpointAppendixDraft(t.Context(), snapshot.AppendixRequest{AppendixID: "a-1", ActorUserID: appendixActor})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "appendix is retired", reason)

	f = &fakeCoreAppendices{err: status.Error(codes.Unavailable, "down")}
	_, _, err = snapshot.NewAppendixClient(f).CheckpointAppendixDraft(t.Context(), snapshot.AppendixRequest{AppendixID: "a-1", ActorUserID: appendixActor})
	require.Equal(t, codes.Unavailable, status.Code(err))
}
