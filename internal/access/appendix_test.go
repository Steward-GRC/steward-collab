// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package access_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-collab/internal/access"
)

type fakeAppendices struct {
	appendix *corev1.PolicyAppendix
	err      error
	asked    []string
}

func (f *fakeAppendices) GetPolicyAppendix(_ context.Context, in *corev1.GetPolicyAppendixRequest, _ ...grpc.CallOption) (*corev1.GetPolicyAppendixResponse, error) {
	f.asked = append(f.asked, in.GetId())
	if f.err != nil {
		return nil, f.err
	}
	return &corev1.GetPolicyAppendixResponse{Appendix: f.appendix}, nil
}

func TestAppendixLookup_ReturnsThePolicyAndRetiredState(t *testing.T) {
	f := &fakeAppendices{appendix: &corev1.PolicyAppendix{Id: "a-1", PolicyId: "p-1"}}
	info, err := access.NewAppendixLookup(f).GetAppendix(t.Context(), "a-1")
	require.NoError(t, err)
	require.Equal(t, access.AppendixInfo{PolicyID: "p-1"}, info)
	require.Equal(t, []string{"a-1"}, f.asked)

	f.appendix.RetiredAt = timestamppb.New(time.Now())
	info, err = access.NewAppendixLookup(f).GetAppendix(t.Context(), "a-1")
	require.NoError(t, err)
	require.True(t, info.Retired)
}

func TestAppendixLookup_MapsCoresErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want error
	}{
		"not found":   {status.Error(codes.NotFound, "no"), access.ErrAppendixNotFound},
		"unavailable": {status.Error(codes.Unavailable, "down"), access.ErrUnavailable},
		"deadline":    {status.Error(codes.DeadlineExceeded, "slow"), access.ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := access.NewAppendixLookup(&fakeAppendices{err: tc.err}).GetAppendix(t.Context(), "a-1")
			require.ErrorIs(t, err, tc.want)
		})
	}
	_, err := access.NewAppendixLookup(&fakeAppendices{err: status.Error(codes.Internal, "boom")}).GetAppendix(t.Context(), "a-1")
	require.Error(t, err)
	require.NotErrorIs(t, err, access.ErrAppendixNotFound)
	require.NotErrorIs(t, err, access.ErrUnavailable)
}

func TestAppendixLookup_AnEmptyAnswerIsNotFound(t *testing.T) {
	_, err := access.NewAppendixLookup(&fakeAppendices{}).GetAppendix(t.Context(), "a-1")
	require.ErrorIs(t, err, access.ErrAppendixNotFound)
}
