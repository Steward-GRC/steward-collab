// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package errcodes_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Steward-GRC/steward-collab/internal/errcodes"
)

func TestDraftEditForbiddenRoundTrip(t *testing.T) {
	st := status.Convert(errcodes.New(context.Background(), errcodes.CodeDraftEditForbidden, "policy_id", "policy-1"))
	require.Equal(t, codes.PermissionDenied, st.Code())
	require.Equal(t, "You don't have permission to edit this draft.", st.Message())

	info, ok := apperrgrpc.FromStatus(st)
	require.True(t, ok, "status must carry ErrorInfo")
	require.Equal(t, "DRAFT_EDIT_FORBIDDEN", info.Symbol)
	require.Equal(t, 8505, info.Code)
	require.Equal(t, "collab", info.Domain)
	require.Equal(t, "policy-1", info.Metadata["policy_id"])
}

func TestRefusalCodesKeepTheirStatusAndSymbol(t *testing.T) {
	cases := []struct {
		code   int
		symbol string
		grpc   codes.Code
	}{
		{errcodes.CodePolicyIDRequired, "POLICY_ID_REQUIRED", codes.InvalidArgument},
		{errcodes.CodeDraftIDRequired, "DRAFT_ID_REQUIRED", codes.InvalidArgument},
		{errcodes.CodeVersionNumberInvalid, "VERSION_NUMBER_INVALID", codes.InvalidArgument},
		{errcodes.CodeActorRequired, "ACTOR_REQUIRED", codes.Unauthenticated},
		{errcodes.CodeDraftEditForbidden, "DRAFT_EDIT_FORBIDDEN", codes.PermissionDenied},
		{errcodes.CodeRoomPolicyMismatch, "ROOM_POLICY_MISMATCH", codes.FailedPrecondition},
		{errcodes.CodeFlushRejected, "FLUSH_REJECTED", codes.FailedPrecondition},
		{errcodes.CodeFlushUnavailable, "FLUSH_UNAVAILABLE", codes.Unavailable},
		{errcodes.CodeFlushTimeout, "FLUSH_TIMEOUT", codes.DeadlineExceeded},
		{errcodes.CodePolicyNotFound, "POLICY_NOT_FOUND", codes.NotFound},
		{errcodes.CodeEditCheckUnavailable, "EDIT_CHECK_UNAVAILABLE", codes.Unavailable},
	}
	for _, c := range cases {
		st := status.Convert(errcodes.New(context.Background(), c.code))
		require.Equal(t, c.grpc, st.Code(), c.symbol)
		info, ok := apperrgrpc.FromStatus(st)
		require.True(t, ok, c.symbol)
		require.Equal(t, c.symbol, info.Symbol)
		require.Equal(t, c.code, info.Code)
		require.Equal(t, errcodes.Domain, info.Domain)
	}
}

// The author has to fix what core refused before publishing, so core's
// reason reaches the caller, in the message and in the metadata.
func TestFlushRejectedCarriesCoreReason(t *testing.T) {
	st := status.Convert(errcodes.New(context.Background(), errcodes.CodeFlushRejected,
		"reason", "grpc_error", "detail", "InvalidArgument: content validation: section missing"))
	require.Equal(t, "The latest edits couldn't be saved, so the draft wasn't published: InvalidArgument: content validation: section missing", st.Message())
	info, _ := apperrgrpc.FromStatus(st)
	require.Equal(t, "grpc_error", info.Metadata["reason"])
	require.Equal(t, "InvalidArgument: content validation: section missing", info.Metadata["detail"])
}

func TestGatewayBugsAreNotUserSafe(t *testing.T) {
	for _, code := range []int{errcodes.CodePolicyIDRequired, errcodes.CodeDraftIDRequired, errcodes.CodeRoomPolicyMismatch} {
		st := status.Convert(errcodes.New(context.Background(), code))
		require.Contains(t, st.Message(), "Internal Error", "a caller bug's detail stays off the wire")
	}
}

func TestUncodedErrorsFallBackToInternal(t *testing.T) {
	info, _ := apperrgrpc.FromError(errcodes.Error(context.Background(), errors.New("boom")))
	require.Equal(t, errcodes.CodeInternal, info.Code)
	require.Equal(t, "INTERNAL", info.Symbol)
}

func TestRegistryBandAndDomain(t *testing.T) {
	require.NotEmpty(t, errcodes.Entries())
	for _, e := range errcodes.Entries() {
		require.GreaterOrEqual(t, e.Code, 8500, "code %d must be in collab's half of band 8", e.Code)
		require.Less(t, e.Code, 9000, "code %d must be in band 8", e.Code)
		_, ok := errcodes.Registry().Describe(e.Code)
		require.True(t, ok)
	}
}

// docs/error-codes.md is generated from the registry; refresh it with
// UPDATE_DOCS=1 go test ./internal/errcodes.
func TestErrorCodesDocIsCurrent(t *testing.T) {
	const path = "../../docs/error-codes.md"
	want := errcodes.Doc()
	if os.Getenv("UPDATE_DOCS") == "1" {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o600))
	}
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, want, string(got), "docs/error-codes.md is stale; run UPDATE_DOCS=1 go test ./internal/errcodes")
}
