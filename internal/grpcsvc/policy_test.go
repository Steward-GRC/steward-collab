// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/grpcsvc"
	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// Only the gateway calls collab, and always for a signed-in user, so it may
// pass that user's actor on every method. Every other service account is
// refused.
func TestCallerPolicyPerMethod(t *testing.T) {
	methods := map[string]string{
		"IssueToken":           collabv1.CollabTokenService_IssueToken_FullMethodName,
		"FlushDraft":           collabv1.CollabRoomService_FlushDraft_FullMethodName,
		"NotifyDraftPublished": collabv1.CollabRoomService_NotifyDraftPublished_FullMethodName,
	}
	for name, method := range methods {
		t.Run(name, func(t *testing.T) {
			acc, ok := grpcsvc.CallerPolicy.Lookup(method, grpcsvc.CallerGateway)
			require.True(t, ok, "the gateway calls %s", name)
			require.Equal(t, workloadauth.OnBehalf, acc)
			require.Len(t, grpcsvc.CallerPolicy[method], 1, "only the gateway is listed")
			for _, other := range []string{"core", "workflow", "ai", "steward-gateway"} {
				_, ok := grpcsvc.CallerPolicy.Lookup(method, other)
				require.False(t, ok, "%s may not call %s", other, name)
			}
		})
	}
	require.Len(t, grpcsvc.CallerPolicy, len(methods), "every served method is listed, and nothing else")
}
