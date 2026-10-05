// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// CallerGateway is the gateway's caller name: its service account
// steward-gateway without the steward- prefix.
const CallerGateway = "gateway"

// CallerPolicy is collab's per-method allow-list. The gateway is the only
// caller and acts for a signed-in user on every call, so it may pass the
// user's actor.
var CallerPolicy = workloadauth.Policy{
	collabv1.CollabTokenService_IssueToken_FullMethodName:          {CallerGateway: workloadauth.OnBehalf},
	collabv1.CollabRoomService_FlushDraft_FullMethodName:           {CallerGateway: workloadauth.OnBehalf},
	collabv1.CollabRoomService_NotifyDraftPublished_FullMethodName: {CallerGateway: workloadauth.OnBehalf},
}
