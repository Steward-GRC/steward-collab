// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package audit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	auditv1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/audit/v1"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/fixture"
)

type published struct {
	key  string
	body []byte
}

type capture struct {
	got []published
	err error
}

func (c *capture) Publish(_ context.Context, key string, body []byte) error {
	c.got = append(c.got, published{key, body})
	return c.err
}

func TestEmitPublishesAuditsProtoEvent(t *testing.T) {
	c := &capture{}
	at := time.Date(2026, 10, 4, 9, 30, 0, 0, time.UTC)
	err := audit.New(c).Emit(context.Background(), audit.Event{
		Tier: audit.TierAudit, Action: "policy.published", ActorUserID: "bob", Subject: "policy:POL-FACILITIES-000001",
		GroupID: fixture.FacilitiesTeam, Attributes: map[string]string{"version": "v1"}, OccurredAt: at,
	})
	require.NoError(t, err)
	require.Len(t, c.got, 1)
	require.Equal(t, "audit.audit", c.got[0].key)

	var ev auditv1.AuditEvent
	require.NoError(t, proto.Unmarshal(c.got[0].body, &ev))
	require.Equal(t, auditv1.Tier_TIER_AUDIT, ev.GetTier())
	require.Equal(t, "policy.published", ev.GetAction())
	require.Equal(t, "bob", ev.GetActorUserId())
	require.Equal(t, "policy:POL-FACILITIES-000001", ev.GetSubject())
	require.Equal(t, fixture.FacilitiesTeam, ev.GetGroupId())
	require.Equal(t, map[string]string{"version": "v1"}, ev.GetAttributes())
	require.True(t, at.Equal(ev.GetOccurredAt().AsTime()))
}

func TestEmitStampsTheTimeAndRoutesTheActivityTier(t *testing.T) {
	c := &capture{}
	before := time.Now().UTC()
	require.NoError(t, audit.New(c).Emit(context.Background(), audit.Event{Tier: audit.TierActivity, Action: "policy.viewed"}))
	require.Equal(t, "audit.activity", c.got[0].key)
	ev, err := audit.Decode(c.got[0].body)
	require.NoError(t, err)
	require.Equal(t, audit.TierActivity, ev.Tier)
	require.False(t, ev.OccurredAt.Before(before), "an unset time is stamped at emit")
}

func TestEmitRefusesAnEventWithoutATierOrAction(t *testing.T) {
	c := &capture{}
	require.Error(t, audit.New(c).Emit(context.Background(), audit.Event{Action: "policy.viewed"}))
	require.Error(t, audit.New(c).Emit(context.Background(), audit.Event{Tier: audit.TierAudit}))
	require.Empty(t, c.got)
}

func TestEmitReturnsThePublishError(t *testing.T) {
	boom := errors.New("broker down")
	err := audit.New(&capture{err: boom}).Emit(context.Background(), audit.Event{Tier: audit.TierAudit, Action: "policy.published"})
	require.ErrorIs(t, err, boom)
}

func TestContentTypeNamesTheMessage(t *testing.T) {
	require.Equal(t, "application/protobuf; proto=steward.audit.v1.AuditEvent", audit.ContentType)
}

func TestActedAsCreditsTheRealAdmin(t *testing.T) {
	ev := audit.Event{Tier: audit.TierAudit, Action: "collab.snapshot.accepted", ActorUserID: fixture.Bob,
		Attributes: map[string]string{"draft_id": "draft-1"}}
	got := ev.ActedAs(fixture.Alice)
	require.Equal(t, fixture.Alice, got.ActorUserID)
	require.Equal(t, fixture.Bob, got.Attributes["impersonated_user_id"])
	require.Equal(t, "draft-1", got.Attributes["draft_id"])
	require.NotContains(t, ev.Attributes, "impersonated_user_id", "the original event is left alone")
}
