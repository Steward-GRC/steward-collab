// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package audit publishes collab's audit events in steward-audit's contract: a
// steward.audit.v1.AuditEvent, as protobuf binary, to the "audit" topic
// exchange with routing key audit.<tier>.
package audit

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	auditv1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/audit/v1"
)

// Exchange is the topic exchange audit consumes from.
const Exchange = "audit"

// ContentType is the AMQP content type every event is published with.
const ContentType = "application/protobuf; proto=steward.audit.v1.AuditEvent"

// Tier is the retention class of an event.
type Tier string

// The tiers.
const (
	TierAudit    Tier = "audit"
	TierActivity Tier = "activity"
)

var tiers = map[Tier]auditv1.Tier{TierAudit: auditv1.Tier_TIER_AUDIT, TierActivity: auditv1.Tier_TIER_ACTIVITY}

// Event is one audit event as the handlers build it.
type Event struct {
	Tier        Tier
	Action      string
	ActorUserID string
	Subject     string
	GroupID     string
	Attributes  map[string]string
	// Left zero, it is stamped when the event is emitted.
	OccurredAt time.Time
}

// ActedAs returns a copy of ev credited to the real admin during act-as, with
// the user acted as kept in impersonated_user_id.
func (ev Event) ActedAs(impersonator string) Event {
	attrs := make(map[string]string, len(ev.Attributes)+1)
	maps.Copy(attrs, ev.Attributes)
	attrs["impersonated_user_id"] = ev.ActorUserID
	ev.Attributes = attrs
	ev.ActorUserID = impersonator
	return ev
}

// Publisher sends one message to the audit exchange. The go-rabbitmq
// publisher behind it sets ContentType.
type Publisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

// Emitter publishes events.
type Emitter struct{ pub Publisher }

// New returns an Emitter on pub.
func New(pub Publisher) *Emitter { return &Emitter{pub: pub} }

var (
	errNoTier   = errors.New("audit: event has no valid tier")
	errNoAction = errors.New("audit: event has no action")
)

// Emit publishes ev.
func (e *Emitter) Emit(ctx context.Context, ev Event) error {
	tier, ok := tiers[ev.Tier]
	if !ok {
		return errNoTier
	}
	if ev.Action == "" {
		return errNoAction
	}
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = time.Now().UTC()
	}
	body, err := proto.Marshal(&auditv1.AuditEvent{
		Tier: tier, Action: ev.Action, ActorUserId: ev.ActorUserID, Subject: ev.Subject,
		GroupId: ev.GroupID, OccurredAt: timestamppb.New(ev.OccurredAt), Attributes: ev.Attributes,
	})
	if err != nil {
		return fmt.Errorf("audit: encode event: %w", err)
	}
	return e.pub.Publish(ctx, "audit."+string(ev.Tier), body)
}

// Decode reads a published body back into an Event.
func Decode(body []byte) (Event, error) {
	var pe auditv1.AuditEvent
	if err := proto.Unmarshal(body, &pe); err != nil {
		return Event{}, err
	}
	ev := Event{
		Action: pe.GetAction(), ActorUserID: pe.GetActorUserId(), Subject: pe.GetSubject(),
		GroupID: pe.GetGroupId(), Attributes: pe.GetAttributes(), OccurredAt: pe.GetOccurredAt().AsTime(),
	}
	for name, t := range tiers {
		if t == pe.GetTier() {
			ev.Tier = name
		}
	}
	return ev, nil
}
