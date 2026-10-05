// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"context"
	"sync"
	"testing"

	grpcactor "github.com/Bugs5382/go-grpc-actor"

	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/audit"
)

const testSecret = "test-secret-32-bytes-long-------"

// fakeAuditPublisher records every audit message published through it.
type fakeAuditPublisher struct {
	mu       sync.Mutex
	messages []fakeAuditMessage
}

type fakeAuditMessage struct {
	RoutingKey string
	Event      audit.Event
}

func (f *fakeAuditPublisher) Publish(_ context.Context, routingKey string, body []byte) error {
	ev, err := audit.Decode(body)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, fakeAuditMessage{RoutingKey: routingKey, Event: ev})
	return nil
}

func (f *fakeAuditPublisher) snapshot() []fakeAuditMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeAuditMessage(nil), f.messages...)
}

func newTestEmitter() (*audit.Emitter, *fakeAuditPublisher) {
	fap := &fakeAuditPublisher{}
	return audit.New(fap), fap
}

func noopEmitter() *audit.Emitter {
	em, _ := newTestEmitter()
	return em
}

// actorCtx is a context carrying a as the go-grpc-actor actor, as the
// gateway's client interceptor would send it.
func actorCtx(t *testing.T, a grpcactor.Actor) context.Context {
	t.Helper()
	return grpcactor.WithActor(t.Context(), a)
}

// publisher is the caller the gateway forwards on a publish: an actor is all
// the room calls require.
func publisher() grpcactor.Actor { return grpcactor.Actor{Subject: "u-bob"} }

// fakeEditChecker answers the edit check from a fixed table.
type fakeEditChecker struct {
	allowed map[string]bool
	names   map[string]string
	err     error
	calls   []string
}

func (f *fakeEditChecker) CanEdit(_ context.Context, userID, policyID string) (access.Result, error) {
	f.calls = append(f.calls, userID+"@"+policyID)
	if f.err != nil {
		return access.Result{}, f.err
	}
	return access.Result{Allowed: f.allowed[userID], Name: f.names[userID]}, nil
}

func allowAll(users ...string) *fakeEditChecker {
	f := &fakeEditChecker{allowed: map[string]bool{}, names: map[string]string{}}
	for _, u := range users {
		f.allowed[u] = true
	}
	return f
}
