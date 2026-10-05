// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package readiness_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bugs5382/go-buildinfo/health"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-collab/internal/readiness"
)

type fakeDB struct{ down atomic.Bool }

func (f *fakeDB) Ping(context.Context) error {
	if f.down.Load() {
		return errors.New("connection refused")
	}
	return nil
}

func (*fakeDB) ServerVersion(context.Context) (string, error) { return "16.4", nil }

type fakeBroker struct{ down atomic.Bool }

func (f *fakeBroker) Healthy() bool { return !f.down.Load() }

type fakePeer struct{ down atomic.Bool }

func (f *fakePeer) check(context.Context) error {
	if f.down.Load() {
		return errors.New("unavailable")
	}
	return nil
}

func checker(t *testing.T, d readiness.Deps) *health.Checker {
	t.Helper()
	c, err := readiness.New(d, health.WithTTL(time.Millisecond), health.WithTimeout(time.Second))
	require.NoError(t, err)
	return c
}

func dep(t *testing.T, r health.Report, name string) health.DependencyReport {
	t.Helper()
	for _, d := range r.Dependencies {
		if d.Name == name {
			return d
		}
	}
	t.Fatalf("no %q in the report", name)
	return health.DependencyReport{}
}

func deps() (readiness.Deps, *fakeDB, *fakeBroker, *fakePeer, *fakePeer) {
	db, mq, idn, core := &fakeDB{}, &fakeBroker{}, &fakePeer{}, &fakePeer{}
	return readiness.Deps{Postgres: db, Broker: mq, Identity: idn.check, Core: core.check}, db, mq, idn, core
}

func TestEveryDependencyIsReported(t *testing.T) {
	d, _, _, _, _ := deps()
	r := checker(t, d).Report(context.Background())
	require.True(t, r.Ready)
	require.Equal(t, health.StateOK, r.Status)
	require.True(t, dep(t, r, readiness.Postgres).Required)
	require.True(t, dep(t, r, readiness.RabbitMQ).Required)
	require.False(t, dep(t, r, readiness.Identity).Required)
	require.False(t, dep(t, r, readiness.Core).Required)
	require.Equal(t, "16.4", dep(t, r, readiness.Postgres).Version)
}

func TestPostgresDownMakesCollabNotReadyAndRecovers(t *testing.T) {
	d, db, _, _, _ := deps()
	c := checker(t, d)
	db.down.Store(true)
	require.Eventually(t, func() bool { return !c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, health.StateDown, dep(t, c.Report(context.Background()), readiness.Postgres).State)
	db.down.Store(false)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
}

func TestRabbitMQDownMakesCollabNotReadyAndRecovers(t *testing.T) {
	d, _, mq, _, _ := deps()
	c := checker(t, d)
	mq.down.Store(true)
	require.Eventually(t, func() bool { return !c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, health.StateDown, dep(t, c.Report(context.Background()), readiness.RabbitMQ).State)
	mq.down.Store(false)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
}

func TestIdentityAndCoreDownDegradeButStayReady(t *testing.T) {
	d, _, _, idn, core := deps()
	c := checker(t, d)
	idn.down.Store(true)
	core.down.Store(true)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Status == health.StateDegraded }, 2*time.Second, 5*time.Millisecond)
	r := c.Report(context.Background())
	require.True(t, r.Ready, "live rooms keep relaying and checkpoints requeue while a peer is down")
	require.Equal(t, health.StateDegraded, dep(t, r, readiness.Identity).State)
	require.Equal(t, health.StateDegraded, dep(t, r, readiness.Core).State)
}

// The token verifier's key set is required: without it no call can be
// checked, so collab would refuse every caller.
func TestWorkloadKeysDownMakesCollabNotReadyAndRecovers(t *testing.T) {
	d, _, _, _, _ := deps()
	keys := &fakePeer{}
	d.WorkloadKeys = keys.check
	c := checker(t, d)
	require.True(t, dep(t, c.Report(context.Background()), readiness.WorkloadAuth).Required)
	keys.down.Store(true)
	require.Eventually(t, func() bool { return !c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
	keys.down.Store(false)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
}

// WORKLOAD_AUTH=disabled stays ready but reports degraded, so a run without
// authentication is visible.
func TestWorkloadAuthDisabledIsDegraded(t *testing.T) {
	d, _, _, _, _ := deps()
	d.WorkloadAuthDisabled = true
	c := checker(t, d)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Status == health.StateDegraded }, 2*time.Second, 5*time.Millisecond)
	r := c.Report(context.Background())
	require.True(t, r.Ready)
	require.False(t, dep(t, r, readiness.WorkloadAuth).Required)
}
