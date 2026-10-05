// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

// Debouncer schedules a callback to fire once after a quiet period. Each
// call to Trigger resets the timer; Stop cancels any pending fire.

func TestDebouncer_FiresAfterQuietPeriod(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := snapshot.NewDebouncer(20 * time.Millisecond)
	defer d.Stop()

	d.Trigger(func() { calls.Add(1) })

	// Wait long enough for the timer to fire.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("debounced callback did not fire; calls=%d", calls.Load())
}

func TestDebouncer_CoalescesRapidCalls(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := snapshot.NewDebouncer(40 * time.Millisecond)
	defer d.Stop()

	// 5 triggers in rapid succession should produce exactly one callback.
	for range 5 {
		d.Trigger(func() { calls.Add(1) })
		time.Sleep(5 * time.Millisecond)
	}

	// Wait well past the debounce window.
	time.Sleep(150 * time.Millisecond)

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 callback after coalescing; got %d", got)
	}
}

func TestDebouncer_StopCancelsPendingFire(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := snapshot.NewDebouncer(50 * time.Millisecond)

	d.Trigger(func() { calls.Add(1) })
	d.Stop()

	time.Sleep(150 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected 0 callbacks after Stop; got %d", got)
	}
}

func TestDebouncer_RefiresAfterFireCompletes(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := snapshot.NewDebouncer(20 * time.Millisecond)
	defer d.Stop()

	d.Trigger(func() { calls.Add(1) })
	time.Sleep(100 * time.Millisecond) // let it fire

	d.Trigger(func() { calls.Add(1) })
	time.Sleep(100 * time.Millisecond) // let it fire again

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 callbacks across two windows; got %d", got)
	}
}
