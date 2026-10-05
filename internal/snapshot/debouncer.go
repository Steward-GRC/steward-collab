// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"sync"
	"time"
)

// Debouncer coalesces a burst of Trigger calls into a single delayed callback.
// Each Trigger resets the wait timer; the callback fires once after no Trigger
// has been received for the configured wait duration. The callback runs on a
// goroutine managed by time.AfterFunc.
//
// Debouncer is safe for concurrent use. Callers should call Stop when the
// owning resource (e.g. a Room) is being torn down to cancel any pending fire.
type Debouncer struct {
	wait time.Duration

	mu    sync.Mutex
	timer *time.Timer
}

// NewDebouncer returns a Debouncer that waits for the given quiet period
// before firing the most-recently-supplied callback.
func NewDebouncer(wait time.Duration) *Debouncer {
	return &Debouncer{wait: wait}
}

// Trigger arms (or re-arms) the debouncer with fn. If a previous Trigger is
// pending its timer is reset and fn replaces the previously-supplied callback.
func (d *Debouncer) Trigger(fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		// Drop the prior pending callback by stopping its timer, then replace
		// it with a new AfterFunc bound to fn. Stop's return value is ignored
		// because if the prior timer already fired, the new AfterFunc is the
		// authoritative scheduled callback going forward.
		d.timer.Stop()
	}
	d.timer = time.AfterFunc(d.wait, func() {
		d.mu.Lock()
		// Clear the timer slot so a future Trigger schedules afresh.
		d.timer = nil
		d.mu.Unlock()
		fn()
	})
}

// Stop cancels any pending callback. Safe to call multiple times.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
}
