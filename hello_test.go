// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0
package stewardcollab

import "testing"

func TestHello(t *testing.T) {
	got := Hello("world")
	want := "Hello, world!"
	if got != want {
		t.Errorf("Hello() = %q, want %q", got, want)
	}
}
