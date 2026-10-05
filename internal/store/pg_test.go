// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"

	"github.com/Steward-GRC/steward-collab/internal/store"
)

func TestPgStore_SaveAndLoad(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()

	s := store.New(pool)

	doc := store.Document{
		DraftID:           "test-draft-1",
		PolicyID:          "policy-99",
		TemplateVersionID: "tmpl-v1",
		YjsState:          []byte{0x01, 0x02, 0x03},
	}
	if err := s.Save(ctx, doc); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.Load(ctx, "test-draft-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got == nil {
		t.Fatal("expected document, got nil")
	}
	if string(got.YjsState) != string(doc.YjsState) {
		t.Fatalf("yjs state mismatch: got %v want %v", got.YjsState, doc.YjsState)
	}
	if got.PolicyID != "policy-99" {
		t.Fatalf("policy id: got %q want %q", got.PolicyID, "policy-99")
	}
	if got.TemplateVersionID != "tmpl-v1" {
		t.Fatalf("template version id: got %q want %q", got.TemplateVersionID, "tmpl-v1")
	}
	if got.DraftID != "test-draft-1" {
		t.Fatalf("draft id: got %q want %q", got.DraftID, "test-draft-1")
	}

	// Save again (upsert) with updated state.
	doc.YjsState = []byte{0x04, 0x05}
	if err := s.Save(ctx, doc); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got2, err := s.Load(ctx, "test-draft-1")
	if err != nil {
		t.Fatalf("load after upsert: %v", err)
	}
	if string(got2.YjsState) != string([]byte{0x04, 0x05}) {
		t.Fatalf("upsert did not update yjs state: got %v", got2.YjsState)
	}
}

func TestPgStore_LoadMissing(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()

	s := store.New(pool)
	got, err := s.Load(ctx, "does-not-exist-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil for missing document")
	}
}
