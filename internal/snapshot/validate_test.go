// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot_test

import (
	"encoding/json"
	"testing"

	"github.com/Steward-GRC/steward-collab/internal/snapshot"
)

// ValidateLexicalJSON is a cheap pre-check the collab service runs on
// client-supplied Lexical EditorState JSON before forwarding to the Policy
// service. It only rejects obvious shape errors (empty input, malformed JSON,
// missing root, root not a JSON object). Deep structural validation
// (section count, boilerplate, region keys) lives in the policy core
// validator and is enforced by the UpdateDraftContent handler.

func TestValidateLexicalJSON_AcceptsMinimalValidDoc(t *testing.T) {
	valid := `{"root":{"type":"root","children":[]}}`
	if err := snapshot.ValidateLexicalJSON(valid); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestValidateLexicalJSON_AcceptsWellFormedDocWithChildren(t *testing.T) {
	valid := `{"root":{"type":"root","children":[{"type":"policy-section","sectionKey":"s1","order":0,"children":[]}]}}`
	if err := snapshot.ValidateLexicalJSON(valid); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestValidateLexicalJSON_RejectsEmptyString(t *testing.T) {
	if err := snapshot.ValidateLexicalJSON(""); err == nil {
		t.Fatal("expected error for empty string")
	}
}

func TestValidateLexicalJSON_RejectsInvalidJSON(t *testing.T) {
	if err := snapshot.ValidateLexicalJSON("{not json}"); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestValidateLexicalJSON_RejectsTrailingGarbage(t *testing.T) {
	if err := snapshot.ValidateLexicalJSON(`{"root":{"type":"root","children":[]}}xxx`); err == nil {
		t.Fatal("expected error for JSON with trailing garbage")
	}
}

func TestValidateLexicalJSON_RejectsMissingRoot(t *testing.T) {
	b, _ := json.Marshal(map[string]any{"other": "field"})
	if err := snapshot.ValidateLexicalJSON(string(b)); err == nil {
		t.Fatal("expected error for missing root field")
	}
}

func TestValidateLexicalJSON_RejectsRootAsString(t *testing.T) {
	b, _ := json.Marshal(map[string]any{"root": "not-an-object"})
	if err := snapshot.ValidateLexicalJSON(string(b)); err == nil {
		t.Fatal("expected error for root as string")
	}
}

func TestValidateLexicalJSON_RejectsRootAsNumber(t *testing.T) {
	b, _ := json.Marshal(map[string]any{"root": 42})
	if err := snapshot.ValidateLexicalJSON(string(b)); err == nil {
		t.Fatal("expected error for root as number")
	}
}

func TestValidateLexicalJSON_RejectsRootAsArray(t *testing.T) {
	b, _ := json.Marshal(map[string]any{"root": []any{1, 2, 3}})
	if err := snapshot.ValidateLexicalJSON(string(b)); err == nil {
		t.Fatal("expected error for root as array")
	}
}

func TestValidateLexicalJSON_RejectsRootAsNull(t *testing.T) {
	if err := snapshot.ValidateLexicalJSON(`{"root":null}`); err == nil {
		t.Fatal("expected error for root as null")
	}
}

func TestValidateLexicalJSON_RejectsTopLevelArray(t *testing.T) {
	if err := snapshot.ValidateLexicalJSON(`[1,2,3]`); err == nil {
		t.Fatal("expected error for top-level array")
	}
}

func TestValidateLexicalJSON_RejectsTopLevelScalar(t *testing.T) {
	if err := snapshot.ValidateLexicalJSON(`"hello"`); err == nil {
		t.Fatal("expected error for top-level scalar string")
	}
}
