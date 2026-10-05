// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ValidateLexicalJSON is a cheap pre-check on a client's Lexical EditorState
// JSON before it goes to core. It refuses empty input, malformed JSON or
// trailing data, a top-level value that isn't an object, and a missing or
// non-object "root". It checks the envelope only, never node types: core's
// shape-aware draft validation is the authority, and it alone can compare the
// snapshot with the stored draft.
func ValidateLexicalJSON(contentJSON string) error {
	if contentJSON == "" {
		return errors.New("empty content JSON")
	}

	// Use a strict decoder so we reject trailing garbage and unknown shapes.
	dec := json.NewDecoder(bytes.NewReader([]byte(contentJSON)))

	// Top-level must be a JSON object — peek the first token.
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("top-level value must be a JSON object, got %v", tok)
	}

	// Decode into a structured shape from the same byte slice. We reuse a fresh
	// decoder because we've already consumed the opening brace above.
	var doc struct {
		Root *json.RawMessage `json:"root"`
	}
	if err := json.Unmarshal([]byte(contentJSON), &doc); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Trailing garbage check: after decoding the top-level object the stream
	// must be at EOF.
	trailDec := json.NewDecoder(bytes.NewReader([]byte(contentJSON)))
	var discard any
	if err := trailDec.Decode(&discard); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if trailDec.More() {
		return errors.New("invalid JSON: trailing data after top-level value")
	}

	if doc.Root == nil {
		return errors.New("missing required field: root")
	}

	// "root" must itself be a JSON object (not null, not array, not scalar).
	raw := bytes.TrimSpace([]byte(*doc.Root))
	if len(raw) == 0 || raw[0] != '{' {
		return fmt.Errorf("root must be a JSON object")
	}

	// Decode root just enough to surface decode errors on malformed objects.
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return fmt.Errorf("invalid root object: %w", err)
	}

	return nil
}
