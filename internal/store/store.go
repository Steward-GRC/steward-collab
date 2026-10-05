// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package store keeps each co-editing room's last compacted document state in
// Postgres, so a room survives a restart.
package store

import (
	"context"
	"errors"

	postgres "github.com/Bugs5382/go-postgres"
	"github.com/jackc/pgx/v5"
)

// Document is the stored state of one draft's room.
type Document struct {
	DraftID           string
	PolicyID          string
	TemplateVersionID string
	YjsState          []byte
}

// Store loads and saves room state.
type Store interface {
	// Load returns nil, nil when the draft has no stored state.
	Load(ctx context.Context, draftID string) (*Document, error)
	// Save replaces the draft's stored state.
	Save(ctx context.Context, doc Document) error
}

type pgStore struct{ db *postgres.DB }

// New returns a Store on db.
func New(db *postgres.DB) Store { return &pgStore{db: db} }

func (s *pgStore) Load(ctx context.Context, draftID string) (*Document, error) {
	var d Document
	err := s.db.Querier().QueryRow(ctx,
		`SELECT draft_id, policy_id, template_version_id, yjs_state
		   FROM collab_documents WHERE draft_id = $1`, draftID).
		Scan(&d.DraftID, &d.PolicyID, &d.TemplateVersionID, &d.YjsState)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *pgStore) Save(ctx context.Context, doc Document) error {
	_, err := s.db.Querier().Exec(ctx,
		`INSERT INTO collab_documents (draft_id, policy_id, template_version_id, yjs_state, updated_at)
		 VALUES ($1, $2, $3, $4, now())
		 ON CONFLICT (draft_id) DO UPDATE
		   SET yjs_state = EXCLUDED.yjs_state,
		       policy_id = EXCLUDED.policy_id,
		       template_version_id = EXCLUDED.template_version_id,
		       updated_at = now()`,
		doc.DraftID, doc.PolicyID, doc.TemplateVersionID, doc.YjsState)
	return err
}
