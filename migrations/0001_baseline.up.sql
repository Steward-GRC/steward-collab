-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- One row per draft with a co-editing room: the last document state a client
-- compacted and sent with a snapshot, stored as is. It is replaced on every
-- snapshot, so a row tracks the document's size, not its edit history.
CREATE TABLE collab_documents (
    draft_id            TEXT        NOT NULL PRIMARY KEY,
    policy_id           TEXT        NOT NULL,
    template_version_id TEXT        NOT NULL,
    yjs_state           BYTEA       NOT NULL DEFAULT '',
    last_snapshot_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_collab_documents_policy_id ON collab_documents (policy_id);
