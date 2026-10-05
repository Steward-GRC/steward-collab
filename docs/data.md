# Data

steward-collab owns one Postgres database. The schema is `migrations/0001_baseline.up.sql`, applied
at start-up with go-postgres (`MIGRATIONS_DIR`, through `MIGRATE_DSN`).

## collab_documents

One row per draft that has had a co-editing room.

| Column | Meaning |
| --- | --- |
| `draft_id` | The draft version the room is keyed by (primary key). |
| `policy_id` | The draft's policy. Indexed. |
| `template_version_id` | The template version the draft is pinned to; empty for a freeform draft. |
| `yjs_state` | The last full document state a client compacted and sent with a snapshot, stored as is. Replaced on every snapshot. |
| `last_snapshot_at` | Reserved; not written. |
| `created_at`, `updated_at` | Row times. |

The stored state only lets a room resume after a restart. The draft's content of record is in core,
which every snapshot updates.
