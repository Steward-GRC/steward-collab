# API

steward-collab serves two gRPC services on `GRPC_PORT` and a websocket endpoint on `HTTP_PORT`.
The proto is `proto/steward/collab/v1/collab.proto` (package `steward.collab.v1`); the Go stubs
are committed under `gen/go/steward/collab/v1`. Every refusal is a coded error
([error-codes.md](error-codes.md)).

Every call needs a go-grpc-actor actor (`ACTOR_REQUIRED` otherwise). Forwarded actors are believed
only from the SPIFFE IDs in `COLLAB_TRUSTED_CALLERS` over mTLS.

## CollabTokenService

| Call | What it does |
| --- | --- |
| `IssueToken` | Checks that the caller may edit the draft, then mints a short-lived token for the room. The token names the effective actor; during act-as it also names the real admin. Returns the token, the websocket path (`/collab/ws/<draft_id>`, behind the gateway) and the expiry. |

The edit check: the policy's owner, or a caller whose `policy.author` grant (steward-authz) is
scoped to the policy's home category or one of its ancestors. Site admins pass through
steward-authz. Refused with `DRAFT_EDIT_FORBIDDEN`; `POLICY_NOT_FOUND` when core has no such
policy; `EDIT_CHECK_UNAVAILABLE` when core or identity can't be reached.

## CollabRoomService

The gateway publishes a draft in three steps: `FlushDraft`, then core's `PublishDraft`, then
`NotifyDraftPublished`. Both calls check the request's `policy_id` against the live room's own and
refuse a mismatch (`ROOM_POLICY_MISMATCH`). No live room is the normal case and is not an error.

| Call | What it does |
| --- | --- |
| `FlushDraft` | Sends the room's newest checkpoint to core and waits for core's answer, including a debounced flush already in flight. `FLUSH_REJECTED` when core refused the content (core's reason and detail in the metadata), `FLUSH_UNAVAILABLE` or `FLUSH_TIMEOUT` when core couldn't answer; the checkpoint then stays queued. The caller must not publish on any error. |
| `NotifyDraftPublished` | Latches the room read-only and sends every editor a `draft.published` frame with the version number. `VERSION_NUMBER_INVALID` for a number below 1. |
