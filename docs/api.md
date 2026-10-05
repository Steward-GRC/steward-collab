# API

steward-collab serves two gRPC services on `GRPC_PORT` and a websocket endpoint on `HTTP_PORT`.
The proto is `proto/steward/collab/v1/collab.proto` (package `steward.collab.v1`); the Go stubs
are committed under `gen/go/steward/collab/v1`. Every refusal is a coded error
([error-codes.md](error-codes.md)).

Every call carries the caller's projected service-account token (audience `steward`) as
`authorization: Bearer`. Collab verifies it against the cluster's JWKS and checks a per-method
allow-list in code (`internal/grpcsvc/policy.go`): only the gateway (`steward/steward-gateway`)
may call, on every method, acting for a signed-in user. A missing or bad token is
`Unauthenticated`; a caller the method doesn't list is `PermissionDenied`; both are audited as
`collab.call.refused`. The gRPC health service needs no token.

Every call also needs a go-grpc-actor actor (`ACTOR_REQUIRED` otherwise). An actor is believed only
from a caller the method lists as acting on a user's behalf.

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
| `FlushDraft` | Sends the room's newest checkpoint to core and waits for core's answer, including a debounced flush already in flight. `FLUSH_REJECTED` when core refused the content (core's detail in the message, its reason and detail in the metadata), `FLUSH_UNAVAILABLE` or `FLUSH_TIMEOUT` when core couldn't answer; the checkpoint then stays queued. The caller must not publish on any error. |
| `NotifyDraftPublished` | Latches the room read-only and sends every editor a `draft.published` frame with the version number. `VERSION_NUMBER_INVALID` for a number below 1. |

## Websocket

`GET /ws/{draft_id}` on `HTTP_PORT`, reached through the gateway's proxy at `/collab/ws/{draft_id}`.
The token goes in `?token=` or `Authorization: Bearer`. It must be HS256 with audience
`steward.collab.ws`, unexpired, and name the same draft as the path; otherwise the answer is 401.
A request with a browser `Origin` is refused unless it is in `COLLAB_ALLOWED_ORIGINS`: browsers come
through the gateway, which sends none.

The relay speaks the y-websocket protocol and keeps no document of its own: sync and awareness
frames fan out to the room as they are. Two frame types are collab's own, with the same envelope
and length-prefixed fields:

| Type | Direction | Payload |
| --- | --- | --- |
| `100` snapshot | client to server | the Lexical EditorState JSON, then the client's compacted Yjs state (either may be empty). The JSON goes to core on a 5 second debounce, when the last editor leaves and on `FlushDraft`; the Yjs state is stored as is so a room survives a restart. |
| `101` control | server to client | JSON: `{"type", "draft_id", "reason", "detail", "version_number"}`. Types: `snapshot.accepted`, `snapshot.rejected` (reason `pre_check_failed`, `grpc_error` or `server_rejected`, with core's detail), `draft.published` (the room is read-only now). |

Presence is bound to the token: every awareness state's `user` is replaced with
`{"uid", "name", "color", "colorIndex"}`. `colorIndex` (0 to 7) is the position in the design's
collaborator palette, so the web picks its theme's colour; `color` is the light-theme value.

A snapshot reaches core as the token's user. When the token was minted during act-as, the real
admin travels with it through go-grpc-actor and is credited in collab's audit events
(`impersonated_user_id` names the user acted as).

## Health

The gRPC health service answers for `""` and `readiness` (follows the required dependencies) and
`liveness` (the process only). `GET /livez` and `GET /readyz` on `HTTP_PORT` say the same. Every
answer carries `steward-version`, `steward-commit`, `steward-dep-postgres` and a
`steward-depstate-<name>` header for `postgres`, `rabbitmq`, `core` and `identity` (on HTTP:
`Steward-Version` and so on). See the [runbook](runbook.md).

## Calling other services

Collab never imports another service's Go module. It calls them through protos pinned in
`proto-refs.env` and generated into `gen/go/thirdparty` by `scripts/proto-generate.sh`, with its
own service-account token (`WORKLOAD_TOKEN_FILE`) and the request's actor on every call:

| Service | Calls | Why |
| --- | --- | --- |
| steward-core | `PolicyService.GetPolicy`, `PolicyService.UpdateDraftContent`, `CategoryService.GetCategory` | the edit check (owner, home category and its ancestors) and every snapshot |
| steward-identity | `IdentityReadService.GetUser` | the caller's roles and scoped grants for the edit check, and the presence name |
| steward-audit | publishes `steward.audit.v1.AuditEvent` to the `audit` exchange | `collab.token.issued`, `collab.session.joined` and `left`, `collab.snapshot.accepted` and `rejected`, `collab.room.frozen` |

Bump a ref, run `task proto` and commit `gen/` in the same change.

`internal/workloadauth` is a copy of steward-core's and is never edited here.
`scripts/workloadauth-check.sh` (run in CI) compares it byte for byte with core's at
`STEWARD_CORE_REF` and fails on any difference, or when core has no copy at that commit.
