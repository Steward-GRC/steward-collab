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

| `IssueAppendixToken` | The same for an appendix's own room, `appendix:<appendix_id>`. Takes only `appendix_id`; collab reads the appendix's policy from core (`GetPolicyAppendix`) and runs the edit check against it, so a caller cannot pair an appendix with a policy it may edit. Returns the token, the websocket path (`/collab/ws/appendix%3A<appendix_id>`), the expiry, the `room_key` and the `policy_id`. |

The edit check: the policy's owner; or Author or Manage allowed by the category rules over the
policy's home category and its ancestors (read from core in one `GetEffectiveRuleset` call and
decided by `go-authz/ordered`: first non-blank cell wins, the category's own rows before its
parents', owners of any level always allowed and immune to deny, the root seat exempt); or a
caller whose catalog `policy.author` grant (`go-authz/rbac`) is scoped to the home category or one
of its ancestors. A site admin who is not root gets no edit right without a rule. The same check
runs again on every websocket join, before the upgrade: a user who lost the right since the token
was minted gets 403 `DRAFT_EDIT_FORBIDDEN`, and 503 `EDIT_CHECK_UNAVAILABLE` when core or identity
can't be reached. Refused with `DRAFT_EDIT_FORBIDDEN`; `POLICY_NOT_FOUND` when core has no such
policy; `EDIT_CHECK_UNAVAILABLE` when core or identity can't be reached.

`IssueAppendixToken` refuses with `APPENDIX_ID_REQUIRED`, `APPENDIX_NOT_FOUND` (checked first),
then the edit check above, then `APPENDIX_RETIRED`: a person who may not edit learns nothing about
a retired appendix. `IssueToken` refuses a `draft_id` that starts with `appendix:`
(`DRAFT_ID_RESERVED`), because the room key decides which core call a checkpoint goes to.

## Appendix rooms

An appendix has a room of its own, keyed `appendix:<appendix_id>`, with its own token. It behaves
as a policy room does: the same checkpoint pre-check (`pre_check_failed` at once for content that
is not a JSON object with a `root`), the same `snapshot.rejected` with `server_rejected` when core
refuses, the same acknowledgment after core answers, no reply to a Yjs-only checkpoint, the same
flush and the same read-only freeze. Only the core call differs: a checkpoint goes to
`PolicyAppendixService.CheckpointAppendixDraft(id, policy_id, content_json, actor_user_id)`, which
validates the content, makes no draft when it equals the published row, updates the open draft or
starts the next one, refuses a retired appendix and records the actor as a contributor. The control
frames name the room key in `draft_id`.

To publish an appendix the gateway uses the room calls below with `draft_id` set to the room key
(`room_key` from `IssueAppendixToken`) and `policy_id` the appendix's policy: `FlushDraft` first,
then core's `PublishAppendix`, then `NotifyDraftPublished`. The websocket path of the token is
`/ws/appendix:<id>` (the gateway forwards it percent-decoded or not; the handler reads it decoded).

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

A checkpoint the room will not keep is answered to the sender alone, at once, and changes nothing
(nothing is held for the debounce, stored or sent to core): `snapshot.rejected` with reason
`pre_check_failed` when the content JSON is not a JSON object with a `root` object, and
`server_rejected` when the draft is published. An empty content JSON is not refused: a checkpoint
may carry only the Yjs state.

### Where collab differs from the mock server on purpose

The mock server (steward-mock-server, `src/product/collab.ts`) models this relay for the web
editor. These differences are kept; each has a reason.

| Mock | Collab | Why |
| --- | --- | --- |
| `snapshot.accepted` at once. | Sent after core answers: up to 5 seconds of debounce, then the call. | The editor reads `accepted` as "core holds this" (web `protocol.ts`). An early ack would say so before core agreed, and `snapshot.rejected` from core would arrive after the editor had been told it was safe. |
| The ack and the refusal go to the sender. | Core's answer goes to every editor in the room. | The debounce merges checkpoints from several editors into one call, so one answer covers them all, and everyone should hear that the content is not durable. Only the immediate refusals above are for the sender alone. |
| Rejects a checkpoint for a version that is not editable. | Core decides, so the refusal arrives after the debounce as `grpc_error` with core's detail. A published draft is refused at once. | Collab holds no draft status; core owns it. |
| Rejects a checkpoint core refuses and keeps nothing. | Core's refusal still stores the checkpoint's Yjs state. | The editors' documents already hold those edits; the room has to start the next session from what they hold. The refusal is reported on the control channel. A checkpoint refused by the pre-check, or on a published draft, keeps nothing. |
| A checkpoint with empty content is a malformed JSON refusal. | It is accepted as a Yjs-only checkpoint: the state is stored, nothing goes to core and no control frame comes back. | The frame allows either part to be empty. |
| Keeps every sync update in the room and replays them to a joiner's sync step 1. | Keeps no document. Sync step 1 is answered with the last checkpoint's Yjs state, and a joiner makes the editors already in the room send their full document, which reaches the joiner as a sync step 2. | Collab runs no Y.Doc, so it cannot merge updates. Edits made since the last checkpoint live in the editors' own documents, and are lost only if the last editor leaves before a checkpoint arrives. The mock keeps them only until its room empties, too. |
| A document update goes to the other editors only. | A document update is fanned out to every editor in the room, the sender included. | Applying an update twice is harmless in Yjs. |

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
| steward-core | `PolicyService.GetPolicy`, `PolicyService.UpdateDraftContent`, `CategoryService.GetEffectiveRuleset`, `PolicyAppendixService.GetPolicyAppendix`, `PolicyAppendixService.CheckpointAppendixDraft` | the edit check (owner, home category and its ancestors' rules and owners), every policy snapshot, the appendix lookup behind `IssueAppendixToken` and every appendix snapshot |
| steward-identity | `IdentityReadService.GetUser` | the caller's roles, scoped grants, groups and root flag for the edit check, and the presence name |
| steward-audit | publishes `steward.audit.v1.AuditEvent` to the `audit` exchange | `collab.token.issued`, `collab.session.joined` and `left`, `collab.snapshot.accepted` and `rejected`, `collab.room.frozen` |

Bump a ref, run `task proto` and commit `gen/` in the same change.

Service-to-service authentication, both verifying callers and sending collab's own token, comes
from `github.com/Bugs5382/go-workload-identity`. Steward's audience (`steward`) and caller-name
mapping (`steward-gateway` is the caller `gateway`) are supplied in `internal/config`
(`config.StewardWorkload`). `STEWARD_CORE_REF` pins only the generated protos.
