# Runbook

## One replica

Rooms are process-local: the room registry, each room's document and its presence. Two editors of
one draft on different replicas get two rooms that never see each other. Run collab as one
replica, with no autoscaling, until draft-affinity routing or a shared backplane exists.

## Health

- **Liveness**: `GET /livez` on `HTTP_PORT`, or the gRPC health check for `liveness`. The process
  only, never a dependency, so an outage never restarts the pod.
- **Readiness**: `GET /readyz`, or the gRPC health check for `""` or `readiness`. Postgres and
  RabbitMQ are required, and so is the token verifier's key set (`workloadauth`) while
  authentication is on: while any is down, readiness is `NOT_SERVING` (HTTP 503) and recovers
  on its own. Core and identity are optional and show as `degraded`: live rooms keep relaying and
  checkpoints requeue while core is down, and only new tokens need identity. Checks are
  short-timeout pings, cached for five seconds.
- The `/readyz` body lists each dependency's state, whether it's required, the last error class,
  the time of the check and its version.
- Read the build and dependency headers with grpcurl:

  ```sh
  grpcurl -v -plaintext localhost:9090 grpc.health.v1.Health/Check
  curl -si localhost:8081/readyz
  ```

## Refused calls

Every call but health needs a service-account token. Refusals are logged and audited as
`collab.call.refused` with the method, the caller, the code and the reason:

- `Unauthenticated`: no token, or one the JWKS doesn't verify. Check the caller's projected token
  mount and its audience (`steward`).
- `PermissionDenied`: a valid token from a service account the method doesn't list, or one that
  isn't in `WORKLOAD_ALLOWED_SERVICEACCOUNTS`.
- `Unavailable`: the JWKS hasn't loaded, so no token can be checked. Readiness reports
  `workloadauth` down meanwhile.

## Snapshots that don't persist

Each editor is told: a `snapshot.rejected` control frame with the reason and core's detail.

- `pre_check_failed`: the client sent JSON with no `root` object. A client bug.
- `grpc_error` with `InvalidArgument` or `FailedPrecondition`: core refused the content (template
  mismatch, failed validation, an empty snapshot over a draft with text). The author fixes it; the
  refusal stands until a newer checkpoint is accepted.
- `grpc_error` with `Unavailable` or `DeadlineExceeded`: core couldn't answer. The checkpoint stays
  queued and is resent on the next snapshot or flush.

Every attempt is audited as `collab.snapshot.accepted` or `collab.snapshot.rejected`. A snapshot
whose actor is missing or not a UUID is attributed to the fixed actor
`00000000-0000-0000-0000-0000000c011a` and logged.

## Publish refused

`FlushDraft` errors stop the gateway's publish on purpose: `FLUSH_REJECTED` means core refused
the room's newest content, `FLUSH_UNAVAILABLE` and `FLUSH_TIMEOUT` mean core didn't answer and the
publish can be retried.
