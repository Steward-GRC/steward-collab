# Configuration

Every setting is an environment variable, read and checked at start-up; a bad value stops the
service. `.env.example` lists them with local values.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_DSN` | required | Postgres for the room state. |
| `MIGRATE_DSN` | `DATABASE_DSN` | A direct connection for migrations. |
| `MIGRATIONS_DIR` | `migrations` | Where the SQL migrations are (`/migrations` in the image). |
| `RABBITMQ_URL` | required | The broker for audit events. |
| `COLLAB_TOKEN_SECRET` | required | Signs and verifies the co-editing tokens. At least 32 bytes. |
| `COLLAB_TOKEN_TTL_SECONDS` | `300` | Token lifetime, above 0. |
| `COLLAB_WS_BASE` | empty | An origin put in front of the issued websocket path, for a web app not served from the gateway's origin. Empty gives `/collab/ws/<draft_id>`. |
| `COLLAB_ALLOWED_ORIGINS` | empty | Comma-separated browser origins allowed to upgrade on collab's own listener. Empty refuses every browser. |
| `CORE_GRPC_ADDR` | `core:9090` | steward-core. |
| `IDENTITY_GRPC_ADDR` | `identity:9090` | steward-identity. |
| `GRPC_PORT` | `9090` | The gRPC services and health. |
| `HTTP_PORT` | `8081` | The websocket upgrades, `/livez` and `/readyz`. |
| `WORKLOAD_OIDC_ISSUER` | required | The cluster's service-account token issuer (https). Without it collab won't start unless `WORKLOAD_AUTH=disabled`. |
| `WORKLOAD_OIDC_JWKS_URL` | discovered | Overrides the issuer's JWKS URL (https). |
| `WORKLOAD_OIDC_CA_FILE`, `WORKLOAD_OIDC_BEARER_FILE` | empty | A CA bundle and a bearer token for fetching the JWKS. |
| `WORKLOAD_AUDIENCE` | `steward` | The audience a caller's token must carry. |
| `WORKLOAD_ALLOWED_SERVICEACCOUNTS` | required with the issuer | Comma-separated `<namespace>/<serviceaccount>` that may call collab at all: `steward/steward-gateway`. The per-method list is in code. |
| `WORKLOAD_TOKEN_FILE` | `/var/run/secrets/steward/token` | Collab's own projected token, sent to core and identity on every call and re-read each time. A missing file stops the boot. |
| `WORKLOAD_AUTH` | enabled | `disabled` turns service-to-service authentication off, for local runs only: every caller is let in, a warning is logged every five minutes and readiness reports `workloadauth` degraded. Any other value is an error. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | Traces and metrics. |
| `LOG_LEVEL`, `LOG_FORMAT` | `info`, `json` | go-log. Local runs use `trace` and `console`. |

## Image

The Dockerfile takes `VERSION` (the image tag) and `COMMIT` (the full source SHA) and stamps them
into go-buildinfo with `-ldflags -X`. An unstamped build reports `dev` and commit `unknown`.

```sh
docker build --build-arg VERSION=v0.1.0 --build-arg COMMIT="$(git rev-parse HEAD)" -t steward-collab .
```
