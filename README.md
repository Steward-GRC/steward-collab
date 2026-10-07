# steward-collab 🐹

> 🧭 Co-editing service for Steward: the live draft editing relay, co-editing tokens and snapshot flush

## 🎯 What it is

The collab service lets several people edit a policy draft at once. Browsers join a room per draft
over a websocket (through the gateway) and the relay fans out their y-websocket sync and presence
frames; it keeps no document of its own. Clients send snapshots of the Lexical content, which
collab passes to core, the draft's source of record. Over gRPC it mints the short-lived token a
browser joins with, and flushes and freezes a room around a publish.

- ✍️ **Snapshots** on a 5 second debounce, when the last editor leaves and before a publish; every
  editor is told whether core took them.
- 🔐 **Tokens** for editors only: the policy's owner, or an author scoped to its category.
- 🎭 **Act-as**: edits are made as the user acted as and credited to the real admin.

## 🚀 Run

```bash
cp .env.example .env   # Postgres, RabbitMQ, the token secret, core and identity addresses
task run
```

The image: `docker build --build-arg VERSION=<tag> --build-arg COMMIT=<sha> .`

## 📚 More

- [API, websocket frames and health](docs/api.md)
- [Configuration](docs/configuration.md)
- [Data and migrations](docs/data.md)
- [Error codes](docs/error-codes.md)
- [Runbook](docs/runbook.md)
- [y-protocols wire fixtures](test/wire/README.md)

- [Contributing](https://github.com/Steward-GRC/.github/blob/main/.github/CONTRIBUTING.md) and
  [security](https://github.com/Steward-GRC/.github/blob/main/.github/SECURITY.md)

## 🛠 Develop

```bash
task build    # go build ./...
task test     # go test ./... (Postgres and RabbitMQ tests need Docker)
task lint     # gofmt check + golangci-lint + yamllint
task proto    # fetch the pinned callee protos and regenerate gen/
task license  # check Apache-2.0 headers (golic)
```

## 🙏 Acknowledgements

Steward was originally written by [@Bugs5382](https://github.com/Bugs5382).

## ⚖️ License

Apache-2.0 (c) 2026 The Steward Authors
