# chora-notifications

Notifications service for Chora: templated email / push / in-app delivery, a
transactional outbox for the `chora.notifications.*` event streams, a federated
account-closure saga subscriber, and an email-send pipeline (SendGrid).

The service is cloud-neutral: PostgreSQL for persistence, NATS JetStream for the
event bus, and OTLP for tracing. No cloud account or managed services are
required.

## Local stack

The service runs against:

- **PostgreSQL** — `chora_notifications` database (notifications, templates,
  preferences, delivery logs, push subscriptions, outbox, idempotency keys,
  closure state)
- **NATS JetStream** — event bus (`CHORA_EVENTS` + `CHORA_DLQ` streams)

## Configuration

Copy `.env.example` to `.env` and adjust. Important variables:

| Variable | Purpose | Local default |
| --- | --- | --- |
| `PORT` | HTTP port | `8080` |
| `CHORA_GRPC_PORT` | gRPC port (`GRPC_PORT` accepted as alias) | `9090` |
| `CHORA_DB_DSN` | PostgreSQL connection string (app_rw role) | `postgres://chora:chora@localhost:5432/chora_notifications?sslmode=disable` |
| `CHORA_OUTBOX_DSN` | Durable outbox database | same as `CHORA_DB_DSN` |
| `NATS_URL` | NATS JetStream event bus | `nats://127.0.0.1:4222` |
| `CHORA_SOURCE_PROJECT` | source_project stamped on event envelopes | `chora-local` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC trace endpoint | unset (stdout) |

## Run locally

```sh
go run ./cmd/server
```

HTTP is exposed on `http://localhost:8080`; gRPC on `9090`.

Health endpoints (no auth):

```sh
curl -s http://localhost:8080/healthz
curl -s http://localhost:8080/readyz
```

Enqueue a notification (requires tenant + gcid headers):

```sh
curl -s -X POST http://localhost:8080/api/notifications \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Id: 01970000-0000-7000-8000-000000000001' \
  -H 'gcid: 01970000-0000-7000-9000-000000000001' \
  -d '{"recipient_gcid":"01970000-0000-7000-9000-000000000002","channel":"email","template_id":"welcome","payload":{"name":"Test"}}'
```

## Database

PostgreSQL is the durable backing store. Schema changes live in `migrations/`.
Forward migrations are `*.sql` files that are not `*.down.sql`, applied in
filename order by the platform migration runner.

## Event bus

Events use the `chora.{domain}.{aggregate}.{event_type}.v{N}` taxonomy,
brokered by `github.com/apollo-chora/chora-common/eventbus` over NATS JetStream.
Streams `CHORA_EVENTS` (`chora.>`) and `CHORA_DLQ` (`_dlq.>`) are provisioned
by the local NATS init. Consumers are durable and created on demand. If
`NATS_URL` is unset the service falls back to in-memory adapters (not durable).

## Architecture

Hexagonal layout:

```
cmd/server/          entrypoint, composition root, bootstrap wiring
internal/domain/     pure aggregates + repository ports (no infra deps)
internal/adapter/
  pg/                PostgreSQL repositories (notifications, delivery logs,
                     push subscriptions, closure state)
  outbox/            transactional outbox (store, publisher, dispatcher,
                     binary payload encoders)
  events/            eventbus subscribers (closure saga, fan-out, email-send,
                     web-push dispatcher) + protomarshal binary encoders
  http/              REST handlers + tenant middleware
  grpc/              NotificationsService gRPC adapter
  email/             template renderer + provider selection (SendGrid)
  sendgrid/          SendGrid v3 mail-send adapter
  clients/           identity gRPC client (gcid → email resolution)
  inmem/             in-memory repository implementations (dev/test)
internal/observability/  OTLP tracing (chora-common otel + observability)
```

The producer-side outbox writes events to `notifications_outbox_events` inside
the caller's transaction; a background dispatcher drains pending rows to the
event bus with a retry + dead-letter ladder. Email and in-app payloads are
stored as JSON and re-encoded to binary protobuf at publish time.

## Tests

```sh
go test ./...
```

Integration tests (build-tagged `integration`) run against a real PostgreSQL
when `CHORA_TEST_DSN` is set:

```sh
CHORA_TEST_DSN=postgres://chora:chora@localhost:5432/chora_notifications?sslmode=disable \
  go test -tags integration ./internal/adapter/pg/...
```
