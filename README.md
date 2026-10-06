# chora-notifications

## About

chora-notifications is the Notifications supporting service for Chora. It stores notifications, append-only versioned templates, and per-user channel preferences, and exposes the service over HTTP and gRPC. It also consumes and publishes Chora events through NATS JetStream, includes a transactional outbox, and can send email through SendGrid.

## Quick start

### Prerequisites

- Go 1.26.1 or newer
- PostgreSQL for durable storage
- NATS JetStream for the event-driven production wiring

The process can also start without PostgreSQL or NATS. With those environment variables unset, it uses in-memory repositories and does not run durable event subscribers or the outbox dispatcher.

Copy the example configuration:

```sh
cp .env.example .env
```

For local PostgreSQL, the example configuration uses:

```
postgres://chora:chora@localhost:5432/chora_notifications?sslmode=disable
```

Apply the SQL migrations in `migrations/` in filename order with the platform migration runner.

Start the service:

```sh
go run ./cmd/server
```

The default listeners are:

- HTTP: `http://localhost:8080`
- gRPC: `:9090`

Check health:

```sh
curl -s http://localhost:8080/healthz
curl -s http://localhost:8080/readyz
```

## Usage

### HTTP

Health and root endpoints do not require tenant headers:

```text
GET  /healthz
GET  /healthz/
GET  /health
GET  /readyz
GET  /
```

Protected `/api/*` requests require both `X-Tenant-Id` and either `gcid` or `X-Chora-GCID`.

Create a notification:

```sh
curl -s -X POST http://localhost:8080/api/notifications \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Id: 01970000-0000-7000-8000-000000000001' \
  -H 'gcid: 01970000-0000-7000-9000-000000000001' \
  -d '{
    "recipient_gcid":"01970000-0000-7000-9000-000000000002",
    "channel":"email",
    "template_id":"welcome",
    "payload":{"name":"Test"}
  }'
```

The notification channel must be one of `email`, `push`, or `in_app`. A successful enqueue returns HTTP 201 unless the notification is suppressed by a matching opted-out preference, in which case it returns HTTP 200.

Other HTTP routes:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/notifications` | List notifications. Supports `recipient_gcid`, `channel`, `limit`, `since`/`from`, and `to`. |
| GET | `/api/notifications/{id}` | Fetch one notification. |
| POST | `/api/notifications/mark-read` | Mark or unmark a notification. Body: `{"notification_id":"...","read":true}`. |
| POST | `/api/templates` | Create a template or append a new version for an existing template name. |
| GET | `/api/templates` | List all template versions. |
| GET | `/api/templates/{id}` | Fetch one template. |
| POST | `/api/preferences` | Upsert a subscription preference. |
| GET | `/api/preferences/{gcid}` | List preferences for a GCID. |
| POST | `/api/notifications/push-subscriptions` | Register a push subscription when PostgreSQL is wired. |
| DELETE | `/api/notifications/push-subscriptions` | Delete a push subscription by token. |

The push-subscription routes are registered only when the PostgreSQL-backed push repository is available.

SendGrid's signed event webhook is mounted at:

```text
POST /webhooks/sendgrid/events
```

The webhook is enabled only when the SendGrid webhook public key is configured. It verifies SendGrid's ECDSA signature and replay window before processing terminal `delivered`, `bounce`, `dropped`, `blocked`, and `deferred` events.

### gRPC

The gRPC server listens on `CHORA_GRPC_PORT`, falling back to `GRPC_PORT`, then `:9090`.

The generated `NotificationsService` implements these RPCs:

- `EnqueueNotification`
- `GetNotification`
- `ListNotifications`
- `CreateTemplate`
- `GetTemplate`
- `ListTemplates`
- `UpsertPreference`
- `ListPreferencesByGcid`

The standard gRPC health service is registered on the same listener.

### Configuration

The main environment variables are:

| Variable | Purpose | Local default |
| --- | --- | --- |
| `CHORA_ENV` | Environment name | `local` |
| `PORT` | HTTP listen port | `8080` |
| `CHORA_GRPC_PORT` | gRPC listen port | `9090` |
| `GRPC_PORT` | gRPC port alias | unset |
| `CHORA_DB_DSN` | PostgreSQL DSN | `postgres://chora:chora@localhost:5432/chora_notifications?sslmode=disable` |
| `CHORA_DB_DSN_SECRET_ID` | Secret identifier for the PostgreSQL DSN | unset |
| `CHORA_DB_PROJECT` | Project used for secret resolution | `chora-local` |
| `CHORA_OUTBOX_DSN` | PostgreSQL DSN for the durable outbox and idempotency store | same as `CHORA_DB_DSN` when omitted |
| `CHORA_OUTBOX_WORKER_ID` | Outbox worker identifier | `chora-notifications-local` in `.env.example` |
| `NATS_URL` | NATS JetStream URL | `nats://127.0.0.1:4222` |
| `CHORA_SOURCE_PROJECT` | Source project stamped on event envelopes | `chora-local` in `.env.example` |
| `EMAIL_PROVIDER` | Email provider: `sendgrid`, `stub`, or empty | empty |
| `SENDGRID_API_KEY` | SendGrid API credential | unset |
| `MAIL_FROM` | Default sender address | `noreply@chora.site` in `.env.example` |
| `MAIL_FROM_NAME` | Default sender display name | `Chora` in `.env.example` |
| `SVC_IDENTITY_GRPC_URL` | Identity gRPC endpoint used for GCID-to-email resolution | unset |
| `CHORA_EMAIL_FANOUT_ENABLED` | Enables email-channel fan-out | off unless set to `1`, `true`, `yes`, or `on` |
| `CHORA_PII_CLOSURE_MAP_PATH` | Path to the PII closure map | `config/PII_Closure_Map.yaml` |
| `CHORA_CLOSURE_SUBSCRIPTION` | Closure subscriber name | `chora-notifications.closure-pseudonymise` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC trace endpoint | stdout fallback |

When `NATS_URL` is set, consumers use durable JetStream consumers with five delivery attempts, a 30-second ack window, backoff, and DLQ subjects derived from the subscribed subject. The service publishes under the `chora.notifications.*` namespace and uses binary protobuf encoding for the supported schema-bound notification event streams.

For email, `EMAIL_PROVIDER=sendgrid` enables the SendGrid v3 adapter. Empty or `stub` selects the development stub path and does not send mail. The email pipeline resolves a GCID through the identity gRPC client, renders the embedded templates under `config/email_templates/`, and records delivery results in PostgreSQL when the database is wired.

## Development

The project is a Go module:

```sh
go mod download
go test ./...
```

Run the service from the repository root with:

```sh
go run ./cmd/server
```

Integration tests are build-tagged and use `CHORA_TEST_DSN` for a real PostgreSQL database:

```sh
CHORA_TEST_DSN=postgres://chora:chora@localhost:5432/chora_notifications?sslmode=disable \
  go test -tags integration ./internal/adapter/pg/...
```

The source is organized around a hexagonal boundary:

```text
cmd/server/                 process entrypoint and dependency wiring
internal/domain/            domain aggregates and repository ports
internal/adapter/http/      REST handlers, tenant middleware, SendGrid webhook
internal/adapter/grpc/      NotificationsService gRPC adapter
internal/adapter/pg/        PostgreSQL repositories
internal/adapter/outbox/    transactional outbox and dispatcher
internal/adapter/events/    NATS subscribers and event encoders
internal/adapter/email/     email provider selection and template rendering
internal/adapter/sendgrid/  SendGrid v3 client
internal/adapter/clients/  identity gRPC client
internal/adapter/inmem/    in-memory repositories for development and tests
internal/observability/     OTLP tracing
config/                     closure map and embedded email templates
migrations/                 PostgreSQL schema migrations
```

The Dockerfile builds `./cmd/server` with Go and produces an Alpine-based runtime image. The GitHub Actions workflow publishes multi-architecture images for `linux/amd64` and `linux/arm64` on pushes to `main` and on version tags.
