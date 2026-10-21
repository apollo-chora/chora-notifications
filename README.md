# chora-notifications

> The notification service: templates, subscriber preferences, per-channel delivery, the
> SendGrid event webhook, and the fan-out subscribers that turn platform events into
> notifications.

Go, HTTP/JSON plus gRPC. PostgreSQL for durable state, NATS JetStream for fan-out and the
outbox.

## What it is

Anything the platform tells a person goes through here. The service owns:

- **Notifications** — create, list, fetch and mark-read, per recipient.
- **Templates** — versioned template records; creating an existing name appends a version.
- **Preferences** — per-GCID subscription preferences, which are what suppress a message.
- **Delivery** — per-channel adapters for `email`, `push` and `in_app`.
- **Fan-out** — subscribers that translate platform events (graded submissions, new
  followers, and three more) into notifications.
- **Push subscriptions** — device tokens registered for push.
- **The SendGrid webhook** — signed delivery events from the email provider.

It serves HTTP on `PORT` (default `8080`) and gRPC on `CHORA_GRPC_PORT` (falling back to
`GRPC_PORT`, then `:9090`). In the stack both are `expose`d and neither is published.

### Only the in-app channel is live in this stack

The delivery adapters are wired but three of the four paths are inert, and the boot log
says so explicitly:

```text
notifications: EMAIL_PROVIDER="" — email channel in STUB mode (no real send)
notifications: email-channel fan-out disabled (CHORA_EMAIL_FANOUT_ENABLED unset) — in_app only
notifications: web push disabled (no cloud-neutral push sender wired)
notifications: SendGrid webhook DISABLED (SENDGRID_WEBHOOK_PUBLIC_KEY[_SECRET_ID] unset)
```

| Path | State in the stack | Why |
| --- | --- | --- |
| `in_app` | **works** | no external dependency |
| `email` | **stub — nothing is sent** | `EMAIL_PROVIDER` is empty, so the stub adapter is selected |
| email fan-out | **off** | `CHORA_EMAIL_FANOUT_ENABLED` is unset, so events produce in-app only |
| `push` | **disabled** | no push sender is wired; there is no cloud-neutral implementation |
| SendGrid webhook | **disabled** | the webhook public key is unset, so the route is not mounted |

This is the single most important thing to know before testing a notification: a `201` from
`POST /api/notifications` with `"channel":"email"` means *enqueued*, not delivered.

Note also the subtlety in the log: the **email-send subscriber is running** (it binds
`chora.notifications.email.queued.v1`) while the provider behind it is a stub. A bound
subscriber is not a working mail path.

## Prerequisites

| Route | What you need |
| --- | --- |
| In the stack | A working `chora-stack` deployment. PostgreSQL, NATS and identity come from it. |
| Standalone | Go **1.26.1 or newer**, PostgreSQL with this repository's schema, and NATS JetStream for fan-out. |
| Real email | A SendGrid account, `EMAIL_PROVIDER=sendgrid`, `SENDGRID_API_KEY`, and — for delivery events — the webhook public key. |
| Real push | A push sender implementation; none ships in this repository. |

## Credentials

By default this service holds **no provider credentials at all** — the stack runs it in
stub mode. That changes only when you switch email on.

| Value | Where it comes from | Variable |
| --- | --- | --- |
| PostgreSQL password | the stack's `POSTGRES_PASSWORD`, carried into both DSNs as `chora_notifications_app_rw` | `CHORA_DB_DSN`, `CHORA_OUTBOX_DSN` |
| NATS endpoint | the stack's NATS service; no credential locally | `NATS_URL` |
| SendGrid API key | **only if you want real email** — from your SendGrid account | `SENDGRID_API_KEY` (direct dev override) or `SENDGRID_API_KEY_SECRET_ID` (production, resolved through the secrets backend under `CHORA_DB_PROJECT`) |
| SendGrid webhook public key | **only if you want delivery events** — from the SendGrid webhook settings | `SENDGRID_WEBHOOK_PUBLIC_KEY` (or `…_SECRET_ID`) |
| Identity gRPC | the stack's identity service, for GCID → email lookup | `SVC_IDENTITY_GRPC_URL` |

Switching email on takes three changes, not one: `EMAIL_PROVIDER=sendgrid`, a valid
`SENDGRID_API_KEY`, and — if you want the delivered/bounce events to come back —
`SENDGRID_WEBHOOK_PUBLIC_KEY`. Without the third the webhook route is never mounted, so
delivery status stays unknown even though mail is sent.

There are **no model-provider credentials** here. Where the stack's shared values come
from, and how to obtain the provider keys that *other* services need, is documented once in
[`chora-stack/README.md`](../chora-stack/README.md#credentials).

## Run it in the stack

`compose.yml` does four things:

1. Builds the image from the sibling checkout at `../chora-notifications`.
2. Runs `notifications-migrate` first — a `postgres:18.6-alpine` one-shot applying this
   repository's migrations to `chora_notifications`.
3. Starts the service once `postgres` and `nats` are healthy, `nats-init` has completed,
   and the migrate one-shot completed successfully.
4. Publishes no host port; it `expose`s `8080` and `9090`.

```sh
cd chora-stack
docker compose up -d notifications
docker compose ps notifications
```

What Compose sets: `PORT`, `CHORA_GRPC_PORT`, the two DSNs, `NATS_URL`,
`CHORA_SOURCE_PROJECT`, `SVC_IDENTITY_GRPC_URL` and `OTEL_EXPORTER_OTLP_ENDPOINT`. It sets
**nothing** for email, push or the webhook — which is why the stack is in-app only.

Probe it from inside the network:

```sh
docker compose exec -T nats wget -qO- http://notifications:8080/readyz
```

## Run it standalone

```sh
git clone https://github.com/apollo-chora/chora-notifications.git
cd chora-notifications
go build ./cmd/server
./server
```

`.env.example` carries local defaults for the DSN, NATS and mail identity. The binary does
not read `.env`; export it:

```sh
set -a; . ./.env; set +a
```

With the database unset the push-subscription routes are not registered, because the
PostgreSQL-backed push repository is what mounts them.

Tests and container:

```sh
go test ./...
docker build -t chora-notifications .
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | HTTP listen port |
| `CHORA_GRPC_PORT` | `9090` | gRPC listen port; `GRPC_PORT` is an alias |
| `CHORA_DB_DSN` | local DSN | PostgreSQL DSN |
| `CHORA_DB_DSN_SECRET_ID` | unset | secret-resolved DSN |
| `CHORA_DB_PROJECT` | `chora-local` | project for secret resolution |
| `CHORA_OUTBOX_DSN` | falls back to `CHORA_DB_DSN` | outbox and idempotency store |
| `CHORA_OUTBOX_WORKER_ID` | `chora-notifications-local` in `.env.example` | outbox worker identity |
| `NATS_URL` | local URL | NATS JetStream |
| `CHORA_SOURCE_PROJECT` | `chora-local` | event envelope source project |
| `EMAIL_PROVIDER` | empty → **stub** | `sendgrid`, `stub`, or empty. An unknown value is a fail-loud config error |
| `SENDGRID_API_KEY` | unset | SendGrid credential; required for real sends |
| `SENDGRID_WEBHOOK_PUBLIC_KEY` / `…_SECRET_ID` | unset | enables the webhook route and signature verification |
| `MAIL_FROM`, `MAIL_FROM_NAME` | `noreply@chora.site`, `Chora` | sender identity |
| `CHORA_EMAIL_FANOUT_ENABLED` | off | must be `1`/`true`/`yes`/`on` to fan events out to email |
| `SVC_IDENTITY_GRPC_URL` | unset | GCID → email resolution |
| `CHORA_PII_CLOSURE_MAP_PATH` | `config/PII_Closure_Map.yaml` | map for the closure subscriber |
| `CHORA_CLOSURE_SUBSCRIPTION` | `chora-notifications.closure-pseudonymise` | closure consumer name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | stdout | OTLP/gRPC trace endpoint |

`EMAIL_PROVIDER` is worth reading twice: empty selects the **stub**, which accepts work and
sends nothing, while an **unrecognised** value is a startup error. So a typo fails loudly
and an omission fails silently — the opposite of what you might expect.

## Verify it works

### It is up

```sh
docker compose exec -T nats wget -qO- http://notifications:8080/healthz
docker compose exec -T nats wget -qO- http://notifications:8080/readyz
docker compose exec -T nats wget -qO- http://notifications:8080/
```

```json
{"status":"ok","time":"..."}
{"status":"ready"}
```

`readyz` is bare; it does not tell you which channels are live.

### Which channels are live

```sh
docker compose logs notifications | grep -E 'EMAIL_PROVIDER|fan-out disabled|web push|SendGrid webhook'
```

Expect, in this stack:

```text
notifications: EMAIL_PROVIDER="" — email channel in STUB mode (no real send)
notifications: email-channel fan-out disabled (CHORA_EMAIL_FANOUT_ENABLED unset) — in_app only
notifications: web push disabled (no cloud-neutral push sender wired)
notifications: SendGrid webhook DISABLED (SENDGRID_WEBHOOK_PUBLIC_KEY[_SECRET_ID] unset)
```

This is the fastest way to know what a green health check actually covers.

### Its migrations landed

```sh
docker compose exec -T postgres psql -U chora_notifications_migrate -d chora_notifications \
  -c "select count(*) from schema_migrations;"
```

Expect a count at or just under the 14 forward files listed under
[Migrations](#migrations).

### Creating a notification does not work in this build

This belongs here as a finding rather than a runbook step, because the documented flow
cannot succeed:

```text
1. POST /api/templates  → returns a template record with an id
2. select count(*) from templates;  → 0
3. POST /api/notifications with that id
   → 500  insert or update on table "notifications" violates foreign key
          constraint "notifications_template_id_fkey" (SQLSTATE 23503)
```

The cause is in the wiring: `cmd/server/main.go` builds
`inmem.NewTemplateRepository()` unconditionally, so templates live in process memory,
while `notifications.template_id` carries a real foreign key to
`templates(template_id)` in PostgreSQL. A template created through the API is invisible to
that constraint.

Three failure shapes, all verified:

| Attempt | Result |
| --- | --- |
| `template_id` omitted | `400 NOTIFICATIONS_INVALID` — the field is required |
| `template_id` a name such as `"welcome"` | `500 … invalid input syntax for type uuid … (22P02)` — it must be a UUID |
| `template_id` a UUID from `POST /api/templates` | `500 … violates foreign key … (23503)` — the row is not in the table |

What does work in this stack: templates, preferences, the fan-out subscribers and the
in-app read path (`GET /api/notifications`). The enqueue path does not, so treat a `201`
from `POST /api/notifications` as something to verify rather than assume.

The **suppression** path is affected too. The handler calls `Save` *before* returning the
`200` that signals suppression, so a suppressed notification hits the same foreign key —
which means the `200`-instead-of-`201` behaviour documented under
[API surface](#api-surface) is not reachable in this stack either.

### The fan-out subscribers are bound

```sh
docker compose logs notifications | grep 'fan-out subscribers started'
```

The line names how many topics were bound — five in this stack.

## Troubleshooting

- **An email notification is created but no mail arrives** — expected. `EMAIL_PROVIDER` is
  empty, so the stub adapter is selected and nothing is sent. Set `EMAIL_PROVIDER=sendgrid`
  and a `SENDGRID_API_KEY` to change that.
- **Events produce in-app notifications but never email** — `CHORA_EMAIL_FANOUT_ENABLED` is
  unset. It requires an explicit truthy value.
- **`POST /api/notifications` returns `500` with SQLSTATE `23503`** — the template id is not
  in the `templates` table. Templates are stored in process memory in this build, so no
  template created through the API can satisfy the foreign key; see
  [Creating a notification does not work in this build](#creating-a-notification-does-not-work-in-this-build).
- **`POST /api/notifications` returns `500` with SQLSTATE `22P02`** — `template_id` is a
  name rather than a UUID.
- **`POST /api/notifications` returns `200` instead of `201`** — the recipient has an
  opted-out preference matching that channel. Deliberate suppression, not a failure; check
  `GET /api/preferences/{gcid}`.
- **The SendGrid webhook `404`s** — the route is only mounted when
  `SENDGRID_WEBHOOK_PUBLIC_KEY` (or its `_SECRET_ID`) is set.
- **Push subscriptions `404`** — the push repository is PostgreSQL-backed, so the routes
  only exist when the database is wired.
- **No push notification arrives** — there is no cloud-neutral push sender in this
  repository; the log says `web push disabled`. The subscription records are durable, the
  delivery path is not implemented.
- **A `400` on create** — `channel` must be one of `email`, `push`, `in_app`, and the
  request needs `X-Tenant-Id` plus either `gcid` or `X-Chora-GCID`.
- **A consumer keeps redelivering** — JetStream consumers here are durable with five
  delivery attempts, a 30-second ack window, backoff, and DLQ subjects derived from the
  subscribed subject.
- **Traces never reach Tempo** — every service in this stack logs
  `invalid target address http://otel-collector:4317 ... too many colons in address`.
  Observed in the running stack; only tracing is affected.

## API surface

Health and the root endpoint need no tenant headers. Protected `/api/*` requests require
`X-Tenant-Id` **and** either `gcid` or `X-Chora-GCID`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz`, `/healthz/`, `/health`, `/readyz`, `/` | public |
| `POST` | `/api/notifications` | enqueue; `201` created, `200` suppressed |
| `GET` | `/api/notifications` | list; filters `recipient_gcid`, `channel`, `limit`, `since`/`from`, `to` |
| `GET` | `/api/notifications/{id}` | fetch one |
| `POST` | `/api/notifications/mark-read` | `{"notification_id":"…","read":true}` |
| `POST` | `/api/templates` | create, or append a version for an existing name |
| `GET` | `/api/templates`, `/api/templates/{id}` | list versions, fetch one |
| `POST` | `/api/preferences` | upsert a subscription preference |
| `GET` | `/api/preferences/{gcid}` | list a GCID's preferences |
| `POST` | `/api/notifications/push-subscriptions` | register a push token (needs PostgreSQL) |
| `DELETE` | `/api/notifications/push-subscriptions` | delete by token |
| `POST` | `/webhooks/sendgrid/events` | SendGrid delivery events; only when the webhook key is set |

The webhook verifies SendGrid's ECDSA signature and replay window before processing
terminal `delivered`, `bounce`, `dropped`, `blocked` and `deferred` events.

gRPC, on `CHORA_GRPC_PORT` → `GRPC_PORT` → `:9090`, from the generated
`NotificationsService`: `EnqueueNotification`, `GetNotification`, `ListNotifications`,
`CreateTemplate`, `GetTemplate`, `ListTemplates`, `UpsertPreference`,
`ListPreferencesByGcid`, plus the standard `grpc.health.v1.Health`.

## Architecture

```text
cmd/server/                 entrypoint, email/push/closure bootstrap
internal/domain/            notifications, templates, preferences, delivery
internal/adapter/http/      REST handlers and the SendGrid webhook
internal/adapter/grpc/      NotificationsService adapter
internal/adapter/pg/        PostgreSQL repositories and the outbox
internal/adapter/events/    fan-out subscribers and the closure saga
config/                     PII closure map and the embedded email templates
migrations/                 PostgreSQL schema
```

Two structural details matter operationally. First, channel adapters are selected at boot
from environment configuration, so "which channels work" is a property of the deployment
rather than of the code. Second, the fan-out subscribers are separate from the delivery
adapters: a subscriber can be bound and producing in-app notifications while the email
adapter behind it is a stub.

## Events

The service is a **consumer** first: fan-out subscribers listen to platform events and turn
them into notifications. In this stack they bind exactly five topics:

```text
chora.delivery.certification.issued.v1
chora.delivery.submission.graded.v1
chora.sharing.relationship.followed.v1
chora.consumption.companion.stirring.v1
chora.consumption.familiar.stirring.v1    (legacy alias, still bound)
```

| Concern | Setting |
| --- | --- |
| Consumers | durable JetStream, five delivery attempts, 30-second ack window, backoff |
| Dead letters | DLQ subjects derived from the subscribed subject |
| Publish namespace | `chora.notifications.*`, binary protobuf for schema-bound streams |
| Outbox | PostgreSQL, `CHORA_OUTBOX_DSN` falling back to `CHORA_DB_DSN` |
| Closure | `CHORA_CLOSURE_SUBSCRIPTION`, default `chora-notifications.closure-pseudonymise` |

`CHORA_EMAIL_FANOUT_ENABLED` is the switch that decides whether a fan-out event also queues
an email. Off — the stack's state — means in-app only.

## Migrations

**14 forward migration files**, applied by the stack's `scripts/migrate.sh` in the
`notifications-migrate` one-shot against `chora_notifications`. The runner is not part of
this repository.

The set runs from `0001_initial.sql`, `0002_outbox.sql` and
`0003_subscriber_preferences.sql` through `0014_uuidv7_pk_defaults.up.sql` and
`9999_grant_app_roles.sql`, covering notifications, the outbox, subscriber preferences,
trigger rules, delivery logs, idempotency keys, push subscriptions (including the FCM
columns), and closure pseudonymisation state.

The numbering skips `0011`; that is the repository's history, not a gap to fill.

Two carry requirements worth knowing:

- **`0014_uuidv7_pk_defaults`** switches primary-key defaults to PostgreSQL 18's native
  `uuidv7()`. An older server fails the migrate one-shot.
- **`9999_grant_app_roles.sql`** carries the `_app_rw` grants; without it the service
  connects and every query fails on permissions.

The `.down.sql` siblings are destructive rollbacks and are never applied: the runner takes
every `*.sql` except `*.down.sql`, records a SHA-256 checksum per file in
`schema_migrations`, and refuses a file whose contents changed after application.
