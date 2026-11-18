# notificationSystem/src — event consumer service

Architecture overview: [root CLAUDE.md](../../CLAUDE.md). Event contract and pipeline diagram: [docs/events/CLAUDE.md](../../docs/events/CLAUDE.md), [events.map.md](../../docs/events/events.map.md). RabbitMQ classes: [infrastructure/rabbitMQ/CLAUDE.md](infrastructure/rabbitMQ/CLAUDE.md). Open issues: [KNOWN_ISSUES.md](../../docs/KNOWN_ISSUES.md).

The real work runs in consumer processes started by `bootstrap.ts`. The HTTP app (`server.ts`/`app.ts`) is WORK IN PROGRESS: the maintainer has deliberately left it unfinished. Today it has no real routes, and it needs Postgres because `app.ts:19` calls `testConnection()`. Its intended role is UNDECIDED — ask the maintainer before adding anything there.

| Folder / file | Status | Purpose |
|---|---|---|
| `bootstrap.ts` | CANONICAL | Picks a consumer from `CONSUMER_TYPE` |
| `handlers/` | CANONICAL | Dispatch by event type, return a `ConsumeOutcome` |
| `services/notificationService.ts` | CANONICAL | Render templates, publish delivery messages |
| `services/idempotencyStoreService.ts` | BROKEN usage | Redis duplicate check (see below) |
| `services/mail/` | not running yet | Gmail provider, error classifier, rate limiter |
| `errors/notificationError.ts` | CANONICAL | Retriable vs non-retriable errors |
| `infrastructure/` | CANONICAL | Event bus, RabbitMQ classes, enums |
| `config/` | CANONICAL | RabbitMQ topology, Redis client, DB config |
| `templates/` | CANONICAL | Handlebars emails |
| `routes/`, `controllers/`, `models/`, `middleware/` | WORK IN PROGRESS | HTTP layer, intentionally unfinished. Mostly template code copied from `backend/` for now. |

## Rules

- Keep handlers thin: switch on `parsedMessage.type`, call a `NotificationService` method, return a `ConsumeOutcome`. Never ack, nack or reject in a handler.
- Use relative imports. This service already does, apart from one type-only `@/` import (`sendEmailService.ts:6`). `ts-node` and the built output don't resolve `@/`.
- Read env vars as `process.env['NAME']`. Add new ones to `.env.example` in the same change.
- Log through `utils/logger.ts`. Don't use `console.*` (`gmailProvider.ts:71,74` and `rateLimiter.ts:60` still do).
- Keep `EventTypes` and `DomainEvent` in step with `backend/`. See [docs/events/CLAUDE.md](../../docs/events/CLAUDE.md).

## ConsumeOutcome

```ts
// from notificationSystem/src/infrastructure/eventTypes.ts
export enum ConsumeOutcome {
  Ack = 'acknowledged',
  RETRY = 'retry',
  REJECT = 'rejected',
  Nack = 'nack',
  PauseConsuming = 'pause-consuming'
}
```

| Outcome | Return it when | Ingest consumer does | Email worker does |
|---|---|---|---|
| `Ack` | The work succeeded, or there's nothing to do | ack | claim idempotency key, ack |
| `RETRY` | A transient failure | requeue once, then `notif_dlq` | next retry queue, or `email_dlq` after the limit |
| `REJECT` | The message can never succeed | `notif_dlq` | `email_dlq` |
| `Nack` | Don't use it: nothing returns it | falls to `default` → `notif_dlq` | falls to `default` → `email_dlq` |
| `PauseConsuming` | The provider is down or the quota is used up (email only) | falls to `default` → `notif_dlq` | cancel the consumer and schedule a resume |

## Errors

```ts
// from notificationSystem/src/errors/notificationError.ts
export abstract class NotificationError extends Error {
  abstract readonly retriable: boolean;
}

export class NonRetriableNotificationError extends NotificationError {
  readonly retriable = false;
}

export class RetriableNotificationError extends NotificationError {
  readonly retriable = true;
}
```

Throw `NonRetriableNotificationError` for bad payloads and missing templates, and `RetriableNotificationError` for transient failures such as a failed publish. The ingest handler treats unknown errors as `RETRY`.

## Idempotency

`IdempotencyStoreService` stores `notif_ingest_event_id:<eventId>` in Redis with `SET NX` and a 1-hour TTL. `claim()` returns `true` only for the first caller. `getEvent()` only checks whether the key exists.

BROKEN, so don't copy the current call sites:
- The consumers read `parsedMsg.payload?.eventId`, but the backend puts `eventId` at the top level of the message (`backend/src/infrastructure/rabbitMQ/rabbitMQEventBus.ts:16`). The value is always `undefined`.
- Ingest calls `getEvent()` but never `claim()`, so it has no duplicate protection.
- Delivery messages carry no `eventId`. Once the email worker runs, its first successful send claims `notif_ingest_event_id:undefined`, and every other email for the next hour is acked without being sent (`emailWorker.ts:43,54`).

Check-then-claim also isn't atomic. Use the `claim()` result as the check.

## Mail

`EmailService.send` applies the rate limit, calls `GmailProvider`, and maps the result to an outcome:

| Provider result | Outcome |
|---|---|
| `SUCCESS` | `Ack`, and increments the daily count |
| `PARTIAL_FAILURE` | `REJECT` (for manual inspection in the DLQ), and increments the daily count |
| `FAILED`, scope `GLOBAL` (auth, connection) | `PauseConsuming` |
| `FAILED`, scope `DAILY_LIMIT_REACHED` (SMTP 454 "Too many recipients") | `PauseConsuming` with payload `-1` |
| `FAILED`, scope `REQUEST`, retryable (other 4xx) | `RETRY` |
| `FAILED`, anything else (`EENVELOPE`, permanent failures) | `REJECT` |

The rate limiter (`LeakyBucketRateLimiter`, Redis Lua script) spaces sends at least 1 second apart across all workers and refuses sends once `email:gmail:daily_count` reaches `MAX_DAILY_LIMIT`.

- BROKEN: the daily count never expires, so the limit becomes permanent once reached (`sendEmailService.ts:43,50`).
- `MAX_DAILY_LIMIT` is required. Without it, `tryAcquire` throws and the worker sends the message to `email_dlq`.
- The comment at `rateLimiter.ts:75` says "fail open", but the code re-throws. Redis being down fails the send.

The author's design notes are in `services/mail/`. `redisRateLimitingWithRabbitMQ.md` is a design proposal: it describes a token bucket and worker setup that differ from the code, so trust the code. `error_handling_mail_errors_with_nodemailer.md` is a reference for nodemailer errors and Gmail limits (500 recipients a day for personal accounts, 2,000 for Workspace).

## Templates

Put templates in `templates/` as `<domain>.<purpose>.template.hbs`, and load them with `NotificationService.fetchEmailTemplate(filename)`, which compiles and caches them. A template that fails to load throws `NonRetriableNotificationError`, so the event goes to the DLQ.

```ts
// from notificationSystem/src/services/notificationService.ts
const replacements = {
  firstName: name || 'User',
  appName: process.env['APP_NAME'] || 'Taskify',
  verificationLink: verificationLink,
  year: new Date().getFullYear()
};
```

- BROKEN: `notificationService.ts:39` asks for `user.account.creation.templates.hbs`. The file is `user.account.creation.template.hbs`.
- BROKEN: `npm run build` runs only `tsc`, so `.hbs` files aren't copied into `dist/`.
- The subject line is still hard-coded as `'Welcome to MyApp – Verify Your Email'`.

## Adding a consumer type

```ts
// from notificationSystem/src/bootstrap.ts
switch (consumerType) {
  case 'notif_ingest':
    logger.info('Starting notification ingest consumer...');
    await eventBus.notificationIngestSub.consume();
    break;
```

1. Build the consumer in `infrastructure/eventBus.ts` and expose it on the `eventBus` object. Email and push are already there as `emailNotificationSub` and `pushNotificationSub`.
2. Add a `case` in `bootstrap.ts` following the block above.
3. Close the new consumer in `setupGracefulShutdown`, which only closes the ingest consumer today.
4. Add `consume:<type>:dev` and `consume:<type>` scripts to `package.json`, following the `consume:notif_ingest` pair.
5. The email worker already has a case (`email_worker`) and scripts (`consume:email_worker:dev` / `consume:email_worker`). It can't start until B2 is fixed, and B7, B10 and B11 in [KNOWN_ISSUES.md](../../docs/KNOWN_ISSUES.md) would make it silently drop emails.

## Environment variables

| Var | Purpose | Default | In `.env.example`? |
|---|---|---|---|
| `CONSUMER_TYPE` | Selects the consumer (set by npm scripts) | none, exits with an error | no |
| `RABBITMQ_EMAIL_RETRY_DELAY_FACTOR_MIN`, `RABBITMQ_PUSH_RETRY_DELAY_FACTOR_MIN` | Retry delays. **The process crashes at startup without them.** | broken fallback | **no** |
| `MAX_DAILY_LIMIT` | Daily email cap, required by the email worker | none | **no** |
| `GMAIL_USER_MAIL_ID`, `GMAIL_CLIENT_ID`, `GMAIL_CLIENT_SECRET`, `GMAIL_REFERSH_TOKEN` (sic) | Gmail OAuth2 | none | **no** |
| `APP_NAME` | Name shown in emails | `Taskify` | no |
| `RABBITMQ_PROTOCOL`, `_HOSTNAME`, `_PORT`, `_USERNAME`, `_PASSWORD`, `_VHOST`, `_RECONNECT_INTERVAL`, `_RECONNECT_ATTEMPTS` | Broker connection | `amqp`, `localhost`, `5672`, `guest`, `guest`, `/`, `5000`, `10` | yes |
| `RABBITMQ_EXCHANGE_NAME`, `RABBITMQ_QUEUE_NAME` | Must match `backend/` | `notif_exchange`, `notif_queue` | yes |
| `RABBITMQ_NOTIF_DLX`, `_DELIVERY_EXCHANGE`, `_EMAIL_DLX`, `_PUSH_DLX`, `_NOTIF_DLQ`, `_EMAIL_DELIVERY_QUEUE`, `_EMAIL_DLQ`, `_EMAIL_RETRY_1..3`, `_PUSH_DELIVERY_QUEUE`, `_PUSH_DLQ`, `_PUSH_RETRY_1..3` | Topology name overrides (each prefixed `RABBITMQ`) | see `config/rabbitMQ.ts` | no |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB` | Redis | `localhost`, `6379`, none, `0` | yes, but `REDIS_PORT=6397` and `REDIS_PASSWORD` has no `=` |
| `PORT`, `NODE_ENV`, `LOG_LEVEL`, `ALLOWED_ORIGINS`, `DB_*` | HTTP scaffold and logging | `8000`, `development`, `info` | yes |

There are no tests for handlers, workers or mail. See [backend/tests/CLAUDE.md](../../backend/tests/CLAUDE.md).
