# Notification system design (current implementation)

Names match the code: topology in `src/config/rabbitMQ.ts`, routing keys in `src/infrastructure/rabbitMQ/routingKeys.ts`. Dashed lines are parts that exist in code but don't run yet. Open bugs are listed in [docs/KNOWN_ISSUES.md](../docs/KNOWN_ISSUES.md).

```mermaid
graph TD
    %% --- The Trigger ---
    subgraph Main_App_Domain [backend/ — Taskify API, port 3000]
        A[🟦 Producers<br/>User afterCreate hook, AuthController<br/>forgotPassword / resend link / invite link]
        A_BUS[🟦 eventBus.notification<br/>NotificationEventBus<br/>adds eventId]
    end

    %% --- RabbitMQ Layer 1: Event Ingestion ---
    subgraph RabbitMQ_Event_Bus [RMQ: Event Infrastructure]
        B{{🟧 notif_exchange<br/>TOPIC Exchange}}
        C[(🟧 notif_queue<br/>binding: #)]
        C_DLX{{🟧 notif_dlx_exchange<br/>TOPIC}}
        C_DLQ[(🟥 notif_dlq)]
    end

    %% --- The CORE LOGIC Layer ---
    subgraph Notification_Service_Logic [notificationSystem/ — process: npm run consume:notif_ingest:dev]
        D[🟦 NotificationConusmer<br/>prefetch 1<br/>**THE BRAIN**]
        D_H[🟦 NotificationIngestHandler<br/>switch on event type]
        D_SVC[🟦 NotificationService]
        D_TPL[Handlebars templates<br/>src/templates/*.template.hbs]
        D_IDEM[(Redis<br/>IdempotencyStoreService)]
    end

    %% --- RabbitMQ Layer 2: Delivery Routing ---
    subgraph RabbitMQ_Delivery_Infrastructure [RMQ: Delivery Infrastructure]
        E{{🟧 delivery_exchange<br/>TOPIC Exchange}}
        F[(🟧 email_delivery_queue)]
        G[(🟧 push_delivery_queue)]
        F_DLX{{🟧 email_dlx_exchange<br/>TOPIC}}
        F_R1[(🟧 email_retry_1<br/>TTL F min)]
        F_R2[(🟧 email_retry_2<br/>TTL F×3 min)]
        F_R3[(🟧 email_retry_3<br/>TTL F×12 min)]
        F_DLQ[(🟥 email_dlq)]
        G_RQ[(🟧 push_retry_1..3, push_dlq<br/>via push_dlx_exchange)]
    end

    %% --- The EXECUTION Layer ---
    subgraph Delivery_Workers [Channel Workers — separate processes]
        H[🟦 EmailConusmer<br/>npm run consume:email_worker:dev<br/>prefetch 1]
        H_RL[(Redis<br/>LeakyBucketRateLimiter<br/>1 send/s, MAX_DAILY_LIMIT)]
        I[🟦 PushNotifConusmer<br/>no bootstrap case yet]
    end

    %% --- External World ---
    subgraph External_APIs [3rd Party Providers]
        J(☁️ Gmail SMTP<br/>nodemailer + OAuth2)
        K(☁️ Push provider<br/>not chosen yet)
    end

    %% --- FLOW CONNECTIONS ---

    %% 1. Backend publishes event
    A --> A_BUS
    A_BUS --1. Publish DomainEvent<br/>route: user.created, user.forgotPassword, ...--> B

    %% 2. Exchange routes every event to the ingest queue
    B -->|"# matches all"| C

    %% 3. THE BRAIN consumes the event
    C --2. Consume Event--> D
    D <-->|duplicate check| D_IDEM
    D --> D_H
    D_H --> D_SVC
    D_SVC <-->|Build Content| D_TPL

    %% Ingest failures
    D --REJECT, or RETRY after 1 requeue<br/>route: notif.ingest.dql--> C_DLX
    C_DLX --> C_DLQ

    %% 4. THE BRAIN publishes delivery commands
    D_SVC --3. Publish CMD: Send Email<br/>route: route.email--> E
    D_SVC -.->|"3. Publish CMD: Send Push<br/>route: route.push"| E

    %% 5. Delivery exchange routes to channel queues
    E --route.email--> F
    E --route.push--> G

    %% 6. Delivery workers pick up jobs
    F --4. Consume CMD--> H
    G -.->|"4. Consume CMD"| I
    H <-->|acquire send slot| H_RL

    %% 7. Workers hit external APIs
    H --5. Send mail--> J
    I -.->|"5. API Call"| K

    %% Email retries: TTL expiry dead-letters back to delivery_exchange
    H --RETRY / REJECT<br/>x-retry-count header--> F_DLX
    F_DLX --email.worker.retry1--> F_R1
    F_DLX --email.worker.retry2--> F_R2
    F_DLX --email.worker.retry3--> F_R3
    F_DLX --email.worker.dql<br/>after maxRetryCount 4, or REJECT--> F_DLQ
    F_R1 & F_R2 & F_R3 --TTL expires<br/>dead-letter, route.email--> E
    I -.-> G_RQ

    %% Styling
    style A fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style A_BUS fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style B fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style C fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style C_DLX fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style C_DLQ fill:#ffcdd2,stroke:#b71c1c,stroke-width:2px
    style D fill:#e1f5fe,stroke:#01579b,stroke-width:4px
    style D_H fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style D_SVC fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style E fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style F fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style G fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style F_DLX fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style F_R1 fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style F_R2 fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style F_R3 fill:#ffe0b2,stroke:#e65100,stroke-width:2px
    style F_DLQ fill:#ffcdd2,stroke:#b71c1c,stroke-width:2px
    style G_RQ fill:#eeeeee,stroke:#9e9e9e,stroke-dasharray:5 5
    style H fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style I fill:#eeeeee,stroke:#9e9e9e,stroke-dasharray:5 5
    style K fill:#eeeeee,stroke:#9e9e9e,stroke-dasharray:5 5
    style D_TPL fill:#cfd8dc,stroke:none
    style D_IDEM fill:#cfd8dc,stroke:none
    style H_RL fill:#cfd8dc,stroke:none
```

## Changes from the original design

| Original design | Current implementation |
|---|---|
| `ex.app.events` | `notif_exchange` (topic) |
| `q.notifications.ingest` | `notif_queue`, bound with `#` |
| `ex.notifications.delivery`, **DIRECT** | `delivery_exchange`, **TOPIC** |
| `q.delivery.email` / `q.delivery.push` | `email_delivery_queue` / `push_delivery_queue` |
| Routing keys `email` / `push` | `route.email` / `route.push` |
| Ingest worker checks a **User Prefs DB** | No preference check. The worker uses Redis only for duplicate detection. |
| SendGrid / SES | Gmail through nodemailer (OAuth2), with a Redis rate limiter |
| Firebase FCM push worker | `PushNotifConusmer` exists but its handler is commented out and nothing starts it |
| No failure paths | `notif_dlq`; email retry queues `email_retry_1..3` with TTLs, then `email_dlq` |

## Status of each part

- **Ingest** (`npm run consume:notif_ingest:dev`): runs. It needs `RABBITMQ_EMAIL_RETRY_DELAY_FACTOR_MIN` and `RABBITMQ_PUSH_RETRY_DELAY_FACTOR_MIN` set, and the `user.created` template filename is misspelled, so those events currently go to `notif_dlq`.
- **Email worker** (`npm run consume:email_worker:dev`): fails at startup because the retry-queue TTLs are absolute timestamps (B2). After that is fixed, the idempotency bug (B7) would drop emails.
- **Push worker**: not started anywhere. Push messages would sit in `push_delivery_queue`.
- `F` is `RABBITMQ_EMAIL_RETRY_DELAY_FACTOR_MIN` in minutes. Push retry TTLs use `RABBITMQ_PUSH_RETRY_DELAY_FACTOR_MIN` ×1, ×2, ×3.

The fuller cross-service diagram is in [docs/events/events.map.md](../docs/events/events.map.md).
