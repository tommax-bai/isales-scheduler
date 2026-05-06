# isales-scheduler

Lead-dispatch scheduler for the iSales platform (stage 3A).

Polls active campaigns each tick, picks leads whose `next_call_at` is due,
respects time-windows + holidays + global concurrency, asks `telephony-api`
to pick a device, packs history + prompt-version snapshot, and pushes a
`DialRequest` onto `engine:dial` for the engine (stage 4) to consume.

## Architecture

- **Active campaigns**: persisted in Redis SET `scheduler:active-campaigns`,
  cached in-process. Updated by consuming `scheduler:campaign-control`
  (CampaignControl messages from `isales-api`).
- **Tick loop**: every `ISALES_SCHEDULER_TICK_INTERVAL` seconds, walks each
  active campaign and dispatches up to `ISALES_SCHEDULER_BATCH_SIZE` leads.
- **Concurrency**: Redis counter `isales:concurrency:active`; capped at
  `ISALES_MAX_CONCURRENCY`. Atomic Lua INCR-with-cap and floored DECR.
- **Lead state writes**: scheduler ONLY writes `lead.status='calling'` on
  successful dispatch, and `lead.next_call_at` on out-of-window deferral.
  Worker (stage 3B) writes everything else (retry/follow-up next_call_at,
  terminal states).

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `ISALES_DATABASE_URL` | (required) | postgres async URL (`postgresql+asyncpg://…`) |
| `ISALES_REDIS_URL` | (required) | redis URL (`redis://localhost:6379/0`) |
| `ISALES_TELEPHONY_API_BASE` | `http://127.0.0.1:8001` | telephony-api base URL |
| `ISALES_SCHEDULER_TICK_INTERVAL` | `60` | tick interval in seconds |
| `ISALES_SCHEDULER_BATCH_SIZE` | `50` | max leads per campaign per tick |
| `ISALES_SCHEDULER_HISTORY_N` | `3` | last-N call summaries packed into DialRequest |
| `ISALES_MAX_CONCURRENCY` | `8` | global concurrent-call cap |
| `ISALES_SELECT_TIMEOUT_SECONDS` | `1.0` | timeout for `/devices/select` |
| `TZ` | (deployment) | server timezone (e.g. `Asia/Shanghai`); time-windows interpret `09:00` etc. in this zone |

DB migrations run from isales-common's alembic — scheduler does not own
migrations.

## Run

    pip install -e '.[dev]'
    isales-scheduler

## Dev verification (3-terminal flow)

isales-engine isn't built yet (stage 4). Use the bundled mock consumer
to validate the full path independently:

    # terminal 1: isales-api (assumed already running on :8000)

    # terminal 2: scheduler
    isales-scheduler

    # terminal 3: mock engine consumer (drains engine:dial, simulates
    # call completion, decrements concurrency, resets lead for the next tick)
    python -m scripts.fake_engine_consumer \
      --redis-url redis://localhost:6379/0 \
      --db-url postgresql+asyncpg://… \
      --rate-hz 1 \
      --ack-mode simulate-call-end

Trigger a campaign via `POST /campaigns/{id}/start` on isales-api. Watch
DialRequest messages flow; `GET isales:concurrency:active` should stay
bounded.

## 1-hour soak test

    # in terminal 3, run for 3600s with simulate-call-end mode and
    # sample the concurrency counter every minute:
    python -m scripts.fake_engine_consumer \
      --rate-hz 2 --ack-mode simulate-call-end --max-messages 0
    while true; do
      redis-cli get isales:concurrency:active
      sleep 60
    done

Acceptance: counter stays in `[0, MAX_CONCURRENCY]`, all DialRequest
messages decode without `ValidationError`.

## Tests

    pytest

Integration tests use testcontainers for Postgres + Redis. The test for
`/devices/select` mocks the telephony-api with `httpx.MockTransport`.
