# serving-api  ·  SERVING LAYER

**Layer:** serving · **Owner:** Dev B · **Status:** 🔲 Part 2 — skeleton only

FastAPI **read model** over Postgres. Never computes — it reads `rt_zone_metrics`
(speed) and `daily_household_billing` / `daily_zone_solar_summary` (batch) and
merges them into one coherent view.

> **Build plan:** [`docs/PART2_ONBOARDING.md`](../../docs/PART2_ONBOARDING.md) §B4.

## Endpoints

| Method | Path | Returns |
|---|---|---|
| GET | `/api/realtime/zones` | latest per-zone metrics (all zones) — **the brief's real-time endpoint** |
| GET | `/api/realtime/zones/{zone}` | one zone, recent window history |
| GET | `/api/billing/daily/{date}` | all household bills for a finalised day |
| GET | `/api/billing/household/{household_id}?date=` | one household's bill(s) |
| GET | `/api/reports/solar/{date}` | per-zone solar contribution report |
| GET | `/healthz` | Postgres up + `rt_zone_metrics` freshness → 200 / 503 |
| GET | `/metrics` | Prometheus |

## Merge rule

- A day with a `batch_runs.status='success'` row → return **batch** numbers, `"status": "final"`.
- Otherwise (today, or not-yet-reconciled) → return **speed-layer** numbers, `"status": "provisional"`.

## What to build

`config.py` (done) · `adapters/repositories.py` (extend `energy_common.storage.BaseRepository`) ·
`api/routes_*.py` · `api/models.py` (response DTOs mapping from `energy_common` domain models) ·
`app.py` (FastAPI factory + Prometheus middleware) · `__main__.py` (uvicorn).

## Definition of Done

`/docs` loads; every endpoint returns real data after `make demo`; `/healthz` flips to 503 when
the stream stalls; `http_request_duration_seconds` exported.
