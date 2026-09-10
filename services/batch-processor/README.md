# batch-processor  ·  BATCH LAYER

**Layer:** processing (Lambda batch layer) · **Owner:** Dev B · **Status:** 🔲 Part 2 — skeleton only

Spark **batch** job. Recomputes the authoritative daily bill for one closed
business day from the immutable Parquet master dataset + that day's tariff/weather
feed. Invoked by the Airflow DAG (`daily_energy_reconciliation`) or directly:

```bash
python -m batch_processor --business-date 2026-01-01        # [--allow-open]
```

> **Build plan:** [`docs/PART2_ONBOARDING.md`](../../docs/PART2_ONBOARDING.md) §B2.

## Contract

| Direction | Resource | Spec |
|---|---|---|
| in | Parquet `readings_raw/business_date=D/` (require `_SUCCESS`) | DATA_CONTRACTS §2 |
| in | `tariff_reference_D.csv`, `weather_forecast_D.csv` from the landing zone | DATA_CONTRACTS §3 |
| in | Postgres `dim_household`, `dim_zone` | DATA_CONTRACTS §4 |
| out | Postgres `daily_household_billing`, `daily_zone_solar_summary` (idempotent upsert; a re-run fully replaces day D) | DATA_CONTRACTS §4 |
| out | `reports/out/billing_report_D.{csv,html}` | — |

## What to build

1. `domain/billing.py` — `BillingStrategy` Strategy pattern: `FlatBilling`,
   `TieredBilling`, `SubsidisedBilling`; each `.charge(EnergyBalance, tariff) -> Charge`.
   **Reuse `energy_common.domain.transforms.energy_balance`** for the solar split.
2. `domain/quality.py` — DQ assertions (every active household billed once; renewable %
   within the weather-implied band; no unexplained negative `net_amount`).
3. `application/reconcile.py` — read partition → per-household rollup → join feed →
   apply strategy → write tables → render report.
4. `adapters/` — Spark session factory, `BillingRepository`, report renderer.
5. `__main__.py` argparse (`--business-date`, `--allow-open`).

## Definition of Done

- Re-running for the same date is byte-idempotent in the tables and reproduces identical numbers.
- DQ assertions pass on a clean day and fail loudly on a seeded-bad day.
- `batch_job_rows{table}` and `batch_job_duration_seconds{business_date}` exported.
