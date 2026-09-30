# Demo Recording Plan

Target: a **8–9 minute** final video (brief allows 5–10). Structured so you can
either record it as tight single take, or record longer and cut the dead time
(image pulls, waiting for a simulated day to close) in editing — recommended,
since watching Docker pull layers is not interesting to a grader.

## Before you hit record

1. **Pre-build everything once, off-camera.** Run `bash scripts/prefetch_pyspark.sh`
   first (a resumable, isolated download of the ~320MB PySpark dependency —
   doing it before `--build` avoids it competing for bandwidth with the other
   6 images installing at the same time, which has timed out on a slower
   connection). Then `docker compose ... up -d --build` downloads
   Kafka/Airflow/Grafana/Postgres/Prometheus and builds 7 custom images (3 of
   which install a JDK + PySpark) — a few minutes with the prefetch done,
   10–20 minutes without it, depending on your connection. Do this beforehand so the recorded run starts
   from warm image caches and comes up in well under a minute.
2. **Pick a short simulated day for the recording.** Set in `.env`:
   ```
   SIM_DAY_SECONDS=120       # 2-minute simulated day -- fits one full day-close in the video
   SIM_HOUSEHOLD_COUNT=200
   ```
   (Leave this at the default 600 for your own longer testing; 120 is just for
   a watchable recording. State the value you used in the report §1.3/Appendix C.)
3. **Reset to a clean start right before recording:**
   ```bash
   make down            # or `make clean` if you want volumes wiped too (slower next start)
   ```
4. **Have these open in browser tabs, ready to switch to:** `localhost:8000/docs`,
   `localhost:8080` (Airflow, logged in already), `localhost:3000` (Grafana,
   logged in already), `localhost:9090/targets`. Logging in on camera wastes time.
5. **Pick one terminal, large font, dark theme** — split or tabbed panes for:
   compose logs, a scratch shell for `curl`/`psql`, and nothing else visible.

## Exact click targets, tab by tab

Reference this while recording so you're never hunting for a button on camera.

**Serving API — `localhost:8000/docs` (Swagger UI)**
Click an endpoint row to expand it → **"Try it out"** (top right of the
expanded panel) → fill any parameter fields (e.g. `date` = a real closed
business date, not `2026-01-01`) → **"Execute"** (blue button) → the JSON
response appears under **Responses** below. Endpoints to hit, in order:
`GET /api/realtime/zones`, `GET /api/billing/daily/{date}`.

**Airflow — `localhost:8080`**
The DAGs list row for `daily_energy_reconciliation` has, left to right: an
on/off toggle (leave it blue/on), the **DAG name link**, tag chips, Owner,
Runs, Schedule (`None`), Last Run, Next Run, a strip of small task-status
circles, then Actions icons (▶ / refresh / red trash), then Links.
**Do not click the ▶ play icon** — it triggers against *today's real date*
and fails (confirmed this week); always trigger from the terminal instead.
Click the **DAG name link** to open the detail page, then the **"Graph"**
tab (next to Details/Gantt/Code/...). After triggering from the terminal,
the 6 task boxes go white → yellow (running) → green (success) over
~30–45 seconds — refresh the page (top-right circular icon) if it doesn't
auto-update.

**Grafana — `localhost:3000` (Smart Grid Overview dashboard)**
Four panels, no clicks needed, just switch to this tab and let it breathe:
top full-width **"Grid Load & Solar Generation by Zone (1-min windows)"**
(the auto-refreshing line chart — your best "this is live" shot), bottom-left
**"Current Renewable Contribution by Zone"** (table), bottom-middle
**"Open Alerts"** (a single number — this is what changes during the alert
segment), bottom-right **"Latest Reconciled Daily Billing"** (table — fills
in once the DAG run finishes).

**Prometheus — `localhost:9090/targets`**
Just the Targets table — point at the **State** column, every row should
read `UP` in green. No clicks needed.

**Terminal**
One pane running `docker compose ... logs -f meter-simulator ingestion`
(segment 2:00–2:45); one pane free for typed commands (`ls data/lake/...`,
the `airflow dags trigger` command, `psql` queries, `make test`).

## Timestamped script

| Time | Segment | What to show / say |
|---|---|---|
| **0:00–0:40** | **Intro** | Say the use case and business question out loud (screen: `README.md` top, or a slide): *"Smart Grid Energy Monitoring & Billing — live grid load and renewable mix per zone right now, plus an authoritative daily bill per household once the tariff feed lands."* |
| **0:40–1:30** | **Architecture in one breath** | Screen: `docs/ARCHITECTURE.md`'s context diagram or the README's ASCII diagram. State: *"Lambda architecture — a speed layer for the live view, a batch layer for the audited bill, because those are genuinely different correctness requirements, not the same computation twice."* One sentence on why not Kappa (reprocessing cost + the daily bill needs a bounded, auditable recompute). |
| **1:30–2:00** | **Start the stack** | Terminal: `make up-full` (already-cached images → comes up in ~30–60s), then `make topics && make seed`. Narrate over it rather than watching it silently. |
| **2:00–2:45** | **Data is flowing (ingestion)** | `docker compose ... logs -f meter-simulator ingestion` for a few seconds — point at a `reading.published` / `partition.flushed` line. Switch to `localhost:9090/targets` — every target `UP`. |
| **2:45–3:45** | **Speed layer, live** | Open `localhost:8000/docs`, execute `GET /api/realtime/zones` — point out `renewable_pct`, `active_meter_count`, and that the numbers are changing if you re-run it 10 seconds later. Or: Grafana dashboard's "Grid Load & Solar by Zone" panel, auto-refreshing. |
| **3:45–4:15** | **The immutable lake** | Terminal: `ls -R data/lake/readings_raw/` — point out the `business_date=...` partitions and that files only accumulate, never get rewritten. One sentence: *"this is what the batch layer reprocesses from."* |
| **4:15–5:15** | **A simulated day closes → orchestrate the batch layer** | Let the simulated clock cross midnight (≈2 min with `SIM_DAY_SECONDS=120` — this is the one wait worth showing, or cut to just the moment it happens). Terminal: `batch-file-generator` log line `feed.success_marker`. Then trigger it: `docker compose ... exec airflow airflow dags trigger daily_energy_reconciliation -e <date>` (check `ls data/lake/readings_raw/` first for the latest *closed* date). Switch to the **Airflow tab**, click the **`daily_energy_reconciliation` name link** (not ▶), then the **Graph** tab — watch the 6 boxes turn green over ~30–45s. Say one sentence on *why* it's `schedule=None` rather than `@daily` (a wall-clock schedule would never fire inside a demo this short) — the CLI trigger is the verified-reliable path; see `docs/RUNBOOK.md §3.2` if asked why it isn't automatic yet. |
| **5:15–6:15** | **The authoritative result** | `GET /api/billing/daily/<date>` in `/docs` — point at `"status": "final"` and read out one household's numbers (consumption, solar, net amount). Open the rendered `reports/billing_report_<date>.html` in a browser tab as the human-readable version of the same data. |
| **6:15–7:15** | **Observability — cause an alert on purpose** | `docker compose ... stop meter-simulator`. Within `ALERT_NO_DATA_MINUTES` (default 3 min — lower it in `.env` for the recording, e.g. `ALERT_NO_DATA_MINUTES=1`), show the `alerts` table gaining a row (`psql ... "select rule_name, grid_zone, severity, fired_at from alerts order by fired_at desc limit 5;"`) or the Grafana "Open Alerts" stat panel incrementing. Say which brief requirement this satisfies (*"no data received in N minutes"*). `docker compose ... start meter-simulator` and (optional, if time) show it resolve. |
| **7:15–7:50** | **Code quality, briefly** | Terminal: `make test` — let the green summary line show (100+ tests). One sentence: hexagonal architecture, ports/adapters, Strategy pattern for billing tiers and alert rules — point at one file in the editor for two seconds (e.g. `domain/billing.py`) as evidence, don't read code on camera. |
| **7:50–8:15** | **Close** | Restate the architecture decision in one sentence and where the report covers the rest: *"Full justification, diagrams, and results are in the report — this was the pipeline running end to end."* |

## If something doesn't cooperate on the day

- **DAG run already exists for that date** (e.g. you triggered it once while
  testing before recording): `airflow tasks clear daily_energy_reconciliation -s <date> -e <date> -y`
  re-runs it cleanly, or trigger a different, not-yet-used `business_date`.
- **No alert fires in time:** lower `ALERT_NO_DATA_MINUTES` further, or use
  `docker compose ... stop batch-file-generator` before a day closes to
  starve the feed and trigger `BatchFailureRule` instead the next cycle.
- **Running low on time:** cut 4:15–5:15 (the wait) hardest in editing — it's
  the one segment that's mostly dead air even at `SIM_DAY_SECONDS=120`.

## After recording

- Trim to the 5–10 minute window the brief asks for.
- Use the exact same run's numbers/screenshots for report §9 — record first,
  then fill in the report's `[SCREENSHOT: ...]` placeholders from stills
  pulled out of the video (or take the screenshots live while recording, muted,
  right after each segment).
