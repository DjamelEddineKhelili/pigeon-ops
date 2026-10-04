# LEVEL 1: The plumbing

> **Goal:** live data from 3 public sources flows by itself, every few minutes, into a **data lake**, then into a **warehouse** of clean tables. **n8n** is the boss that says when. **No AI yet.**

```
                      ┌──────────── n8n (the office manager) ────────────┐
                      │  every 5 min · every hour · every 3 h · daily     │
                      └───────────────┬──────────────────────────────────┘
                                      │ HTTP orders: POST /ingest/..., POST /build
                                      ▼
  Vélib' API ─┐            ┌─────────────────────────────┐
  Open-Meteo ─┼─ download ─►  pipeline (FastAPI, Python)  │
  Fuel CSV  ──┘            └──────┬──────────────┬───────┘
                                  │ raw, gzip    │ SQL (DuckDB)
                                  ▼              ▼
                     lake/bronze/... (garage)  warehouse.duckdb → silver.* tables (shop)
```

## ✅ Definition of done

- [ ] `python -m checks.check_l1` → **13/13**
- [ ] `docker compose ps` → `pipeline` and `n8n` are both **running**
- [ ] The n8n workflow **"L1 · Data Engineering"** is active, and its executions are green
- [ ] `python -m checks.check_l1 --live` → **3/3**
- [ ] The system has run for **24 hours**, and you ran the 4 first-look queries (Step 12)
- [ ] It's pushed to GitHub with the workflow export and a screenshot

Steps 1–2 are setup. Steps 3–7 are **your code**, in pair mode. Steps 8–11 are infra and n8n. Steps 12–13 are looking and shipping.

---

## Step 1: Install the tools (once)

1. **Docker Desktop for Windows**: install it from docker.com, keep **"Use WSL 2"** checked, and restart the PC when it asks. Open Docker Desktop once and wait for the whale icon to say *running*.
2. Check in a **new** PowerShell:
   ```powershell
   docker --version
   docker compose version
   python --version      # 3.11 or 3.12 is fine
   git --version
   ```
   If `docker` is "not recognized", Docker Desktop isn't running, or you need to reopen PowerShell.

> 🐳 **What's Docker?** A container is a **lunchbox**: the program plus *everything* it needs (Python, libraries, settings), packed so it runs the same on any machine. `docker-compose.yml` is the **menu**: which lunchboxes to open, and how they talk to each other.

## Step 2: Create the project

1. Unzip `paris-data-dept.zip` into a short path, **not** Downloads (to avoid the `analyst-crew\analyst-crew` double-folder thing). For example: `C:\Users\djame\Projects\paris-data-dept`.
2. In PowerShell:
   ```powershell
   cd C:\Users\djame\Projects\paris-data-dept
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python -m checks.check_l1
   ```
   You should see **0/13** with ⬜ everywhere. That's normal: ⬜ = "not written yet".
3. First commit:
   ```powershell
   git init
   git add .
   git commit -m "Level 1 scaffold"
   ```

---

## Step 3: The lake → `pipeline/lake.py` ✍️

Fill the 3 functions: `bronze_path`, `write_atomic`, `log_ingestion`. Every docstring has the hints.

```powershell
python -m checks.check_l1
```
Target: the first **3 ✅**.

> 📚 **Concepts**
> - **Bronze** = raw, untouched. If our cleaning has a bug, we rebuild from bronze. Nothing is ever lost.
> - **Partitioning** (`source=/date=/hour=`) = drawers. Tools can skip whole drawers they don't need.
> - **Atomic write** = never let anyone read a half-written file (`.tmp` then rename).
> - **gzip**: JSON compresses ~15×. The lake is cheap, not free.

## Step 4: Ingestion → `pipeline/ingest.py` ✍️

Fill the success path (5 small steps in the TODO). Target: checks **4 and 5 ✅**. They use a fake internet, so no network is needed.

Then go fetch the **real** world:
```powershell
python -m pipeline.ingest velib_status
python -m pipeline.ingest velib_info
python -m pipeline.ingest weather
python -m pipeline.ingest fuel
```
Open the `lake` folder in the Explorer and walk the drawers. Open `lake\_ingestion_log.jsonl` in VS Code. Compare `raw_bytes` with `stored_bytes`: that ratio is your compression.

> 💡 For fun, break a URL in `sources.py` (add an `x`) and re-run. The run does **not** crash: you get a clean `"status": "error"` line in the log. Then put the URL back.

## Step 5: Look before you clean → `pipeline/explore.py` (already written)

```powershell
python -m pipeline.explore velib_status
python -m pipeline.explore velib_info
python -m pipeline.explore weather
python -m pipeline.explore fuel
```
Create a file **`NOTES.md`** (your *data dirt diary*). Write down **at least 5 weird things** you see. Some leads:
- Two fields that say the same thing with different names?
- Is `lastUpdatedOther` a standard name? (Google "GBFS last_updated".)
- How are bike types stored?
- Fuel: in what language are the columns? What type are the prices? What on earth is `latitude = 4818300`? Compare it with `geom`.

This diary becomes the **job description of the Level 2 Quality team**. Don't skip it.

## Step 6: The warehouse → `pipeline/warehouse.py` ✍️

Fill every `???` (5 gaps) and the `parts` list. Target: checks **7 to 13 ✅**.

Then build on the **real** data and ask it questions:
```powershell
python -m pipeline.warehouse
python -m pipeline.sql "SELECT count(*) FROM silver.station_status"
python -m pipeline.sql "SELECT fuel, round(avg(price), 3) AS avg_price, count(*) FROM silver.fuel_prices GROUP BY fuel ORDER BY avg_price"
```
Run `python -m pipeline.ingest velib_status` again, rebuild, and count again. The count does **not** double. Do you understand why? (Hint: PROBLEM 2.)

> 📚 **Concepts**
> - **Silver** = cleaned and typed, one row per real-world fact.
> - **Deduplication** by *identity* (`station_id + reported_at`), not by "the whole row is identical".
> - **Wide → long** (unpivot): one column per fuel becomes one row per fuel. Analysts and charts want long.
> - **Idempotent** = run it twice, same result. That's the property that lets you retry anything safely.
> - **UTC everywhere.** Paris time is for humans, and summer/winter time changes break naive timestamps.

## Step 7: The API → `pipeline/app.py` ✍️

Fill `ingest_source` (3 rules in the docstring). Target: **13/13** 🎉

Try the API by hand:
```powershell
uvicorn pipeline.app:app --reload
```
Open **http://localhost:8000/docs**. It's an interactive page FastAPI generates for free. Click `POST /ingest/{source}` → *Try it out* → `weather` → *Execute*. Then `POST /build`, then `GET /stats`. Try `nope` as a source and read the 404.

Stop the server with **Ctrl+C**, then commit:
```powershell
git add .
git commit -m "Level 1: lake, ingestion, warehouse, API (13/13 offline checks)"
```

---

## Step 8: Put the department in containers

1. Make sure the local uvicorn is **stopped** (otherwise port 8000 is taken).
2. Run:
   ```powershell
   docker compose up -d --build
   docker compose ps
   ```
   Both `pipeline` and `n8n` should say `running` (or `Up`).
3. Run `docker compose logs -f pipeline` to watch the API logs live (Ctrl+C stops *watching*, not the container).
4. Open **http://localhost:8000/docs** again: same API, now inside Docker, writing into the **same** `lake/` folder (see `volumes:` in the compose file).

From now on, the **container owns the warehouse**. Query it from inside:
```powershell
docker compose exec pipeline python -m pipeline.sql "SELECT count(*) FROM silver.station_status"
```
(Running `python -m pipeline.warehouse` on Windows *while* the container builds = two writers = a lock error. Let the container do the building.)

> 📚 Inside Docker, n8n reaches the API at **`http://pipeline:8000`**, the *service name*. `localhost` inside the n8n container means *n8n itself*. This is the #1 Docker beginner trap.

## Step 9: First launch of n8n

1. Open **http://localhost:5678**.
2. Create the **owner account**. It's local: it lives in the `n8n_data` volume on your PC.
3. Skip the survey and the tour.

> 🔁 **Useful commands**
> - `docker compose stop` pauses everything, `docker compose start` resumes.
> - `docker compose down` removes the containers. Your workflows survive in the volume, and your data survives in `lake/`.
> - **Never** run `docker compose down -v` unless you want to delete your n8n account and workflows.

## Step 10: Build the workflow "L1 · Data Engineering"

**Overview → Create workflow.** Name it `L1 · Data Engineering`. On **one canvas**, build **5 pairs** of *Schedule Trigger → HTTP Request*. One canvas = one beautiful screenshot for the README.

For each pair: **+** → search **"Schedule Trigger"** → set it up → then **+** on its right → **"HTTP Request"** → set it up.

| # | Schedule Trigger | HTTP Request: Method + URL | HTTP options |
|---|---|---|---|
| 1 | Interval **Minutes**, every **5** | `POST` `http://pipeline:8000/ingest/velib_status` | |
| 2 | Interval **Hours**, every **1**, at minute **2** | `POST` `http://pipeline:8000/ingest/weather` | |
| 3 | Interval **Hours**, every **3**, at minute **7** | `POST` `http://pipeline:8000/ingest/fuel` | Options → **Timeout** `120000` (the file is 19 MB) |
| 4 | Interval **Days**, every **1**, at hour **4**, minute **0** | `POST` `http://pipeline:8000/ingest/velib_info` | |
| 5 | Interval **Minutes**, every **15** | `POST` `http://pipeline:8000/build` | Options → **Timeout** `300000` |

On **every** HTTP Request node, open the **Settings** tab and set:
- **Retry On Fail**: ON
- **Max Tries**: 3
- **Wait Between Tries**: 5000 ms

APIs hiccup; a retry fixes most of it. That's why ingestion and build must be **idempotent**: a retry must never create duplicates.

Rename the nodes to something human: `Every 5 min` → `Ingest Vélib status`, etc.

**Test before activating.** Click each HTTP node → **Execute step**. You should see the JSON returned by your API (status `ok`, path, sizes). For #5 you should see the build report.

**Error workflow** (n8n's alarm bell; it gets a real notification in Level 2):
1. Create a second workflow, `L1 · Errors`, containing a single **Error Trigger** node. Save it.
2. Back in `L1 · Data Engineering`: **⋯ → Settings → Error workflow** → `L1 · Errors` → Save.

**Activate:** turn the workflow on with the **Active** toggle (or **Publish**, depending on your n8n version) in the top bar. From now on it runs even when the browser is closed.

## Step 11: Watch it run

- n8n → **Executions** tab: a green line every 5 minutes. Click one to see exactly what happened.
- **http://localhost:8000/stats**: row counts and `minutes_ago` per source.
- After ~20 minutes, from Windows (venv active):
  ```powershell
  python -m checks.check_l1 --live
  ```
  Target: **3/3**. Fuel and Vélib station info only pass once their own schedule has run at least once. You can force them with *Execute step* in n8n.

---

## Step 12: Let it live 24 h, then look

Data only gets interesting with time. Keep the PC awake: **Settings → System → Power → Sleep: Never** (while plugged in). Or accept the gaps: they're realistic, and Level 2 will detect them 😉

After 24 h, run these 4 first-look queries. Each one is a Level 2 story waiting to happen.

```powershell
# 1. Which big stations are EMPTY right now?
docker compose exec pipeline python -m pipeline.sql "WITH latest AS (SELECT * FROM silver.station_status QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY reported_at DESC) = 1) SELECT i.name, l.bikes, i.capacity, l.reported_at FROM latest l JOIN silver.station_info i USING (station_id) WHERE l.bikes = 0 AND l.is_renting ORDER BY i.capacity DESC LIMIT 10"

# 2. Ghost stations: silent for more than a day
docker compose exec pipeline python -m pipeline.sql "WITH latest AS (SELECT * FROM silver.station_status QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY reported_at DESC) = 1) SELECT count(*) FILTER (WHERE reported_at < now() AT TIME ZONE 'UTC' - INTERVAL 1 DAY) AS silent_for_a_day, min(reported_at) AS oldest FROM latest"

# 3. Impossible: more bikes than slots
docker compose exec pipeline python -m pipeline.sql "SELECT i.name, max(l.bikes) AS max_bikes, i.capacity FROM silver.station_status l JOIN silver.station_info i USING (station_id) WHERE l.bikes > i.capacity GROUP BY i.name, i.capacity ORDER BY max_bikes - i.capacity DESC LIMIT 10"

# 4. Fuel prices nobody updated for a month
docker compose exec pipeline python -m pipeline.sql "SELECT fuel, count(*) FILTER (WHERE updated_at < now() AT TIME ZONE 'UTC' - INTERVAL 30 DAY) AS stale_30d, count(*) AS total FROM silver.fuel_prices GROUP BY fuel ORDER BY fuel"
```
Add what you find to `NOTES.md`. (Spoiler from a test run: stations silent since **2021**, and one station with **47 bikes for 42 slots**.)

## Step 13: Ship it

1. **Export the workflow:** in n8n, open it → **⋯ → Download** → save as `n8n/level1_data_engineering.json`. Do the same for `L1 · Errors`.
2. **Screenshot** the canvas → `docs/n8n-level1.png`.
3. Write a first `README.md`: the diagram from the top of this file, the screenshot, the 3 sources, and your 3 most surprising findings from `NOTES.md`.
4. Push:
   ```powershell
   git add .
   git commit -m "Level 1 complete: live lake + warehouse orchestrated by n8n"
   gh repo create paris-data-dept --public --source=. --remote=origin --push
   ```

---

## 🧯 Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| n8n HTTP node: `ECONNREFUSED 127.0.0.1:8000` | URL uses `localhost` | Use `http://pipeline:8000/...` |
| `docker compose up`: port 8000 already allocated | local uvicorn still running | Ctrl+C it, then `docker compose up -d` |
| `/build` returns 409 | two builds at once | Normal, the lock works. The next one will pass. |
| `/build` returns 500 with `FileNotFoundError` | a source never ingested yet | Run its ingestion once (Execute step) |
| `IO Error: Could not set lock on file` | two processes writing the warehouse | Build only from the container (Step 8) |
| `/ingest/fuel` 502 with a timeout | slow download | HTTP node timeout 120000, and the retries handle it |
| Code edits not picked up in Docker | reload missed | `docker compose restart pipeline` |
| Executions stop at night | the PC went to sleep | Power settings, or accept the gap |

## What you can say after Level 1

> "I built a small data platform on live Paris data: 3 public sources ingested on a schedule by n8n into a partitioned, compressed bronze lake, then rebuilt idempotently into a DuckDB silver warehouse, all containerised with Docker Compose and exposed through a FastAPI service, with an ingestion log, retries and an error workflow."

Every word of that sentence, you now understand. **Next: Level 2, the Quality team (first agents + human approval on your phone).**
