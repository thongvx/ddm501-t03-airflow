# Tutorial 03 — Airflow: a pipeline that runs without you

## What this tutorial is for

The pipeline is deliberately not machine learning: ingest, validate, split, scale.

It now ends with one extra step, `train`, which fits a baseline classifier and
records the run in MLflow. See [Extension: the train task](#extension-the-train-task).
Everything above it is unchanged, and so are the four exercises.

## Two ways to run it

Both give the same DAG.

**A. Locally** (macOS, Linux, Windows + WSL2) — lighter, faster:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt

export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

The web UI comes up on <http://127.0.0.1:8080>. `standalone` prints the admin password on first start and also writes it to
`$AIRFLOW_HOME/standalone_admin_password.txt`.

**B. Docker** — one container, built once from the `Dockerfile` beside this file:

```bash
# On Linux only
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up -d --build
docker compose ps        # wait for STATUS = healthy, about a minute
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

After the first time, `docker compose up -d` is enough — Docker reuses the
image it already built. Add `--build` again only when you change the
`Dockerfile`.

<http://127.0.0.1:18080>, user `admin`. Port 18080 and not 8080, because Lab 2
owns 8080 and you will want both running one day.

Two containers come up, not one: Airflow, and the MLflow tracking server the
`train` task writes to. Airflow waits for MLflow to report healthy before it
starts, so `docker compose up -d` prints `mlflow Healthy` before
`airflow Starting`.

| | |
|---|---|
| Airflow UI | <http://127.0.0.1:18080> — user `admin` |
| MLflow UI | <http://127.0.0.1:15030> — no login |

## Running the pipeline

This runs every task in order, in your terminal:

```bash
airflow dags test wdbc_pipeline 2026-08-25
```

Then look at what it produced:

```
data/staging/2026-08-25/
  raw.parquet              snapshot of the extract, frozen for this run
  clean.parquet            rows that passed validation
  rejected.parquet         rows that did not, kept for inspection
  validation_report.json   what failed and how often
  train.parquet  test.parquet  scaler.json
  metrics.json             what the classifier scored on this date's test split
  summary.json
data/staging/history.jsonl one line per run
```

## The exercises

| | Do this | Look for |
|---|---|---|
| 1 | `airflow dags test wdbc_pipeline 2026-08-25` twice | The outputs are byte-identical and `history.jsonl` still has one line for that date. Re-running a date is safe. |
| 2 | `python scripts/corrupt_extract.py` then re-run | `validate` fails with `13.0% of rows rejected, limit is 5%`, and the log says **Immediate failure requested** — the three retries were skipped on purpose. Repair with `--repair`. |
| 3 | `airflow dags backfill wdbc_pipeline -s 2026-08-22 -e 2026-08-24` | Three run folders appear, one per date, three lines in `history.jsonl`. |
| 4 | Open the UI, Grid view, click a failed task, then Logs | The traceback for one task of one date, without SSH-ing anywhere. |

Exercise 4 needs a failed task that has a log file. `airflow dags test` prints to
your terminal and writes none, so produce the failure with `dags backfill`
instead — that runs the task through the executor, which does write one.

## Extension: the train task

`train` sits between `scale` and `report`. It fits a logistic regression on the
scaled training split, scores it on the test split, and logs the parameters, the
metrics and the model itself to MLflow under experiment `wdbc_pipeline`, tagged
with the run's `ds`.

```bash
docker compose exec airflow airflow dags test wdbc_pipeline 2026-08-25
open http://127.0.0.1:15030          # one run named wdbc-2026-08-25
```

Three things about it are worth more than the model is:

**Re-running a date still gives identical bytes.** `train` writes `metrics.json`
rounded to six decimals, and before logging it deletes any earlier MLflow run
carrying the same `ds` tag — the rule `report` already applies to
`history.jsonl`. So a second run of a date replaces its MLflow run instead of
leaving two that disagree. The MLflow run id is deliberately kept out of
`summary.json`, because it changes every run and would break exercise 1.

**A bad extract never reaches the model.** `train` is downstream of `validate`,
so the failure in exercise 2 leaves it `upstream_failed`. Nothing gets trained
on the 13% of rows that were rejected, and MLflow gets no run for that date.

**The client is `mlflow-skinny`, and the server lives in its own container.**
Full `mlflow` brings its own Flask, SQLAlchemy and alembic, which collide with
the versions Airflow 2.8.4 pins. The Airflow image takes the tracking client
only; `Dockerfile.mlflow` builds the server separately.