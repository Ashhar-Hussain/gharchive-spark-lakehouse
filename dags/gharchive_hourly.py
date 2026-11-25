"""Hourly GH Archive pipeline: download -> bronze -> silver -> contract check -> gold.

Each run processes the hour that ended before its logical date, so runs are independent and
re-runnable (dynamic partition overwrite makes every step idempotent). Gold tables whose inputs
fail the data contract are skipped for that hour instead of being published with wrong numbers.

Requires Airflow 2.7+ and pyspark; set LAKE_ROOT to the lake location (local path or mounted bucket).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

LAKE = Path(os.environ.get("LAKE_ROOT", "/opt/lake"))
RAW = LAKE / "raw"
CONTRACT = LAKE / "_state" / "contract.json"


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=2,
    default_args=dict(retries=3, retry_delay=timedelta(minutes=10), retry_exponential_backoff=True),
    tags=["gharchive", "lakehouse"],
)
def gharchive_hourly():

    @task
    def download(data_interval_start=None) -> str:
        from pipeline.lakehouse import fetch_hour
        return str(fetch_hour(data_interval_start, RAW))

    @task
    def validate(path: str) -> dict:
        from pipeline.lakehouse import profile, learn_contract, check, gold_decision
        batch = profile([Path(path)])
        if not CONTRACT.exists():  # first run: learn the contract from this hour, then reuse it
            CONTRACT.parent.mkdir(parents=True, exist_ok=True)
            CONTRACT.write_text(json.dumps(learn_contract(batch)))
        results = check(json.loads(CONTRACT.read_text()), batch)
        return dict(results=results, decision=gold_decision(results))

    @task
    def bronze_and_silver(path: str) -> list:
        from pipeline.lakehouse import spark_session, to_bronze, to_silver
        name = Path(path).name.replace(".json.gz", "")
        dt, hr = name.rsplit("-", 1)
        spark = spark_session()
        to_bronze(spark, [Path(path)], LAKE)
        return list(to_silver(spark, [(dt, int(hr))], LAKE))

    @task
    def gold(report: dict, _silver_counts: list) -> list:
        from pipeline.lakehouse import spark_session, to_gold
        tables = [t for t, d in report["decision"].items() if d == "publish"]
        if tables:
            to_gold(spark_session(), LAKE, tables)
        return tables

    path = download()
    report = validate(path)
    counts = bronze_and_silver(path)  # raw truth always lands in bronze/silver
    gold(report, counts)


gharchive_hourly()
