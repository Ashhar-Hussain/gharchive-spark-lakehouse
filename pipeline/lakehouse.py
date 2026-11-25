"""Bronze/silver/gold steps for GH Archive, importable by the notebook's DAG.

Same logic as the notebook, packaged so that each step can run as an orchestrated task
on one hour of data at a time.
"""
from __future__ import annotations

import gzip
import json
import shutil
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

USER_AGENT = "gharchive-lakehouse-notebook/1.0"

SCHEMA = StructType([
    StructField("id", StringType()), StructField("type", StringType()),
    StructField("actor", StructType([StructField("id", LongType()), StructField("login", StringType())])),
    StructField("repo", StructType([StructField("id", LongType()), StructField("name", StringType())])),
    StructField("org", StructType([StructField("login", StringType())])),
    StructField("public", BooleanType()), StructField("created_at", StringType()),
    StructField("payload", StructType([
        StructField("action", StringType()), StructField("size", LongType()), StructField("distinct_size", LongType()),
        StructField("ref_type", StringType()),
        StructField("pull_request", StructType([StructField("merged", BooleanType()), StructField("additions", LongType()),
                                                StructField("deletions", LongType()), StructField("changed_files", LongType())])),
    ])),
    StructField("_corrupt_record", StringType()),
])

QSCHEMA = StructType([StructField("id", StringType()), StructField("type", StringType()), StructField("created_at", StringType()),
                      StructField("reject_reason", StringType()), StructField("_source_file", StringType()),
                      StructField("dt", StringType()), StructField("hr", LongType())])

KNOWN_TYPES = ["PushEvent", "CreateEvent", "DeleteEvent", "PullRequestEvent", "PullRequestReviewEvent",
               "PullRequestReviewCommentEvent", "IssuesEvent", "IssueCommentEvent", "WatchEvent", "ForkEvent",
               "ReleaseEvent", "MemberEvent", "PublicEvent", "GollumEvent", "CommitCommentEvent"]


def spark_session(app: str = "gharchive-lakehouse") -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def fetch_hour(t: datetime, raw_dir: Path) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    name = f"{t:%Y-%m-%d}-{t.hour}.json.gz"
    path = raw_dir / name
    if not path.exists():
        req = urllib.request.Request(f"https://data.gharchive.org/{name}", headers={"User-Agent": USER_AGENT})
        tmp = path.with_suffix(".part")
        with urllib.request.urlopen(req, timeout=300) as r, open(tmp, "wb") as f:
            shutil.copyfileobj(r, f)
        tmp.replace(path)  # never leave a half-written file behind
    return path


def to_bronze(spark: SparkSession, paths: list[Path], lake: Path) -> int:
    df = (spark.read.schema(SCHEMA).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
          .json([str(p) for p in paths])
          .withColumn("_source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
          .withColumn("_ingested_at", F.current_timestamp()))
    rx = r"(\d{4}-\d{2}-\d{2})-(\d{1,2})\.json\.gz"
    df = df.withColumn("dt", F.regexp_extract("_source_file", rx, 1)).withColumn("hr", F.regexp_extract("_source_file", rx, 2).cast("int"))
    df.write.mode("overwrite").partitionBy("dt", "hr").parquet(str(lake / "bronze"))
    return df.count()


def to_silver(spark: SparkSession, parts: list[tuple[str, int]], lake: Path) -> tuple[int, int]:
    cond = F.lit(False)
    for dt, hr in parts:
        cond = cond | ((F.col("dt") == dt) & (F.col("hr") == hr))
    b = spark.read.parquet(str(lake / "bronze")).filter(cond)
    ts = F.to_timestamp("created_at")
    start = F.to_timestamp(F.concat_ws(" ", F.col("dt").cast("string"), F.format_string("%02d:00:00", "hr")))
    b = b.withColumn("event_ts", ts).withColumn(
        "reject_reason",
        F.when(F.col("_corrupt_record").isNotNull(), "corrupt")
         .when(F.col("id").isNull() | F.col("type").isNull() | F.col("actor.id").isNull() | F.col("repo.id").isNull(), "missing_required")
         .when(ts.isNull() | (F.abs(F.unix_timestamp(ts) - F.unix_timestamp(start) - 1800) > 5400), "timestamp_out_of_window")
         .when(~F.col("type").isin(KNOWN_TYPES), "unknown_type"))
    bad = b.filter(F.col("reject_reason").isNotNull())
    good = b.filter(F.col("reject_reason").isNull())
    good = good.withColumn("_rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("_ingested_at").desc()))).filter("_rn = 1")
    silver = good.select(
        F.col("id").cast("long").alias("event_id"), "type", "event_ts", "dt", "hr",
        F.col("actor.id").alias("actor_id"), F.col("actor.login").alias("actor_login"),
        F.col("actor.login").endswith("[bot]").alias("is_bot"),
        F.col("repo.id").alias("repo_id"), F.col("repo.name").alias("repo_name"), F.col("org.login").alias("org_login"),
        F.col("payload.action").alias("action"), F.col("payload.size").alias("push_commits"),
        F.col("payload.ref_type").alias("ref_type"), F.col("payload.pull_request.merged").alias("pr_merged"),
        F.col("payload.pull_request.additions").alias("pr_additions"), F.col("payload.pull_request.deletions").alias("pr_deletions"))
    silver.write.mode("overwrite").partitionBy("dt", "hr").parquet(str(lake / "silver"))
    bad.select("id", "type", "created_at", "dt", "hr", "reject_reason", "_source_file").write.mode("overwrite") \
       .partitionBy("dt", "hr").parquet(str(lake / "quarantine"))
    return silver.count(), bad.count()


def profile(paths: list[Path]) -> dict:
    types, pr = Counter(), Counter()
    per_hour, n = [], 0
    for p in paths:
        k = 0
        with gzip.open(p, "rt") as f:
            for line in f:
                e = json.loads(line); n += 1; k += 1
                types[e["type"]] += 1
                if e["type"] == "PullRequestEvent":
                    pull = (e.get("payload") or {}).get("pull_request") or {}
                    pr["n"] += 1; pr["has_merged"] += "merged" in pull; pr["has_additions"] += "additions" in pull
        per_hour.append(k)
    return dict(n=n, per_hour=per_hour, types=dict(types), pr=dict(pr))


def learn_contract(reference: dict) -> dict:
    share = {t: c / reference["n"] for t, c in reference["types"].items()}
    return dict(volume=(0.5 * min(reference["per_hour"]), 2.0 * max(reference["per_hour"])),
                required={t: 0.25 * s for t, s in share.items() if s >= 0.01},
                pr_field_min=0.95, allowed=sorted(reference["types"]))


def check(contract: dict, batch: dict) -> list[dict]:
    out = []
    lo, hi = contract["volume"]
    out += [dict(check="volume", item=f"hour {i}", ok=lo <= k <= hi, detail=f"{k:,} events") for i, k in enumerate(batch["per_hour"])]
    for t, m in contract["required"].items():
        s = batch["types"].get(t, 0) / max(batch["n"], 1)
        out.append(dict(check="required type", item=t, ok=s >= m, detail=f"share {s:.2%}"))
    n_pr = max(batch["pr"].get("n", 0), 1)
    for field in ("has_merged", "has_additions"):
        rate = batch["pr"].get(field, 0) / n_pr
        out.append(dict(check="payload field", item=field, ok=rate >= contract["pr_field_min"], detail=f"{rate:.0%}"))
    new = sorted(set(batch["types"]) - set(contract["allowed"]))
    out.append(dict(check="closed type list", item="unknown types", ok=not new, detail=", ".join(new) or "none"))
    return out


GOLD_DEPENDS = {"hourly_activity": ["volume"], "repo_activity": ["required type", "payload field"], "bot_share": []}


def gold_decision(results: list[dict]) -> dict:
    failed = {r["check"] for r in results if not r["ok"]}
    return {g: ("hold back" if set(deps) & failed else "publish") for g, deps in GOLD_DEPENDS.items()}


def to_gold(spark: SparkSession, lake: Path, tables: list[str]) -> None:
    s = spark.read.parquet(str(lake / "silver"))
    builders = {
        "hourly_activity": lambda: s.groupBy("dt", "hr", "type").agg(F.count("*").alias("events"), F.countDistinct("actor_id").alias("actors"),
                                                                     F.countDistinct("repo_id").alias("repos")),
        "repo_activity": lambda: s.groupBy("dt", "repo_name").agg(
            F.sum((F.col("type") == "WatchEvent").cast("int")).alias("stars"),
            F.sum((F.col("type") == "ForkEvent").cast("int")).alias("forks"),
            F.sum(((F.col("type") == "PullRequestEvent") & (F.col("action") == "opened")).cast("int")).alias("prs_opened"),
            F.sum(((F.col("type") == "PullRequestEvent") & (F.col("action") == "closed") & F.col("pr_merged")).cast("int")).alias("prs_merged"),
            F.sum((F.col("type") == "PushEvent").cast("int")).alias("pushes"),
            F.sum(F.coalesce("push_commits", F.lit(0))).alias("commits")),
        "bot_share": lambda: s.groupBy("dt", "type").agg(F.avg(F.col("is_bot").cast("double")).alias("bot_share"), F.count("*").alias("events")),
    }
    for name in tables:
        builders[name]().write.mode("overwrite").partitionBy("dt").parquet(str(lake / "gold" / name))
