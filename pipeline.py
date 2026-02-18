#!/usr/bin/env python3
"""Simple YAML-driven data engineering pipeline.

Pipeline stages:
1) Read source datasets (CSV/Parquet) configured in YAML.
2) Load them into a DuckDB "raw" database.
3) Run YAML-driven transformations (filter, join, aggregate, SQL).
4) Load selected output tables into a target Postgres database.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd
import yaml
from sqlalchemy import create_engine


@dataclass
class PipelineConfig:
    raw_db_path: str
    sources: List[Dict[str, Any]]
    transformations: List[Dict[str, Any]]
    targets: List[Dict[str, Any]]
    postgres: Dict[str, Any]


SUPPORTED_SOURCE_FORMATS = {"csv", "parquet"}


def load_yaml_config(config_path: str) -> PipelineConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        payload = yaml.safe_load(f)

    raw_db_path = payload.get("raw_db", {}).get("path", "raw.duckdb")
    sources = payload.get("sources", [])
    transformations = payload.get("transformations", [])
    targets = payload.get("targets", [])
    postgres = payload.get("target", {}).get("postgres", {})

    if not sources:
        raise ValueError("Config must include at least one source in 'sources'.")
    if not postgres:
        raise ValueError("Config must include postgres target under 'target.postgres'.")
    if not targets:
        raise ValueError("Config must include at least one target table under 'targets'.")

    return PipelineConfig(
        raw_db_path=raw_db_path,
        sources=sources,
        transformations=transformations,
        targets=targets,
        postgres=postgres,
    )


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def load_sources_to_duckdb(conn: duckdb.DuckDBPyConnection, sources: List[Dict[str, Any]]) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for src in sources:
        name = src["name"]
        table = src["table"]
        fmt = src["format"].lower()
        path = src["path"]

        if fmt not in SUPPORTED_SOURCE_FORMATS:
            raise ValueError(f"Unsupported source format '{fmt}' for source '{name}'.")
        if not Path(path).exists():
            raise FileNotFoundError(f"Source file does not exist: {path}")

        full_table = f"raw.{quote_ident(table)}"
        if fmt == "csv":
            sql = f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM read_csv_auto(?, header=true)"
        else:
            sql = f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM read_parquet(?)"

        conn.execute(sql, [path])
        print(f"Loaded source '{name}' into {full_table}")


def apply_transformations(conn: duckdb.DuckDBPyConnection, transformations: List[Dict[str, Any]]) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS curated")

    for step in transformations:
        t_type = step["type"].lower()
        output_table = step["output_table"]
        output_name = f"curated.{quote_ident(output_table)}"

        if t_type == "filter":
            source = step["source"]
            condition = step["condition"]
            sql = (
                f"CREATE OR REPLACE TABLE {output_name} AS "
                f"SELECT * FROM {source} WHERE {condition}"
            )
        elif t_type == "join":
            left = step["left"]
            right = step["right"]
            join_type = step.get("join_type", "inner").upper()
            on = step["on"]
            select_expr = step.get("select", "*")
            sql = (
                f"CREATE OR REPLACE TABLE {output_name} AS "
                f"SELECT {select_expr} FROM {left} {join_type} JOIN {right} ON {on}"
            )
        elif t_type == "aggregate":
            source = step["source"]
            group_by = step.get("group_by", [])
            metrics = step["metrics"]
            where = step.get("where")

            group_expr = ", ".join(group_by) if group_by else ""
            metric_expr = ", ".join(metrics)
            select_cols = ", ".join([x for x in [group_expr, metric_expr] if x])
            where_clause = f" WHERE {where}" if where else ""
            group_clause = f" GROUP BY {group_expr}" if group_expr else ""

            sql = (
                f"CREATE OR REPLACE TABLE {output_name} AS "
                f"SELECT {select_cols} FROM {source}{where_clause}{group_clause}"
            )
        elif t_type == "sql":
            sql_query = step["query"]
            sql = f"CREATE OR REPLACE TABLE {output_name} AS {sql_query}"
        else:
            raise ValueError(f"Unsupported transformation type: {t_type}")

        conn.execute(sql)
        print(f"Applied transformation '{t_type}' -> {output_name}")


def postgres_connection_uri(cfg: Dict[str, Any]) -> str:
    required = ["host", "port", "database", "user", "password"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing postgres target config fields: {', '.join(missing)}")
    return (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def load_targets_to_postgres(
    conn: duckdb.DuckDBPyConnection, targets: List[Dict[str, Any]], pg_cfg: Dict[str, Any]
) -> None:
    engine = create_engine(postgres_connection_uri(pg_cfg))

    with engine.begin() as pg_conn:
        for tgt in targets:
            source_table = tgt["source_table"]
            target_table = tgt["target_table"]
            target_schema = tgt.get("target_schema", "public")
            if_exists = tgt.get("if_exists", "replace")

            df = conn.execute(f"SELECT * FROM {source_table}").fetch_df()
            df.to_sql(
                name=target_table,
                con=pg_conn,
                schema=target_schema,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=10000,
            )
            print(
                f"Loaded {len(df)} rows from '{source_table}' "
                f"to Postgres table {target_schema}.{target_table}"
            )


def run_pipeline(config_path: str, skip_target: bool = False) -> None:
    cfg = load_yaml_config(config_path)

    with duckdb.connect(cfg.raw_db_path) as conn:
        load_sources_to_duckdb(conn, cfg.sources)
        apply_transformations(conn, cfg.transformations)
        if skip_target:
            print("Skipping Postgres load (--skip-target enabled).")
        else:
            load_targets_to_postgres(conn, cfg.targets, cfg.postgres)

    print("Pipeline completed successfully.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run YAML-driven data pipeline")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML config file.",
    )
    parser.add_argument(
        "--skip-target",
        action="store_true",
        help="Run source loads and transformations, but skip loading to Postgres.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.config, skip_target=args.skip_target)
