# YAML-driven Python Data Engineering Pipeline

This repository contains a simple data engineering pipeline that:

1. Reads source datasets in **CSV** and **Parquet** formats from a YAML config.
2. Loads source data into a **raw DuckDB** database with configured table names.
3. Applies YAML-driven transformations (**filter**, **join**, **aggregate**, and optional raw SQL).
4. Loads selected output tables into a target **PostgreSQL** database configured in YAML.

## Files

- `pipeline.py` - pipeline runner.
- `config/pipeline.yaml` - sample configuration.
- `sample_data/customers.csv` - sample CSV source.
- `create_sample_data.py` - helper script to generate sample CSV/Parquet source files.
- `requirements.txt` - Python dependencies.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generate sample data (optional)

```bash
python create_sample_data.py
```

## Run

Run full pipeline (includes Postgres load):

```bash
python pipeline.py --config config/pipeline.yaml
```

Run only source load + transforms (skip Postgres target):

```bash
python pipeline.py --config config/pipeline.yaml --skip-target
```

## YAML structure

```yaml
raw_db:
  path: raw.duckdb

sources:
  - name: customers_csv
    format: csv
    path: sample_data/customers.csv
    table: customers
  - name: orders_parquet
    format: parquet
    path: sample_data/orders.parquet
    table: orders

transformations:
  - type: filter
    source: raw.orders
    condition: amount > 50
    output_table: high_value_orders
  - type: join
    left: curated.high_value_orders
    right: raw.customers
    on: curated.high_value_orders.customer_id = raw.customers.customer_id
    output_table: customer_orders
  - type: aggregate
    source: curated.customer_orders
    group_by: [customer_id, customer_name]
    metrics:
      - COUNT(*) AS order_count
      - SUM(amount) AS total_amount
    output_table: customer_order_summary

target:
  postgres:
    host: localhost
    port: 5432
    database: analytics
    user: postgres
    password: postgres

targets:
  - source_table: curated.customer_order_summary
    target_schema: public
    target_table: customer_order_summary
    if_exists: replace
```

## Notes

- Source tables are loaded under `raw.<table_name>` in DuckDB.
- Transformation outputs are written under `curated.<output_table>` in DuckDB.
- Each entry in `targets` loads a DuckDB table/query source into the configured Postgres DB.
