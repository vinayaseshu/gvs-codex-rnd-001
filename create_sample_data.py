#!/usr/bin/env python3
"""Generate sample source files used by the pipeline config."""

from pathlib import Path

import pandas as pd


def main() -> None:
    out_dir = Path("sample_data")
    out_dir.mkdir(parents=True, exist_ok=True)

    customers = pd.DataFrame(
        [
            {"customer_id": 1, "customer_name": "Alice", "country": "US"},
            {"customer_id": 2, "customer_name": "Bob", "country": "UK"},
            {"customer_id": 3, "customer_name": "Carla", "country": "IN"},
            {"customer_id": 4, "customer_name": "Dan", "country": "US"},
        ]
    )
    customers.to_csv(out_dir / "customers.csv", index=False)

    orders = pd.DataFrame(
        [
            {"order_id": 1001, "customer_id": 1, "amount": 42.5, "order_date": "2026-01-04"},
            {"order_id": 1002, "customer_id": 1, "amount": 199.0, "order_date": "2026-01-10"},
            {"order_id": 1003, "customer_id": 2, "amount": 75.0, "order_date": "2026-01-12"},
            {"order_id": 1004, "customer_id": 3, "amount": 20.0, "order_date": "2026-01-13"},
            {"order_id": 1005, "customer_id": 4, "amount": 600.0, "order_date": "2026-01-14"},
        ]
    )
    orders.to_parquet(out_dir / "orders.parquet", index=False)

    print("Generated sample_data/customers.csv and sample_data/orders.parquet")


if __name__ == "__main__":
    main()
