from __future__ import annotations

import json
from pathlib import Path


def load_physical_schema(project_root: Path) -> dict[str, set[str]]:
    schema_path = project_root / "tpcds_simple.json"
    raw_tables = json.loads(schema_path.read_text(encoding="utf-8"))
    return {table["table_name"]: set(table["columns"]) for table in raw_tables}


def validate_physical_column(schema: dict[str, set[str]], table_name: str, column_name: str) -> None:
    if table_name not in schema:
        raise ValueError(f"Unknown physical table: {table_name}")
    if column_name not in schema[table_name]:
        raise ValueError(f"Unknown physical column: {table_name}.{column_name}")
