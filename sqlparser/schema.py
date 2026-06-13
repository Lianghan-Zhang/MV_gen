from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PhysicalSchema:
    tables: dict[str, set[str]]

    @classmethod
    def load(cls, project_root: str | Path | None = None) -> "PhysicalSchema":
        root = Path(project_root) if project_root is not None else Path(__file__).resolve().parents[1]
        schema_path = root / "tpcds_simple.json"
        raw_tables = json.loads(schema_path.read_text(encoding="utf-8"))
        return cls(
            tables={
                table["table_name"].lower(): {column.lower() for column in table["columns"]}
                for table in raw_tables
            }
        )

    def is_physical_table(self, table_name: str) -> bool:
        return table_name.lower() in self.tables

    def table_has_column(self, table_name: str, column_name: str) -> bool:
        return column_name.lower() in self.tables.get(table_name.lower(), set())

    def tables_for_column(self, column_name: str) -> list[str]:
        column = column_name.lower()
        return [table for table, columns in self.tables.items() if column in columns]
