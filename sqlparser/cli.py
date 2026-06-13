from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlparser.extractor import DEFAULT_DIALECT, extract_sql_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract sqlglot-based SQL features.")
    parser.add_argument("path", type=Path, help="SQL file or directory containing .sql files")
    parser.add_argument("--dialect", default=DEFAULT_DIALECT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    payload = _extract_path(args.path, args.dialect)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


def _extract_path(path: Path, dialect: str) -> dict:
    if path.is_dir():
        return {
            "queries": [
                extract_sql_features(
                    sql_text=sql_path.read_text(encoding="utf-8"),
                    query_id=sql_path.stem,
                    dialect=dialect,
                )
                for sql_path in sorted(path.glob("*.sql"))
            ]
        }
    return extract_sql_features(
        sql_text=path.read_text(encoding="utf-8"),
        query_id=path.stem,
        dialect=dialect,
    )


if __name__ == "__main__":
    main()
