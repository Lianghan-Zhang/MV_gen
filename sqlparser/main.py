from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlparser.extractor import DEFAULT_DIALECT, extract_sql_features, validate_sqlglot_version  # noqa: E402


SQLPARSER_ROOT = Path(__file__).resolve().parent
DEFAULT_SQL_DIR = PROJECT_ROOT / "tpcds-spark"
DEFAULT_OUTPUT_ROOT = SQLPARSER_ROOT / "test"


def main() -> None:
    validate_sqlglot_version()
    parser = argparse.ArgumentParser(description="Run sqlglot feature extraction for all TPC-DS Spark SQL files.")
    parser.add_argument("--sql-dir", type=Path, default=DEFAULT_SQL_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dialect", default=DEFAULT_DIALECT)
    args = parser.parse_args()

    result = run_all_tpcds(
        sql_dir=args.sql_dir,
        output_root=args.output_root,
        run_id=args.run_id,
        dialect=args.dialect,
    )
    print(json.dumps({"run_id": result["run_id"], "output_dir": result["output_dir"]}, ensure_ascii=False))
    if result["failed_queries"]:
        raise SystemExit(1)


def run_all_tpcds(
    *,
    sql_dir: Path = DEFAULT_SQL_DIR,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    dialect: str = DEFAULT_DIALECT,
) -> dict[str, Any]:
    validate_sqlglot_version()
    run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_dir = sql_dir.resolve()
    output_dir = output_root.resolve() / run_id
    queries_dir = output_dir / "queries"
    queries_dir.mkdir(parents=True, exist_ok=False)

    sql_files = sorted(sql_dir.glob("*.sql"))
    query_blocks: list[dict[str, Any]] = []
    query_to_qbs: dict[str, list[str]] = {}
    qb_to_query: dict[str, str] = {}
    manifest: list[dict[str, Any]] = []
    failed_queries: list[dict[str, str]] = []

    for sql_path in sql_files:
        query_id = sql_path.stem
        manifest.append(
            {
                "query_id": query_id,
                "sql_path": str(sql_path),
                "sql_path_relative": str(sql_path.relative_to(PROJECT_ROOT)) if sql_path.is_relative_to(PROJECT_ROOT) else str(sql_path),
                "size_bytes": sql_path.stat().st_size,
            }
        )
        try:
            output = extract_sql_features(
                sql_text=sql_path.read_text(encoding="utf-8"),
                query_id=query_id,
                dialect=dialect,
                project_root=PROJECT_ROOT,
            )
        except Exception as error:  # noqa: BLE001 - run artifact should preserve per-query failures.
            failed_queries.append(
                {
                    "query_id": query_id,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                }
            )
            continue

        (queries_dir / f"{query_id}.json").write_text(
            json.dumps(output, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        query_blocks.extend(output["query_blocks"])
        query_to_qbs[query_id] = output["query_to_qbs"][query_id]
        qb_to_query.update(output["qb_to_query"])

    summary = {
        "run_id": run_id,
        "dialect": dialect,
        "sql_dir": str(sql_dir),
        "output_dir": str(output_dir),
        "query_count": len(sql_files),
        "success_count": len(sql_files) - len(failed_queries),
        "failed_count": len(failed_queries),
        "query_block_count": len(query_blocks),
        "failed_queries": failed_queries,
    }

    _write_json(output_dir / "manifest.json", {"queries": manifest})
    _write_json(output_dir / "query_blocks.json", {"query_blocks": query_blocks})
    _write_json(output_dir / "query_to_qbs.json", query_to_qbs)
    _write_json(output_dir / "qb_to_query.json", qb_to_query)
    _write_json(output_dir / "summary.json", summary)

    return summary


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
