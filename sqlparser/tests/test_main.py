from __future__ import annotations

import json
from pathlib import Path

from sqlparser.main import DEFAULT_SQL_DIR, run_all_tpcds


def test_run_all_tpcds_writes_run_artifacts(tmp_path: Path) -> None:
    run_id = "test_run"

    summary = run_all_tpcds(
        sql_dir=DEFAULT_SQL_DIR,
        output_root=tmp_path,
        run_id=run_id,
    )

    output_dir = tmp_path / run_id
    assert summary["run_id"] == run_id
    assert summary["query_count"] == 105
    assert summary["success_count"] == 105
    assert summary["failed_count"] == 0
    assert summary["query_block_count"] > 105
    assert (output_dir / "manifest.json").is_file()
    assert (output_dir / "query_blocks.json").is_file()
    assert (output_dir / "query_to_qbs.json").is_file()
    assert (output_dir / "qb_to_query.json").is_file()
    assert (output_dir / "summary.json").is_file()
    assert len(list((output_dir / "queries").glob("*.json"))) == 105

    persisted_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert persisted_summary == summary
