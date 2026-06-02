from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


def default_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


class ArtifactStore:
    def __init__(
        self,
        run_id: str,
        artifact_root: str | Path | None = None,
    ) -> None:
        self.project_root = default_project_root()
        self.run_id = run_id
        self.artifact_root = Path(artifact_root) if artifact_root else self.project_root / "llm_demo" / "artifacts"
        self.run_dir = self.artifact_root / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

    @property
    def run_log_path(self) -> Path:
        return self.run_dir / "06_execution_logs" / "run_log.jsonl"

    @property
    def sql_manifest_path(self) -> Path:
        return self.path("00_raw_sql/sql_manifest.json")

    @property
    def query_blocks_path(self) -> Path:
        return self.path("01_query_blocks/query_blocks.json")

    @property
    def query_to_qbs_path(self) -> Path:
        return self.path("01_query_blocks/query_to_qbs.json")

    @property
    def qb_to_query_path(self) -> Path:
        return self.path("01_query_blocks/qb_to_query.json")

    @property
    def feature_status_path(self) -> Path:
        return self.path("01_query_blocks/feature_extract_status.json")

    @property
    def family_candidates_path(self) -> Path:
        return self.path("02_families/family_candidates.json")

    @property
    def query_families_path(self) -> Path:
        return self.path("02_families/query_families.json")

    @property
    def complexity_batches_path(self) -> Path:
        return self.path("03_batches/complexity_batches.json")

    @property
    def materialized_mvs_path(self) -> Path:
        return self.path("04_batch_mvs/materialized_mvs.json")

    @property
    def coverage_summary_path(self) -> Path:
        return self.path("08_coverage/coverage_summary.json")

    def batch_mv_candidates_path(self, batch_id: int) -> Path:
        return self.path(f"04_batch_mvs/batch_{batch_id}_mv_candidates.json")

    def batch_mv_build_sql_path(self, batch_id: int) -> Path:
        return self.path(f"04_batch_mvs/batch_{batch_id}_mv_build.sql")

    def rewrite_dir(self, batch_id: int, rewrite_stage: str) -> Path:
        return self.path(f"05_rewritten_sql/batch_{batch_id}/{rewrite_stage}_rewrite")

    def execution_order_path(self, batch_id: int) -> Path:
        return self.path(f"06_execution_logs/batch_{batch_id}_execution_order.json")

    def path(self, relative_path: str | Path) -> Path:
        return self.run_dir / Path(relative_path)

    def ensure_dir(self, relative_path: str | Path) -> Path:
        path = self.path(relative_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_text(self, relative_path: str | Path, text: str) -> Path:
        path = self.path(relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def read_text(self, path: str | Path) -> str:
        return Path(path).read_text(encoding="utf-8")

    def write_json(self, relative_path: str | Path, data: dict[str, Any] | list[Any]) -> Path:
        path = self.path(relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def read_json(self, path: str | Path) -> Any:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def append_run_log(
        self,
        *,
        agent_name: str,
        event: str,
        input_artifact_paths: list[str | Path],
        output_artifact_paths: list[str | Path],
        elapsed_ms: int,
        batch_id: int | None = None,
        candidate_id: str | None = None,
        error: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.run_log_path.parent.mkdir(parents=True, exist_ok=True)
        scope = f"batch_{batch_id}" if batch_id is not None else "global"
        event_id = f"{self.run_id}:{agent_name}:{scope}:{self._next_event_seq():04d}"
        record = {
            "run_id": self.run_id,
            "event_id": event_id,
            "agent_name": agent_name,
            "batch_id": batch_id,
            "candidate_id": candidate_id,
            "input_artifact_paths": [str(path) for path in input_artifact_paths],
            "output_artifact_paths": [str(path) for path in output_artifact_paths],
            "elapsed_ms": elapsed_ms,
            "event": event,
            "error": error,
            "details": details or {},
            "created_at_ms": int(time.time() * 1000),
        }
        with self.run_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def _next_event_seq(self) -> int:
        if not self.run_log_path.exists():
            return 1
        with self.run_log_path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip()) + 1
