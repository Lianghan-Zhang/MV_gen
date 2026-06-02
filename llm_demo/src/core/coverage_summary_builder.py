from __future__ import annotations

from collections import Counter
import json
import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.artifact_store import ArtifactStore


class CoverageSummaryBuilder:
    def __init__(self, store: ArtifactStore) -> None:
        self.store = store

    def run(self) -> Path:
        started_at = time.monotonic()
        summary = {
            "run_id": self.store.run_id,
            "total_queries": self._total_queries(),
            "feature_status_counts": self._feature_status_counts(),
            "family_candidate_count": self._count_items(self.store.family_candidates_path, "family_candidates"),
            "family_count": self._count_items(self.store.query_families_path, "query_families"),
            "batch_query_counts": self._batch_query_counts(),
            "mv_candidate_counts": self._mv_candidate_counts(),
            "rewrite_status_counts": self._rewrite_status_counts(),
            "execution_step_counts": self._execution_step_counts(),
            "notes": self._notes(),
        }
        path = self.store.write_json("08_coverage/coverage_summary.json", summary)
        self.store.append_run_log(
            agent_name="CoverageSummaryBuilder",
            event="success",
            input_artifact_paths=self._existing_inputs(),
            output_artifact_paths=[path],
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            details={"total_queries": summary["total_queries"]},
        )
        return path

    def _total_queries(self) -> int:
        if not self.store.sql_manifest_path.exists():
            return 0
        return len(self.store.read_json(self.store.sql_manifest_path).get("queries", []))

    def _feature_status_counts(self) -> dict[str, int]:
        if not self.store.feature_status_path.exists():
            return {}
        statuses = self.store.read_json(self.store.feature_status_path).get("queries", [])
        return dict(Counter(item.get("status", "unknown") for item in statuses))

    def _count_items(self, path: Path, key: str) -> int:
        if not path.exists():
            return 0
        return len(self.store.read_json(path).get(key, []))

    def _batch_query_counts(self) -> dict[str, int]:
        if not self.store.complexity_batches_path.exists():
            return {}
        batches = self.store.read_json(self.store.complexity_batches_path).get("complexity_batches", [])
        return {str(batch["batch_id"]): len(batch.get("query_ids", [])) for batch in batches}

    def _mv_candidate_counts(self) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for path in sorted(self.store.run_dir.glob("04_batch_mvs/batch_*_mv_candidates.json")):
            data = self.store.read_json(path)
            for candidate in data.get("mv_candidates", []):
                counts[candidate.get("decision", "unknown")] += 1
        return dict(counts)

    def _rewrite_status_counts(self) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for path in sorted(self.store.run_dir.glob("05_rewritten_sql/batch_*/*_rewrite/*_rewrite_meta.json")):
            meta = self.store.read_json(path)
            counts[f"{meta.get('rewrite_stage', 'unknown')}:{meta.get('status', 'unknown')}"] += 1
        return dict(counts)

    def _execution_step_counts(self) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for path in sorted(self.store.run_dir.glob("06_execution_logs/batch_*_execution_order.json")):
            data = self.store.read_json(path)
            for step in data.get("steps", []):
                counts[f"{step.get('step_type', 'unknown')}:{step.get('status', 'unknown')}"] += 1
        return dict(counts)

    def _notes(self) -> list[str]:
        notes = []
        if self.store.feature_status_path.exists():
            failed = [
                item["query_id"]
                for item in self.store.read_json(self.store.feature_status_path).get("queries", [])
                if item.get("status") in {"feature_failed", "unsupported_sql_pattern"}
            ]
            if failed:
                notes.append(f"feature_failed_or_unsupported_queries={len(failed)}")
        if self.store.run_log_path.exists():
            errors = [
                json.loads(line)
                for line in self.store.run_log_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and json.loads(line).get("error")
            ]
            if errors:
                notes.append(f"run_log_error_events={len(errors)}")
        return notes

    def _existing_inputs(self) -> list[Path]:
        paths: list[Path] = []
        for path in (
            self.store.sql_manifest_path,
            self.store.feature_status_path,
            self.store.family_candidates_path,
            self.store.query_families_path,
            self.store.complexity_batches_path,
            self.store.run_log_path,
        ):
            if path.exists():
                paths.append(path)
        paths.extend(sorted(self.store.run_dir.glob("04_batch_mvs/batch_*_mv_candidates.json")))
        paths.extend(sorted(self.store.run_dir.glob("05_rewritten_sql/batch_*/*_rewrite/*_rewrite_meta.json")))
        paths.extend(sorted(self.store.run_dir.glob("06_execution_logs/batch_*_execution_order.json")))
        return paths
