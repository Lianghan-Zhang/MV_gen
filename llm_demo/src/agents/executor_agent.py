from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.agent_base import BaseAgent
from llm_demo.src.core.artifact_store import ArtifactStore


class ExecutorAgent(BaseAgent):
    def __init__(self, store: ArtifactStore) -> None:
        super().__init__(store=store, agent_name="ExecutorAgent")

    def materialize_mvs(
        self,
        batch_id: int,
        mv_candidates_path: str | Path,
        mv_build_sql_path: str | Path | None = None,
        materialized_mvs_path: str | Path | None = None,
    ) -> Path:
        started_at = time.monotonic()
        candidates_path = Path(mv_candidates_path)
        build_sql_path = Path(mv_build_sql_path) if mv_build_sql_path else self.store.path(f"04_batch_mvs/batch_{batch_id}_mv_build.sql")
        state_path = self._ensure_materialized_mvs(materialized_mvs_path)
        candidates = self.store.read_json(candidates_path)
        state = self.store.read_json(state_path)
        existing_mv_ids = {mv["mv_id"] for mv in state.get("materialized_mvs", [])}
        steps = self._load_execution_steps(batch_id)

        for candidate in candidates.get("mv_candidates", []):
            candidate_id = candidate["candidate_id"]
            mv_id = candidate.get("mv_id")
            missing_deps = [
                dependency
                for dependency in candidate.get("depends_on_mv_ids", [])
                if dependency not in existing_mv_ids
            ]
            if candidate["decision"] == "skip":
                status = "skipped"
                event = "mv_candidate_skipped"
                reason = candidate.get("reason", "decision=skip")
            elif missing_deps:
                status = "failed"
                event = "mv_materialize_failed"
                reason = f"missing dependencies: {', '.join(missing_deps)}"
            else:
                status = "success"
                event = "mv_materialize_success"
                reason = candidate.get("reason")
                if mv_id and mv_id not in existing_mv_ids:
                    state.setdefault("materialized_mvs", []).append(self._materialized_mv_record(candidate, batch_id, build_sql_path))
                    existing_mv_ids.add(mv_id)

            steps.append(
                {
                    "step_order": len(steps) + 1,
                    "step_type": "materialize_mv",
                    "status": status,
                    "candidate_id": candidate_id,
                    "mv_id": mv_id,
                    "sql_path": str(build_sql_path),
                    "reason": reason,
                    "depends_on_mv_ids": candidate.get("depends_on_mv_ids", []),
                }
            )
            self.store.append_run_log(
                agent_name=self.agent_name,
                event=event,
                input_artifact_paths=[candidates_path, build_sql_path],
                output_artifact_paths=[state_path, self._execution_order_path(batch_id)],
                elapsed_ms=self._elapsed_ms(started_at),
                batch_id=batch_id,
                candidate_id=candidate_id,
                error=reason if status == "failed" else None,
                details={"status": status, "mv_id": mv_id, "reason": reason},
            )

        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        self._write_execution_order(batch_id, steps)
        return state_path

    def run_queries(self, batch_id: int, final_rewrite_dir: str | Path, complexity_batches_path: str | Path) -> Path:
        started_at = time.monotonic()
        rewrite_dir = Path(final_rewrite_dir)
        batches_path = Path(complexity_batches_path)
        batch = self._find_batch(self.store.read_json(batches_path), batch_id)
        steps = self._load_execution_steps(batch_id)

        for query_id in batch.get("query_ids", []):
            sql_path = rewrite_dir / f"{query_id}_rewritten.sql"
            meta_path = rewrite_dir / f"{query_id}_rewrite_meta.json"
            if not sql_path.exists():
                raise FileNotFoundError(f"Missing rewritten SQL for {query_id}: {sql_path}")
            if not meta_path.exists():
                raise FileNotFoundError(f"Missing rewrite meta for {query_id}: {meta_path}")
            meta = self.store.read_json(meta_path)
            if "used_mv_ids" not in meta:
                raise ValueError(f"Rewrite meta for {query_id} missing used_mv_ids")
            used_mv_ids = meta["used_mv_ids"]
            steps.append(
                {
                    "step_order": len(steps) + 1,
                    "step_type": "run_query",
                    "status": "planned",
                    "query_id": query_id,
                    "sql_path": str(sql_path),
                    "meta_path": str(meta_path),
                    "reason": "dry-run query execution order",
                    "depends_on_mv_ids": used_mv_ids,
                }
            )
            self.store.append_run_log(
                agent_name=self.agent_name,
                event="query_execution_planned",
                input_artifact_paths=[sql_path, meta_path, batches_path],
                output_artifact_paths=[self._execution_order_path(batch_id)],
                elapsed_ms=self._elapsed_ms(started_at),
                batch_id=batch_id,
                details={
                    "query_id": query_id,
                    "sql_path": str(sql_path),
                    "meta_path": str(meta_path),
                    "used_mv_ids": used_mv_ids,
                },
            )

        return self._write_execution_order(batch_id, steps)

    def _ensure_materialized_mvs(self, materialized_mvs_path: str | Path | None) -> Path:
        path = Path(materialized_mvs_path) if materialized_mvs_path else self.store.path("04_batch_mvs/materialized_mvs.json")
        if not path.is_absolute():
            path = self.store.path(path)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{\n  "materialized_mvs": []\n}', encoding="utf-8")
        return path

    def _materialized_mv_record(self, candidate: dict[str, Any], batch_id: int, build_sql_path: Path) -> dict[str, Any]:
        return {
            "mv_id": candidate["mv_id"],
            "table_name": candidate.get("target_table_name") or candidate["mv_id"],
            "source_candidate_id": candidate["candidate_id"],
            "source_batch_id": candidate["source_batch_id"],
            "available_from_batch": batch_id,
            "family_id": candidate["family_id"],
            "target_queries": candidate.get("target_queries", []),
            "depends_on_mv_ids": candidate.get("depends_on_mv_ids", []),
            "mv_type": candidate.get("mv_type"),
            "group_by_exprs": candidate.get("group_by_exprs", []),
            "measure_exprs": candidate.get("measure_exprs", []),
            "output_columns": candidate.get("output_columns", []),
            "column_mappings": candidate.get("column_mappings", []),
            "build_sql_path": str(build_sql_path),
        }

    def _find_batch(self, complexity_batches: dict[str, Any], batch_id: int) -> dict[str, Any]:
        for batch in complexity_batches.get("complexity_batches", []):
            if batch.get("batch_id") == batch_id:
                return batch
        raise ValueError(f"Batch {batch_id} not found in complexity_batches")

    def _execution_order_path(self, batch_id: int) -> Path:
        return self.store.path(f"06_execution_logs/batch_{batch_id}_execution_order.json")

    def _load_execution_steps(self, batch_id: int) -> list[dict[str, Any]]:
        path = self._execution_order_path(batch_id)
        if not path.exists():
            return []
        return self.store.read_json(path).get("steps", [])

    def _write_execution_order(self, batch_id: int, steps: list[dict[str, Any]]) -> Path:
        path = self.store.write_json(
            f"06_execution_logs/batch_{batch_id}_execution_order.json",
            {
                "run_id": self.store.run_id,
                "batch_id": batch_id,
                "mode": "dry_run",
                "steps": steps,
            },
        )
        return path
