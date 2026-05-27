from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import BatchMVOutput


class BatchMVAgent(LLMRulesAgent):
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="BatchMVAgent")

    def _rules_name(self) -> str:
        return "batch_mv_agent"

    def run(
        self,
        batch_id: int,
        complexity_batches_path: str | Path,
        query_blocks_path: str | Path,
        families_path: str | Path,
        historical_rewrite_dir: str | Path,
        materialized_mvs_path: str | Path | None = None,
    ) -> Path:
        started_at = time.monotonic()
        batches_path = Path(complexity_batches_path)
        qb_path = Path(query_blocks_path)
        family_path = Path(families_path)
        rewrite_dir = Path(historical_rewrite_dir)
        mv_state_path = self._ensure_materialized_mvs(materialized_mvs_path)

        complexity_batches = self.store.read_json(batches_path)
        current_batch = self._find_batch(complexity_batches, batch_id)
        query_blocks = self.store.read_json(qb_path)
        query_to_qbs = self.store.read_json(qb_path.parent / "query_to_qbs.json")
        qb_to_query = self.store.read_json(qb_path.parent / "qb_to_query.json")
        families = self.store.read_json(family_path)
        materialized_mvs = self.store.read_json(mv_state_path)
        historical_rewrites = self._load_historical_rewrites(rewrite_dir)

        input_artifacts = {
            "current_batch": current_batch,
            "historical_rewrites": historical_rewrites,
            **query_blocks,
            "query_to_qbs": query_to_qbs,
            "qb_to_query": qb_to_query,
            **families,
            "materialized_mvs": materialized_mvs,
            "complexity_batches": complexity_batches["complexity_batches"],
        }
        candidate_output = self._infer_structured(
            task="基于当前 batch 的 historical rewrite SQL、QueryBlock、QueryFamily 和 materialized_mvs 生成 candidate_mv_output。",
            context={"run_id": self.store.run_id, "batch_id": batch_id},
            input_artifacts=input_artifacts,
            output_model=BatchMVOutput,
        )
        output = self._infer_structured(
            task=(
                "evaluate candidate_mv_output：检查当前 batch 边界、family 边界、shared upstream superset MV、"
                "depends_on_mv_ids、build_sql 和 decision；返回修正后的完整 BatchMVOutput。"
            ),
            context={"run_id": self.store.run_id, "batch_id": batch_id},
            input_artifacts={**input_artifacts, "candidate_mv_output": candidate_output},
            output_model=BatchMVOutput,
        )
        self._validate_output(output, current_batch, families["query_families"], materialized_mvs, batch_id)

        mv_candidates_path = self.store.write_json(f"04_batch_mvs/batch_{batch_id}_mv_candidates.json", output)
        mv_build_path = self.store.write_text(
            f"04_batch_mvs/batch_{batch_id}_mv_build.sql",
            self._render_build_sql(output),
        )
        candidate_ids = [candidate["candidate_id"] for candidate in output["mv_candidates"]]
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[batches_path, qb_path, family_path, rewrite_dir, mv_state_path],
            output_artifact_paths=[mv_candidates_path, mv_build_path],
            elapsed_ms=self._elapsed_ms(started_at),
            batch_id=batch_id,
            details={
                "llm_stages": ["generate_candidate_mv_output", "evaluate_mv_output"],
                "candidate_count": len(candidate_ids),
                "candidate_ids": candidate_ids,
            },
        )
        self._append_candidate_logs(output, mv_candidates_path, mv_build_path, batch_id)
        return mv_candidates_path

    def _ensure_materialized_mvs(self, materialized_mvs_path: str | Path | None) -> Path:
        path = Path(materialized_mvs_path) if materialized_mvs_path else self.store.path("04_batch_mvs/materialized_mvs.json")
        if not path.is_absolute():
            path = self.store.path(path)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{\n  "materialized_mvs": []\n}', encoding="utf-8")
        return path

    def _find_batch(self, complexity_batches: dict[str, Any], batch_id: int) -> dict[str, Any]:
        for batch in complexity_batches.get("complexity_batches", []):
            if batch.get("batch_id") == batch_id:
                return batch
        raise ValueError(f"Batch {batch_id} not found in complexity_batches")

    def _load_historical_rewrites(self, rewrite_dir: Path) -> list[dict[str, Any]]:
        if not rewrite_dir.is_dir():
            raise FileNotFoundError(f"Historical rewrite dir not found: {rewrite_dir}")
        rewrites = []
        for sql_path in sorted(rewrite_dir.glob("*_rewritten.sql")):
            query_id = sql_path.name.removesuffix("_rewritten.sql")
            meta_path = rewrite_dir / f"{query_id}_rewrite_meta.json"
            meta = self.store.read_json(meta_path) if meta_path.exists() else {}
            rewrites.append(
                {
                    "query_id": query_id,
                    "rewritten_sql_path": str(sql_path),
                    "rewritten_sql": sql_path.read_text(encoding="utf-8"),
                    "rewrite_meta": meta,
                }
            )
        if not rewrites:
            raise ValueError(f"No historical rewritten SQL files found in {rewrite_dir}")
        return rewrites

    def _validate_output(
        self,
        output: dict[str, Any],
        current_batch: dict[str, Any],
        query_families: list[dict[str, Any]],
        materialized_mvs: dict[str, Any],
        batch_id: int,
    ) -> None:
        if output["batch_id"] != batch_id:
            raise ValueError(f"BatchMVOutput batch_id {output['batch_id']} does not match {batch_id}")

        batch_queries = set(current_batch.get("query_ids", []))
        family_ids = {family["family_id"] for family in query_families}
        current_batch_family_ids = {group["family_id"] for group in current_batch.get("family_groups", [])}
        available_mv_ids = {
            mv["mv_id"]
            for mv in materialized_mvs.get("materialized_mvs", [])
            if mv.get("available_from_batch", batch_id) <= batch_id
        }
        seen_candidate_ids: set[str] = set()
        for candidate in output["mv_candidates"]:
            candidate_id = candidate["candidate_id"]
            if candidate_id in seen_candidate_ids:
                raise ValueError(f"Duplicate candidate_id: {candidate_id}")
            seen_candidate_ids.add(candidate_id)

            if candidate["source_batch_id"] != batch_id:
                raise ValueError(f"Candidate {candidate_id} source_batch_id must be {batch_id}")
            if candidate["family_id"] not in family_ids:
                raise ValueError(f"Candidate {candidate_id} has unknown family_id {candidate['family_id']}")
            if current_batch_family_ids and candidate["family_id"] not in current_batch_family_ids:
                raise ValueError(f"Candidate {candidate_id} family_id must belong to current batch family_groups")
            if not candidate["source_query_ids"]:
                raise ValueError(f"Candidate {candidate_id} source_query_ids cannot be empty")
            if not candidate["target_queries"]:
                raise ValueError(f"Candidate {candidate_id} target_queries cannot be empty")
            if not set(candidate["source_query_ids"]).issubset(batch_queries):
                raise ValueError(f"Candidate {candidate_id} source_query_ids must belong to current batch")
            if not set(candidate["target_queries"]).issubset(batch_queries):
                raise ValueError(f"Candidate {candidate_id} target_queries must belong to current batch")

            mv_id = candidate.get("mv_id")
            for dependency in candidate.get("depends_on_mv_ids", []):
                if dependency == mv_id:
                    raise ValueError(f"Candidate {candidate_id} cannot depend on itself")
                if dependency not in available_mv_ids:
                    raise ValueError(f"Candidate {candidate_id} depends on unavailable MV {dependency}")

            if candidate["decision"] == "materialize":
                missing = [
                    field
                    for field in ("mv_id", "target_table_name", "build_sql")
                    if not candidate.get(field)
                ]
                if missing:
                    raise ValueError(f"Candidate {candidate_id} decision=materialize missing {missing}")

    def _render_build_sql(self, output: dict[str, Any]) -> str:
        statements = []
        for candidate in output["mv_candidates"]:
            if candidate["decision"] == "materialize" and candidate.get("build_sql"):
                statements.append(f"-- {candidate['candidate_id']}\n{candidate['build_sql'].rstrip()}")
        if not statements:
            return "-- No materialized MV candidates for this batch.\n"
        return "\n\n".join(statements) + "\n"

    def _append_candidate_logs(self, output: dict[str, Any], mv_candidates_path: Path, mv_build_path: Path, batch_id: int) -> None:
        for candidate in output["mv_candidates"]:
            event = "mv_candidate_skipped" if candidate["decision"] == "skip" else "mv_candidate_generated"
            self.store.append_run_log(
                agent_name=self.agent_name,
                event=event,
                input_artifact_paths=[mv_candidates_path],
                output_artifact_paths=[mv_build_path],
                elapsed_ms=0,
                batch_id=batch_id,
                candidate_id=candidate["candidate_id"],
                details={
                    "decision": candidate["decision"],
                    "mv_id": candidate.get("mv_id"),
                    "source_query_ids": candidate["source_query_ids"],
                    "target_queries": candidate["target_queries"],
                    "reason": candidate["reason"],
                },
            )
