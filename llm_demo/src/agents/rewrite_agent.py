from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import RewriteOutput


class RewriteAgent(LLMRulesAgent):
    VALID_STAGES = {"historical", "final"}

    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="RewriteAgent")

    def run(
        self,
        batch_id: int,
        rewrite_stage: str,
        complexity_batches_path: str | Path,
        sql_manifest_path: str | Path,
        query_blocks_path: str | Path,
        materialized_mvs_path: str | Path | None = None,
    ) -> Path:
        started_at = time.monotonic()
        if rewrite_stage not in self.VALID_STAGES:
            raise ValueError(f"rewrite_stage must be one of {sorted(self.VALID_STAGES)}")

        batches_path = Path(complexity_batches_path)
        manifest_path = Path(sql_manifest_path)
        qb_path = Path(query_blocks_path)
        mv_state_path = self._ensure_materialized_mvs(materialized_mvs_path)

        complexity_batches = self.store.read_json(batches_path)
        current_batch = self._find_batch(complexity_batches, batch_id)
        materialized_mvs = self.store.read_json(mv_state_path)
        query_blocks = self.store.read_json(qb_path)
        queries = self._load_current_batch_queries(manifest_path, current_batch.get("query_ids", []))

        input_artifacts = {
            "rewrite_stage": rewrite_stage,
            "current_batch": current_batch,
            "queries": queries,
            **query_blocks,
            "materialized_mvs": materialized_mvs,
        }
        candidate_output = self._infer_structured(
            task="基于当前 batch 的 original SQL、QueryBlock 和 materialized_mvs 生成 candidate rewrite 输出。",
            context={"run_id": self.store.run_id, "batch_id": batch_id, "rewrite_stage": rewrite_stage},
            input_artifacts=input_artifacts,
            output_model=RewriteOutput,
        )
        output = self._infer_structured(
            task=(
                "evaluate candidate rewrite 输出：检查 query_id、rewrite_stage、used_mv_ids、fallback_reason "
                "和语义等价说明；返回修正后的完整 RewriteOutput。"
            ),
            context={"run_id": self.store.run_id, "batch_id": batch_id, "rewrite_stage": rewrite_stage},
            input_artifacts={**input_artifacts, "candidate_rewrite_output": candidate_output},
            output_model=RewriteOutput,
        )
        self._validate_output(output, current_batch, materialized_mvs, batch_id, rewrite_stage)

        rewrite_dir_relative = f"05_rewritten_sql/batch_{batch_id}/{rewrite_stage}_rewrite"
        rewrite_dir = self.store.ensure_dir(rewrite_dir_relative)
        output_paths = self._write_rewrites(output, rewrite_dir_relative, rewrite_dir, batch_id, rewrite_stage)

        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[batches_path, manifest_path, qb_path, mv_state_path],
            output_artifact_paths=output_paths,
            elapsed_ms=self._elapsed_ms(started_at),
            batch_id=batch_id,
            details={
                "rewrite_stage": rewrite_stage,
                "query_ids": current_batch.get("query_ids", []),
                "rewrite_statuses": {record["query_id"]: record["status"] for record in output["rewrites"]},
            },
        )
        return rewrite_dir

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

    def _load_current_batch_queries(self, manifest_path: Path, query_ids: list[str]) -> list[dict[str, str]]:
        manifest = self.store.read_json(manifest_path)
        by_id = {item["query_id"]: item for item in manifest.get("queries", [])}
        queries = []
        for query_id in query_ids:
            if query_id not in by_id:
                raise ValueError(f"Query {query_id} missing from sql manifest")
            sql_path = self._resolve_sql_path(by_id[query_id])
            queries.append(
                {
                    "query_id": query_id,
                    "original_sql_path": str(sql_path),
                    "original_sql": sql_path.read_text(encoding="utf-8"),
                }
            )
        return queries

    def _resolve_sql_path(self, manifest_item: dict[str, Any]) -> Path:
        sql_path = Path(manifest_item["sql_path"])
        if sql_path.is_file():
            return sql_path
        relative_path = manifest_item.get("sql_path_relative")
        if relative_path:
            project_relative_path = self.store.project_root / relative_path
            if project_relative_path.is_file():
                return project_relative_path
        raise FileNotFoundError(f"SQL file not found for query_id={manifest_item.get('query_id')}: {sql_path}")

    def _validate_output(
        self,
        output: dict[str, Any],
        current_batch: dict[str, Any],
        materialized_mvs: dict[str, Any],
        batch_id: int,
        rewrite_stage: str,
    ) -> None:
        expected_query_ids = set(current_batch.get("query_ids", []))
        actual_query_ids = {record["query_id"] for record in output.get("rewrites", [])}
        if actual_query_ids != expected_query_ids:
            raise ValueError(f"Rewrite output query_ids {actual_query_ids} do not match expected {expected_query_ids}")

        available_mv_ids = {
            mv["mv_id"]
            for mv in materialized_mvs.get("materialized_mvs", [])
            if mv.get("available_from_batch", batch_id) <= batch_id
        }
        for record in output["rewrites"]:
            if record["rewrite_stage"] != rewrite_stage:
                raise ValueError(f"Rewrite record {record['query_id']} has wrong rewrite_stage")
            if record["status"] == "fallback":
                if record.get("used_mv_ids"):
                    raise ValueError(f"Fallback rewrite {record['query_id']} must not use MV")
                if not record.get("fallback_reason"):
                    raise ValueError(f"Fallback rewrite {record['query_id']} must include fallback_reason")
            else:
                used_mv_ids = set(record.get("used_mv_ids", []))
                if not used_mv_ids:
                    raise ValueError(f"Rewritten query {record['query_id']} must include used_mv_ids")
                if not used_mv_ids.issubset(available_mv_ids):
                    raise ValueError(f"Rewritten query {record['query_id']} uses unavailable MV {used_mv_ids - available_mv_ids}")
                if record.get("fallback_reason") is not None:
                    raise ValueError(f"Rewritten query {record['query_id']} must have fallback_reason=null")

    def _write_rewrites(
        self,
        output: dict[str, Any],
        rewrite_dir_relative: str,
        rewrite_dir: Path,
        batch_id: int,
        rewrite_stage: str,
    ) -> list[Path]:
        output_paths: list[Path] = []
        for record in output["rewrites"]:
            query_id = record["query_id"]
            rewritten_sql_relative = f"{rewrite_dir_relative}/{query_id}_rewritten.sql"
            rewrite_meta_relative = f"{rewrite_dir_relative}/{query_id}_rewrite_meta.json"
            rewritten_sql_path = self.store.write_text(rewritten_sql_relative, record["rewritten_sql"])
            record["rewritten_sql_path"] = str(rewritten_sql_path)
            record["rewrite_meta_path"] = str(rewrite_dir / f"{query_id}_rewrite_meta.json")
            rewrite_meta_path = self.store.write_json(rewrite_meta_relative, record)
            output_paths.extend([rewritten_sql_path, rewrite_meta_path])
            self.store.append_run_log(
                agent_name=self.agent_name,
                event="rewrite_success" if record["status"] == "rewritten" else "rewrite_fallback",
                input_artifact_paths=[],
                output_artifact_paths=[rewritten_sql_path, rewrite_meta_path],
                elapsed_ms=0,
                batch_id=batch_id,
                details={
                    "query_id": query_id,
                    "rewrite_stage": rewrite_stage,
                    "status": record["status"],
                    "used_mv_ids": record.get("used_mv_ids", []),
                    "fallback_reason": record.get("fallback_reason"),
                },
            )
        return output_paths
