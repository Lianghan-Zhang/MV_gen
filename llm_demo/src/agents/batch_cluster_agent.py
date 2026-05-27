from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import BatchClusterOutput


class BatchClusterAgent(LLMRulesAgent):
    BATCH_BY_TYPE = {
        "join": 1,
        "join_filter": 2,
        "join_filter_groupby": 3,
        "other": 4,
    }
    TYPE_BY_BATCH = {batch_id: batch_type for batch_type, batch_id in BATCH_BY_TYPE.items()}
    COMPLEXITY_RANK = {
        "join": 1,
        "join_filter": 2,
        "join_filter_groupby": 3,
        "other": 4,
    }

    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="BatchClusterAgent")

    def run(self, query_blocks_path: str | Path, families_path: str | Path) -> Path:
        started_at = time.monotonic()
        qb_path = Path(query_blocks_path)
        family_path = Path(families_path)
        query_blocks_artifact = self.store.read_json(qb_path)
        query_blocks_dir = qb_path.parent
        query_to_qbs_path = query_blocks_dir / "query_to_qbs.json"
        qb_to_query_path = query_blocks_dir / "qb_to_query.json"
        query_to_qbs = self.store.read_json(query_to_qbs_path)
        qb_to_query = self.store.read_json(qb_to_query_path)
        families_artifact = self.store.read_json(family_path)

        input_artifacts = {
            **query_blocks_artifact,
            "query_to_qbs": query_to_qbs,
            "qb_to_query": qb_to_query,
            **families_artifact,
        }
        candidate_output = self._infer_structured(
            task="根据 QueryBlock、query_to_qbs 和 QueryFamily 生成 candidate_complexity_batches。",
            context={"run_id": self.store.run_id},
            input_artifacts=input_artifacts,
            output_model=BatchClusterOutput,
        )
        output = self._infer_structured(
            task=(
                "evaluate candidate_complexity_batches：检查每个 SQL 是否进入唯一 global batch，"
                "顶层 query_ids 是否去重，family_groups 是否遗漏或误分 QueryBlock；返回修正后的完整 BatchClusterOutput。"
            ),
            context={"run_id": self.store.run_id},
            input_artifacts={**input_artifacts, "candidate_complexity_batches": candidate_output},
            output_model=BatchClusterOutput,
        )
        output = self._normalize_batches(output)
        self._validate_batches(output, query_blocks_artifact["query_blocks"], query_to_qbs, families_artifact["query_families"])

        batches_path = self.store.write_json("03_batches/complexity_batches.json", output)
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[qb_path, query_to_qbs_path, qb_to_query_path, family_path],
            output_artifact_paths=[batches_path],
            elapsed_ms=self._elapsed_ms(started_at),
            details={
                "llm_stages": ["generate_candidate_complexity_batches", "evaluate_complexity_batches"],
                "batch_count": len(output["complexity_batches"]),
            },
        )
        return batches_path

    def _normalize_batches(self, output: dict[str, Any]) -> dict[str, Any]:
        by_id = {batch["batch_id"]: batch for batch in output.get("complexity_batches", [])}
        normalized = []
        for batch_id in (1, 2, 3, 4):
            batch = by_id.get(
                batch_id,
                {
                    "batch_id": batch_id,
                    "batch_type": self.TYPE_BY_BATCH[batch_id],
                    "query_ids": [],
                    "family_groups": [],
                },
            )
            batch["batch_id"] = batch_id
            batch["batch_type"] = self.TYPE_BY_BATCH[batch_id]
            batch["query_ids"] = self._dedupe(batch.get("query_ids", []))
            for group in batch.get("family_groups", []):
                group["query_ids"] = self._dedupe(group.get("query_ids", []))
                group["qb_ids"] = self._dedupe(group.get("qb_ids", []))
            normalized.append(batch)
        return {"complexity_batches": normalized}

    def _validate_batches(
        self,
        output: dict[str, Any],
        query_blocks: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
        query_families: list[dict[str, Any]],
    ) -> None:
        expected_batch_by_query = self._expected_batch_by_query(query_blocks, query_to_qbs)
        actual_batch_by_query: dict[str, int] = {}
        for batch in output["complexity_batches"]:
            for query_id in batch["query_ids"]:
                if query_id in actual_batch_by_query:
                    raise ValueError(f"Query {query_id} appears in multiple global batches")
                actual_batch_by_query[query_id] = batch["batch_id"]

        for query_id, expected_batch_id in expected_batch_by_query.items():
            if actual_batch_by_query.get(query_id) != expected_batch_id:
                raise ValueError(
                    f"Query {query_id} expected batch {expected_batch_id}, got {actual_batch_by_query.get(query_id)}"
                )

        family_members = {
            family["family_id"]: set(family.get("members", []))
            for family in query_families
        }
        qb_to_query = {block["qb_id"]: block["query_id"] for block in query_blocks}
        for batch in output["complexity_batches"]:
            batch_queries = set(batch["query_ids"])
            for group in batch.get("family_groups", []):
                family_id = group["family_id"]
                if family_id not in family_members:
                    raise ValueError(f"Unknown family_id in family_groups: {family_id}")
                if not set(group["qb_ids"]).issubset(family_members[family_id]):
                    raise ValueError(f"family_group {family_id} contains qb_ids outside its QueryFamily")
                derived_query_ids = {qb_to_query[qb_id] for qb_id in group["qb_ids"] if qb_id in qb_to_query}
                if set(group["query_ids"]) != derived_query_ids:
                    raise ValueError(f"family_group {family_id} query_ids do not match qb_ids")
                if not set(group["query_ids"]).issubset(batch_queries):
                    raise ValueError(f"family_group {family_id} query_ids are outside batch query_ids")

    def _expected_batch_by_query(
        self,
        query_blocks: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
    ) -> dict[str, int]:
        block_by_id = {block["qb_id"]: block for block in query_blocks}
        expected: dict[str, int] = {}
        for query_id, qb_ids in query_to_qbs.items():
            complexity_type = "join"
            for qb_id in qb_ids:
                block_type = block_by_id[qb_id]["complexity_type"]
                if self.COMPLEXITY_RANK[block_type] > self.COMPLEXITY_RANK[complexity_type]:
                    complexity_type = block_type
            expected[query_id] = self.BATCH_BY_TYPE[complexity_type]
        return expected

    def _dedupe(self, values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))
