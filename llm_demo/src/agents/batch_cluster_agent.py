from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.family_utils import normalize_query_families
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
        families_artifact, family_normalization_events = self._normalize_family_artifact(families_artifact, family_path)

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
        corrected_query_ids = self._batch_corrections(output, query_blocks_artifact["query_blocks"], query_to_qbs)
        output = self._canonicalize_batches(
            query_blocks=query_blocks_artifact["query_blocks"],
            query_to_qbs=query_to_qbs,
            query_families=families_artifact["query_families"],
        )
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
                "corrected_query_ids": corrected_query_ids,
                "family_normalization_events": family_normalization_events,
            },
        )
        return batches_path

    def _normalize_family_artifact(
        self,
        families_artifact: dict[str, Any],
        family_path: Path,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        normalized_families, events = normalize_query_families(families_artifact.get("query_families", []))
        if not events:
            return families_artifact, []

        normalized_artifact = {**families_artifact, "query_families": normalized_families}
        family_path.parent.mkdir(parents=True, exist_ok=True)
        family_path.write_text(json.dumps(normalized_artifact, ensure_ascii=False, indent=2), encoding="utf-8")
        return normalized_artifact, events

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

    def _canonicalize_batches(
        self,
        *,
        query_blocks: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
        query_families: list[dict[str, Any]],
    ) -> dict[str, Any]:
        batch_by_query = self._expected_batch_by_query(query_blocks, query_to_qbs)
        block_by_id = {block["qb_id"]: block for block in query_blocks}
        batches = [
            {
                "batch_id": batch_id,
                "batch_type": self.TYPE_BY_BATCH[batch_id],
                "query_ids": [],
                "family_groups": [],
            }
            for batch_id in (1, 2, 3, 4)
        ]
        batches_by_id = {batch["batch_id"]: batch for batch in batches}

        for query_id in query_to_qbs:
            batch_id = batch_by_query.get(query_id, 4)
            batches_by_id[batch_id]["query_ids"].append(query_id)

        grouped_qbs: dict[tuple[int, str], list[str]] = {}
        for family in query_families:
            family_id = family["family_id"]
            for qb_id in family.get("members", []):
                block = block_by_id.get(qb_id)
                if not block or block.get("unsupported_reasons"):
                    continue
                query_id = block["query_id"]
                batch_id = batch_by_query.get(query_id, 4)
                grouped_qbs.setdefault((batch_id, family_id), []).append(qb_id)

        for (batch_id, family_id), qb_ids in sorted(grouped_qbs.items()):
            deduped_qb_ids = self._dedupe(qb_ids)
            query_ids = self._dedupe([block_by_id[qb_id]["query_id"] for qb_id in deduped_qb_ids])
            batches_by_id[batch_id]["family_groups"].append(
                {
                    "family_id": family_id,
                    "query_ids": query_ids,
                    "qb_ids": deduped_qb_ids,
                }
            )

        for batch in batches:
            batch["query_ids"] = self._dedupe(batch["query_ids"])
        return {"complexity_batches": batches}

    def _batch_corrections(
        self,
        output: dict[str, Any],
        query_blocks: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
    ) -> dict[str, dict[str, int | None]]:
        canonical_batch_by_query = self._expected_batch_by_query(query_blocks, query_to_qbs)
        llm_batch_by_query: dict[str, int] = {}
        for batch in output.get("complexity_batches", []):
            for query_id in batch.get("query_ids", []):
                llm_batch_by_query[query_id] = batch["batch_id"]
        corrections: dict[str, dict[str, int | None]] = {}
        for query_id, canonical_batch_id in canonical_batch_by_query.items():
            llm_batch_id = llm_batch_by_query.get(query_id)
            if llm_batch_id != canonical_batch_id:
                corrections[query_id] = {
                    "llm_batch": llm_batch_id,
                    "canonical_batch": canonical_batch_id,
                }
        return corrections

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
        unsupported_qb_ids = {block["qb_id"] for block in query_blocks if block.get("unsupported_reasons")}
        for batch in output["complexity_batches"]:
            batch_queries = set(batch["query_ids"])
            for group in batch.get("family_groups", []):
                family_id = group["family_id"]
                if family_id not in family_members:
                    raise ValueError(f"Unknown family_id in family_groups: {family_id}")
                if not set(group["qb_ids"]).issubset(family_members[family_id]):
                    raise ValueError(f"family_group {family_id} contains qb_ids outside its QueryFamily")
                if set(group["qb_ids"]) & unsupported_qb_ids:
                    raise ValueError(f"family_group {family_id} contains unsupported QueryBlock")
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
            complexity_type = "other"
            for qb_id in qb_ids:
                block = block_by_id[qb_id]
                if block.get("unsupported_reasons"):
                    continue
                block_type = self._canonical_block_complexity(block)
                if block_type == "other":
                    continue
                if complexity_type == "other" or self.COMPLEXITY_RANK[block_type] > self.COMPLEXITY_RANK[complexity_type]:
                    complexity_type = block_type
            expected[query_id] = self.BATCH_BY_TYPE[complexity_type]
        return expected

    def _canonical_block_complexity(self, block: dict[str, Any]) -> str:
        has_join = bool(block.get("join_edges"))
        has_filter = bool(block.get("predicates"))
        has_aggregate = bool(block.get("group_by_exprs") or block.get("aggregate_exprs"))
        if has_join and has_aggregate:
            return "join_filter_groupby"
        if has_join and has_filter:
            return "join_filter"
        if has_join:
            return "join"
        return "other"

    def _dedupe(self, values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))
