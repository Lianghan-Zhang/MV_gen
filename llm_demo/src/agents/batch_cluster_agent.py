from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

import sqlglot

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.family_utils import normalize_query_families
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import BatchClusterOutput
from llm_demo.src.core.sql_utils import SQL_DIALECT


class BatchClusterAgent(LLMRulesAgent):
    CSV_BATCH_LABELS = ("Batch-1", "Batch-2", "Batch-3", "Batch-4", "Batch-5", "Other")
    CSV_BATCH_ID_BY_LABEL = {label: index + 1 for index, label in enumerate(CSV_BATCH_LABELS)}

    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="BatchClusterAgent")

    def run_from_classification_csv(
        self,
        *,
        classification_csv_path: str | Path,
        sql_manifest_path: str | Path,
        query_blocks_path: str | Path,
        families_path: str | Path | None = None,
    ) -> Path:
        started_at = time.monotonic()
        csv_path = Path(classification_csv_path)
        manifest_path = Path(sql_manifest_path)
        qb_path = Path(query_blocks_path)
        family_path = Path(families_path) if families_path else None

        manifest = self.store.read_json(manifest_path).get("queries", [])
        query_blocks_artifact = self.store.read_json(qb_path)
        query_blocks_dir = qb_path.parent
        query_to_qbs_path = query_blocks_dir / "query_to_qbs.json"
        qb_to_query_path = query_blocks_dir / "qb_to_query.json"
        query_to_qbs = self.store.read_json(query_to_qbs_path)
        qb_to_query = self.store.read_json(qb_to_query_path)
        if family_path is not None:
            families_artifact = self.store.read_json(family_path)
            families_artifact, family_normalization_events = self._normalize_family_artifact(families_artifact, family_path)
        else:
            families_artifact = {"query_families": []}
            family_normalization_events = []

        output, details = self._build_batches_from_classification_csv(
            classification_csv_path=csv_path,
            manifest=manifest,
            query_blocks=query_blocks_artifact.get("query_blocks", []),
            query_to_qbs=query_to_qbs,
            query_families=families_artifact.get("query_families", []),
        )
        self._validate_csv_batches(output, manifest, query_to_qbs, families_artifact.get("query_families", []))

        batches_path = self.store.write_json("03_batches/complexity_batches.json", output)
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[
                path
                for path in [csv_path, manifest_path, qb_path, query_to_qbs_path, qb_to_query_path, family_path]
                if path is not None
            ],
            output_artifact_paths=[batches_path],
            elapsed_ms=self._elapsed_ms(started_at),
            details={
                "mode": "classification_csv",
                "batch_count": len(output["complexity_batches"]),
                "batch_counts": {
                    batch["batch_type"]: len(batch.get("query_ids", []))
                    for batch in output["complexity_batches"]
                },
                "missing_feature_query_ids": details["missing_feature_query_ids"],
                "sqlglot_parsed_query_count": details["sqlglot_parsed_query_count"],
                "family_normalization_events": family_normalization_events,
            },
        )
        return batches_path

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
            task="根据 BatchClusterAgent 规则、QueryBlock、query_to_qbs、qb_to_query 和 QueryFamily 生成 candidate_complexity_batches。",
            context={"run_id": self.store.run_id, "batch_labels": list(self.CSV_BATCH_LABELS)},
            input_artifacts=input_artifacts,
            output_model=BatchClusterOutput,
        )
        output = self._infer_structured(
            task=(
                "evaluate candidate_complexity_batches：严格依据 BatchClusterAgent 规则检查每个 SQL 是否进入唯一 global batch，"
                "batch_type 是否为 Batch-1/Batch-2/Batch-3/Batch-4/Batch-5/Other，"
                "顶层 query_ids 是否去重，family_groups 是否遗漏或误分 QueryBlock；返回修正后的完整 BatchClusterOutput。"
            ),
            context={"run_id": self.store.run_id, "batch_labels": list(self.CSV_BATCH_LABELS)},
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

    def _build_batches_from_classification_csv(
        self,
        *,
        classification_csv_path: Path,
        manifest: list[dict[str, Any]],
        query_blocks: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
        query_families: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        manifest_query_ids = {query["query_id"] for query in manifest}
        manifest_by_sql_name = self._manifest_by_sql_name(manifest)
        parsed_sql_by_query_id: dict[str, Any] = {}
        batch_rows: dict[str, list[str]] = {label: [] for label in self.CSV_BATCH_LABELS}
        seen_query_ids: set[str] = set()
        duplicate_query_ids: list[str] = []

        with classification_csv_path.open(encoding="utf-8-sig", newline="") as handle:
            classification_rows = list(csv.DictReader(handle))
        required_columns = {"SQL", "Batch", "Reason"}
        if not classification_rows or not required_columns.issubset(classification_rows[0].keys()):
            raise ValueError(f"CSV must contain columns {sorted(required_columns)}")

        for row in classification_rows:
            batch_label = row["Batch"].strip()
            if batch_label not in self.CSV_BATCH_ID_BY_LABEL:
                raise ValueError(f"Unsupported batch label in CSV: {batch_label}")
            query_id = self._csv_query_id(row["SQL"], manifest_by_sql_name, parsed_sql_by_query_id)
            if query_id in seen_query_ids:
                duplicate_query_ids.append(query_id)
            seen_query_ids.add(query_id)
            batch_rows[batch_label].append(query_id)

        unknown_query_ids = sorted(seen_query_ids - manifest_query_ids)
        missing_from_csv = sorted(manifest_query_ids - seen_query_ids)
        if duplicate_query_ids or unknown_query_ids or missing_from_csv:
            raise ValueError(
                {
                    "duplicate_query_ids": duplicate_query_ids,
                    "unknown_query_ids": unknown_query_ids,
                    "missing_from_csv": missing_from_csv,
                }
            )

        block_by_id = {block["qb_id"]: block for block in query_blocks}
        family_by_qb = self._family_by_qb(query_families)
        complexity_batches = []
        for label in self.CSV_BATCH_LABELS:
            query_ids = batch_rows[label]
            complexity_batches.append(
                {
                    "batch_id": self.CSV_BATCH_ID_BY_LABEL[label],
                    "batch_type": label,
                    "query_ids": query_ids,
                    "family_groups": self._family_groups_for_query_ids(
                        query_ids=query_ids,
                        query_to_qbs=query_to_qbs,
                        block_by_id=block_by_id,
                        family_by_qb=family_by_qb,
                    ),
                }
            )

        return (
            {"complexity_batches": complexity_batches},
            {
                "missing_feature_query_ids": sorted(query_id for query_id in seen_query_ids if query_id not in query_to_qbs),
                "sqlglot_parsed_query_count": len(parsed_sql_by_query_id),
            },
        )

    def _manifest_by_sql_name(self, manifest: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        by_name: dict[str, dict[str, Any]] = {}
        for item in manifest:
            sql_path = self._resolve_manifest_sql_path(item)
            sql_name = sql_path.name
            if sql_name in by_name:
                raise ValueError(f"Duplicate SQL file name in manifest: {sql_name}")
            by_name[sql_name] = {**item, "resolved_sql_path": sql_path}
        return by_name

    def _resolve_manifest_sql_path(self, manifest_item: dict[str, Any]) -> Path:
        sql_path = Path(manifest_item["sql_path"])
        if sql_path.is_file():
            return sql_path
        relative_path = manifest_item.get("sql_path_relative")
        if relative_path:
            project_relative_path = self.store.project_root / relative_path
            if project_relative_path.is_file():
                return project_relative_path
        raise FileNotFoundError(f"SQL file not found for query_id={manifest_item.get('query_id')}: {sql_path}")

    def _csv_query_id(
        self,
        sql_name: str,
        manifest_by_sql_name: dict[str, dict[str, Any]],
        parsed_sql_by_query_id: dict[str, Any],
    ) -> str:
        sql_file_name = Path(sql_name.strip()).name
        manifest_item = manifest_by_sql_name.get(sql_file_name)
        if not manifest_item:
            raise ValueError(f"CSV SQL not found in manifest: {sql_name}")

        query_id = manifest_item["query_id"]
        if query_id not in parsed_sql_by_query_id:
            sql_path = manifest_item["resolved_sql_path"]
            sql_text = sql_path.read_text(encoding="utf-8")
            parsed_sql_by_query_id[query_id] = sqlglot.parse_one(sql_text, dialect=SQL_DIALECT)
        return query_id

    def _family_by_qb(self, query_families: list[dict[str, Any]]) -> dict[str, str]:
        family_by_qb = {}
        for family in query_families:
            for qb_id in family.get("members", []):
                family_by_qb[qb_id] = family["family_id"]
        return family_by_qb

    def _family_groups_for_query_ids(
        self,
        *,
        query_ids: list[str],
        query_to_qbs: dict[str, list[str]],
        block_by_id: dict[str, dict[str, Any]],
        family_by_qb: dict[str, str],
    ) -> list[dict[str, Any]]:
        grouped_qbs: dict[str, list[str]] = {}
        query_id_set = set(query_ids)
        for query_id in query_ids:
            for qb_id in query_to_qbs.get(query_id, []):
                block = block_by_id.get(qb_id)
                family_id = family_by_qb.get(qb_id)
                if not block or not family_id or block.get("unsupported_reasons"):
                    continue
                grouped_qbs.setdefault(family_id, []).append(qb_id)

        family_groups = []
        for family_id, qb_ids in sorted(grouped_qbs.items()):
            deduped_qb_ids = self._dedupe(qb_ids)
            derived_query_id_set = {
                block_by_id[qb_id]["query_id"]
                for qb_id in deduped_qb_ids
                if qb_id in block_by_id
            } & query_id_set
            family_groups.append(
                {
                    "family_id": family_id,
                    "query_ids": [query_id for query_id in query_ids if query_id in derived_query_id_set],
                    "qb_ids": deduped_qb_ids,
                }
            )
        return family_groups

    def _validate_csv_batches(
        self,
        output: dict[str, Any],
        manifest: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
        query_families: list[dict[str, Any]],
    ) -> None:
        expected_query_ids = {query["query_id"] for query in manifest}
        actual_batch_by_query: dict[str, int] = {}
        for batch in output["complexity_batches"]:
            batch_id = batch["batch_id"]
            expected_label = self.CSV_BATCH_LABELS[batch_id - 1]
            if batch["batch_type"] != expected_label:
                raise ValueError(f"Batch {batch_id} must use batch_type {expected_label}")
            for query_id in batch["query_ids"]:
                if query_id in actual_batch_by_query:
                    raise ValueError(f"Query {query_id} appears in multiple global batches")
                actual_batch_by_query[query_id] = batch_id

        actual_query_ids = set(actual_batch_by_query)
        if actual_query_ids != expected_query_ids:
            raise ValueError(
                {
                    "missing_query_ids": sorted(expected_query_ids - actual_query_ids),
                    "unknown_query_ids": sorted(actual_query_ids - expected_query_ids),
                }
            )

        family_members = {
            family["family_id"]: set(family.get("members", []))
            for family in query_families
        }
        qb_to_query = {qb_id: query_id for query_id, qb_ids in query_to_qbs.items() for qb_id in qb_ids}
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

    def _normalize_batches(self, output: dict[str, Any]) -> dict[str, Any]:
        by_id = {batch["batch_id"]: batch for batch in output.get("complexity_batches", [])}
        by_type = {batch["batch_type"]: batch for batch in output.get("complexity_batches", [])}
        normalized = []
        for label in self.CSV_BATCH_LABELS:
            batch_id = self.CSV_BATCH_ID_BY_LABEL[label]
            batch = by_type.get(label) or by_id.get(batch_id) or {
                "batch_id": batch_id,
                "batch_type": label,
                "query_ids": [],
                "family_groups": [],
            }
            batch["batch_id"] = batch_id
            batch["batch_type"] = label
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
        expected_query_ids = set(query_to_qbs)
        actual_batch_by_query: dict[str, int] = {}
        for batch in output["complexity_batches"]:
            expected_label = self.CSV_BATCH_LABELS[batch["batch_id"] - 1]
            if batch["batch_type"] != expected_label:
                raise ValueError(f"Batch {batch['batch_id']} must use batch_type {expected_label}")
            for query_id in batch["query_ids"]:
                if query_id in actual_batch_by_query:
                    raise ValueError(f"Query {query_id} appears in multiple global batches")
                actual_batch_by_query[query_id] = batch["batch_id"]

        actual_query_ids = set(actual_batch_by_query)
        if actual_query_ids != expected_query_ids:
            raise ValueError(
                {
                    "missing_query_ids": sorted(expected_query_ids - actual_query_ids),
                    "unknown_query_ids": sorted(actual_query_ids - expected_query_ids),
                }
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

    def _dedupe(self, values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))
