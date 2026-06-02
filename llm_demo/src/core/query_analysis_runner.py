from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from llm_demo.src.agents.feature_agent import FeatureAgent
from llm_demo.src.core.artifact_store import ArtifactStore


class QueryAnalysisRunner:
    def __init__(self, store: ArtifactStore, feature_agent: FeatureAgent) -> None:
        self.store = store
        self.feature_agent = feature_agent

    def run_all(self, sql_manifest_path: str | Path, resume_feature: bool = False) -> Path:
        started_at = time.monotonic()
        manifest_path = Path(sql_manifest_path)
        manifest = self.store.read_json(manifest_path)
        queries = manifest.get("queries", [])
        status_by_query = self._load_status_by_query() if resume_feature else {}
        feature_output = self._load_feature_output()

        failed_queries: list[dict[str, Any]] = []
        for query in queries:
            query_id = query["query_id"]
            existing_status = status_by_query.get(query_id, {})
            if resume_feature and existing_status.get("status") in {"success", "partial_success"}:
                continue
            status = self._extract_and_merge(query, feature_output, attempt=1)
            status_by_query[query_id] = status
            if status["status"] in {"feature_failed", "unsupported_sql_pattern"}:
                failed_queries.append(query)

        for query in failed_queries:
            query_id = query["query_id"]
            status = self._extract_and_merge(query, feature_output, attempt=2)
            status["attempts"] = 2
            status_by_query[query_id] = status

        self._write_feature_output(feature_output)
        status_path = self._write_status(status_by_query, queries)
        self.store.append_run_log(
            agent_name="QueryAnalysisRunner",
            event="success",
            input_artifact_paths=[manifest_path],
            output_artifact_paths=[self.store.query_blocks_path, self.store.query_to_qbs_path, self.store.qb_to_query_path, status_path],
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            details={
                "query_count": len(queries),
                "failed_after_retry": [
                    query_id
                    for query_id, status in status_by_query.items()
                    if status.get("status") in {"feature_failed", "unsupported_sql_pattern"}
                ],
            },
        )
        return self.store.query_blocks_path

    def _extract_and_merge(self, query: dict[str, Any], feature_output: dict[str, Any], attempt: int) -> dict[str, Any]:
        query_id = query["query_id"]
        try:
            output = self.feature_agent.extract_one(query, attempt=attempt)
            query_qbs = [block for block in output["query_blocks"] if block["query_id"] == query_id]
            usable_qbs = [block for block in query_qbs if not block.get("unsupported_reasons")]
            if usable_qbs and len(usable_qbs) == len(query_qbs):
                status = "success"
            elif usable_qbs:
                status = "partial_success"
            else:
                status = "unsupported_sql_pattern"

            if status in {"success", "partial_success"}:
                self._merge_output(query_id, output, feature_output)
            return {
                "query_id": query_id,
                "status": status,
                "attempts": attempt,
                "qb_ids": output["query_to_qbs"].get(query_id, []),
                "unsupported_reasons": self._unsupported_reasons(query_qbs),
                "error_type": None,
                "error_message": None,
            }
        except Exception as error:  # noqa: BLE001 - status artifact must preserve the concrete failure.
            self.store.append_run_log(
                agent_name="QueryAnalysisRunner",
                event="feature_extract_failed",
                input_artifact_paths=[query.get("sql_path", query.get("sql_path_relative", query_id))],
                output_artifact_paths=[],
                elapsed_ms=0,
                error=str(error),
                details={"query_id": query_id, "attempt": attempt, "error_type": type(error).__name__},
            )
            return {
                "query_id": query_id,
                "status": "feature_failed",
                "attempts": attempt,
                "qb_ids": [],
                "unsupported_reasons": [],
                "error_type": type(error).__name__,
                "error_message": str(error),
            }

    def _merge_output(self, query_id: str, output: dict[str, Any], feature_output: dict[str, Any]) -> None:
        old_qb_ids = set(feature_output["query_to_qbs"].get(query_id, []))
        feature_output["query_blocks"] = [
            block
            for block in feature_output["query_blocks"]
            if block["qb_id"] not in old_qb_ids and block["query_id"] != query_id
        ]
        for qb_id in old_qb_ids:
            feature_output["qb_to_query"].pop(qb_id, None)

        query_qb_ids = output["query_to_qbs"][query_id]
        feature_output["query_blocks"].extend(
            block
            for block in output["query_blocks"]
            if block["qb_id"] in query_qb_ids
        )
        feature_output["query_to_qbs"][query_id] = query_qb_ids
        for qb_id in query_qb_ids:
            feature_output["qb_to_query"][qb_id] = query_id

    def _load_feature_output(self) -> dict[str, Any]:
        if self.store.query_blocks_path.exists():
            query_blocks = self.store.read_json(self.store.query_blocks_path).get("query_blocks", [])
            query_to_qbs = self.store.read_json(self.store.query_to_qbs_path) if self.store.query_to_qbs_path.exists() else {}
            qb_to_query = self.store.read_json(self.store.qb_to_query_path) if self.store.qb_to_query_path.exists() else {}
            return {"query_blocks": query_blocks, "query_to_qbs": query_to_qbs, "qb_to_query": qb_to_query}
        return {"query_blocks": [], "query_to_qbs": {}, "qb_to_query": {}}

    def _write_feature_output(self, feature_output: dict[str, Any]) -> None:
        self.store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": feature_output["query_blocks"]})
        self.store.write_json("01_query_blocks/query_to_qbs.json", feature_output["query_to_qbs"])
        self.store.write_json("01_query_blocks/qb_to_query.json", feature_output["qb_to_query"])

    def _load_status_by_query(self) -> dict[str, dict[str, Any]]:
        if not self.store.feature_status_path.exists():
            return {}
        status = self.store.read_json(self.store.feature_status_path)
        return {item["query_id"]: item for item in status.get("queries", [])}

    def _write_status(self, status_by_query: dict[str, dict[str, Any]], queries: list[dict[str, Any]]) -> Path:
        ordered_statuses = [
            status_by_query.get(
                query["query_id"],
                {
                    "query_id": query["query_id"],
                    "status": "feature_failed",
                    "attempts": 0,
                    "qb_ids": [],
                    "unsupported_reasons": [],
                    "error_type": "NotProcessed",
                    "error_message": "query was not processed",
                },
            )
            for query in queries
        ]
        return self.store.write_json(
            "01_query_blocks/feature_extract_status.json",
            {"run_id": self.store.run_id, "queries": ordered_statuses},
        )

    def _unsupported_reasons(self, query_blocks: list[dict[str, Any]]) -> list[str]:
        reasons: list[str] = []
        for block in query_blocks:
            reasons.extend(block.get("unsupported_reasons", []))
        return list(dict.fromkeys(reasons))
