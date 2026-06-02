from __future__ import annotations

import time
from pathlib import Path

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import FeatureOutput


class FeatureAgent(LLMRulesAgent):
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="FeatureAgent")

    def run(self, raw_sql_artifact: str | Path) -> Path:
        started_at = time.monotonic()
        raw_artifact = Path(raw_sql_artifact)
        queries = self._load_queries(raw_artifact)
        if not queries:
            raise ValueError(f"No SQL files found from {raw_artifact}")

        extracted_output = self._infer_structured(
            task="从输入 SQL Text 中提取 QueryBlock，并输出 query_to_qbs 与 qb_to_query 索引。",
            context={"run_id": self.store.run_id, "expected_query_ids": [query["query_id"] for query in queries]},
            input_artifacts={"queries": queries},
            output_model=FeatureOutput,
        )
        output = self._infer_structured(
            task=(
                "对 candidate_feature_output 进行 evaluate：检查 QueryBlock 是否使用物理表名而非 SQL alias，"
                "检查 query_id/qb_id 索引是否一致；如发现 alias 或不合规字段，直接返回修正后的 FeatureOutput。"
            ),
            context={"run_id": self.store.run_id, "expected_query_ids": [query["query_id"] for query in queries]},
            input_artifacts={"queries": queries, "candidate_feature_output": extracted_output},
            output_model=FeatureOutput,
        )
        self._validate_expected_queries(output, [query["query_id"] for query in queries])

        query_blocks_path = self.store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": output["query_blocks"]})
        query_to_qbs_path = self.store.write_json("01_query_blocks/query_to_qbs.json", output["query_to_qbs"])
        qb_to_query_path = self.store.write_json("01_query_blocks/qb_to_query.json", output["qb_to_query"])

        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[raw_artifact],
            output_artifact_paths=[query_blocks_path, query_to_qbs_path, qb_to_query_path],
            elapsed_ms=self._elapsed_ms(started_at),
        )
        return query_blocks_path

    def extract_one(self, query: dict[str, str], attempt: int = 1) -> dict:
        started_at = time.monotonic()
        query_with_sql = self._query_with_sql_text(query)
        query_id = query_with_sql["query_id"]
        extracted_output = self._infer_structured(
            task="从单条输入 SQL Text 中提取 QueryBlock，并输出 query_to_qbs 与 qb_to_query 索引。",
            context={"run_id": self.store.run_id, "expected_query_ids": [query_id], "attempt": attempt},
            input_artifacts={"query": query_with_sql},
            output_model=FeatureOutput,
        )
        output = self._infer_structured(
            task=(
                "对 candidate_feature_output 进行 evaluate：检查 outer、CTE、subquery、set branch QueryBlock 是否完整，"
                "检查是否使用物理表名而非 SQL alias，检查 query_id/qb_id 索引是否一致；"
                "如发现 alias 或不合规字段，直接返回修正后的 FeatureOutput。"
            ),
            context={"run_id": self.store.run_id, "expected_query_ids": [query_id], "attempt": attempt},
            input_artifacts={"query": query_with_sql, "candidate_feature_output": extracted_output},
            output_model=FeatureOutput,
        )
        self._validate_expected_queries(output, [query_id])
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="extract_one_success",
            input_artifact_paths=[query.get("sql_path", query.get("sql_path_relative", query_id))],
            output_artifact_paths=[],
            elapsed_ms=self._elapsed_ms(started_at),
            details={
                "query_id": query_id,
                "attempt": attempt,
                "qb_ids": output["query_to_qbs"].get(query_id, []),
            },
        )
        return output

    def _load_queries(self, raw_artifact: Path) -> list[dict[str, str]]:
        if raw_artifact.is_dir():
            return [
                {"query_id": path.stem, "sql_text": path.read_text(encoding="utf-8")}
                for path in sorted(raw_artifact.glob("*.sql"))
            ]

        manifest = self.store.read_json(raw_artifact)
        queries: list[dict[str, str]] = []
        for item in manifest.get("queries", []):
            query_id = item["query_id"]
            sql_path = self._resolve_sql_path(item)
            queries.append({"query_id": query_id, "sql_text": sql_path.read_text(encoding="utf-8")})
        return queries

    def _query_with_sql_text(self, query: dict[str, str]) -> dict[str, str]:
        if query.get("sql_text"):
            return {"query_id": query["query_id"], "sql_text": query["sql_text"]}
        sql_path = self._resolve_sql_path(query)
        return {
            "query_id": query["query_id"],
            "sql_text": sql_path.read_text(encoding="utf-8"),
            "sql_path": str(sql_path),
        }

    def _resolve_sql_path(self, manifest_item: dict) -> Path:
        sql_path = Path(manifest_item["sql_path"])
        if sql_path.is_file():
            return sql_path

        relative_path = manifest_item.get("sql_path_relative")
        if relative_path:
            project_relative_path = self.store.project_root / relative_path
            if project_relative_path.is_file():
                return project_relative_path

        raise FileNotFoundError(f"SQL file not found for query_id={manifest_item.get('query_id')}: {sql_path}")

    def _validate_expected_queries(self, output: dict, expected_query_ids: list[str]) -> None:
        query_to_qbs = output.get("query_to_qbs", {})
        for query_id in expected_query_ids:
            if query_id not in query_to_qbs or not query_to_qbs[query_id]:
                raise ValueError(f"FeatureAgent output missing QueryBlock for {query_id}")
