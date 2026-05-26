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

    def run(self, raw_sql_dir: str | Path) -> Path:
        started_at = time.monotonic()
        raw_dir = Path(raw_sql_dir)
        queries = [
            {"query_id": path.stem, "sql_text": path.read_text(encoding="utf-8")}
            for path in sorted(raw_dir.glob("*.sql"))
        ]
        if not queries:
            raise ValueError(f"No SQL files found in {raw_dir}")

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
            input_artifact_paths=[raw_dir],
            output_artifact_paths=[query_blocks_path, query_to_qbs_path, qb_to_query_path],
            elapsed_ms=self._elapsed_ms(started_at),
        )
        return query_blocks_path

    def _validate_expected_queries(self, output: dict, expected_query_ids: list[str]) -> None:
        query_to_qbs = output.get("query_to_qbs", {})
        for query_id in expected_query_ids:
            if query_id not in query_to_qbs or not query_to_qbs[query_id]:
                raise ValueError(f"FeatureAgent output missing QueryBlock for {query_id}")
