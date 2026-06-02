from __future__ import annotations

import time
from pathlib import Path

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.family_candidate_builder import FamilyCandidateBuilder
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import FamilyOutput


class FamilyAgent(LLMRulesAgent):
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="FamilyAgent")

    def run(self, family_candidates_path: str | Path, query_blocks_path: str | Path | None = None) -> Path:
        started_at = time.monotonic()
        input_path = Path(family_candidates_path)
        if input_path.name == "query_blocks.json":
            query_blocks_path = input_path
            input_path = FamilyCandidateBuilder(self.store).run(input_path)
        candidate_artifact = self.store.read_json(input_path)
        query_blocks_artifact = self.store.read_json(query_blocks_path) if query_blocks_path else {}
        input_artifacts = {
            **query_blocks_artifact,
            **candidate_artifact,
        }
        candidate_output = self._infer_structured(
            task="根据 family_candidates 的 Jaccard、Containment、join graph 和 predicate/measure 证据生成 candidate QueryFamily。",
            context={"run_id": self.store.run_id},
            input_artifacts=input_artifacts,
            output_model=FamilyOutput,
        )
        output = self._infer_structured(
            task=(
                "evaluate candidate QueryFamily：检查是否有重复 family、可合并 family、错误 family；"
                "过滤列完全不同且不能由共享宽表安全覆盖时应拆分；返回修正后的完整 FamilyOutput。"
            ),
            context={"run_id": self.store.run_id},
            input_artifacts={**input_artifacts, "candidate_family_output": candidate_output},
            output_model=FamilyOutput,
        )
        families_path = self.store.write_json("02_families/query_families.json", output)
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[input_path] + ([Path(query_blocks_path)] if query_blocks_path else []),
            output_artifact_paths=[families_path],
            elapsed_ms=self._elapsed_ms(started_at),
            details={"family_count": len(output["query_families"])},
        )
        return families_path
