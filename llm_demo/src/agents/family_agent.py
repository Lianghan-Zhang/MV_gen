from __future__ import annotations

import time
from pathlib import Path

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import FamilyOutput


class FamilyAgent(LLMRulesAgent):
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="FamilyAgent")

    def run(self, query_blocks_path: str | Path) -> Path:
        started_at = time.monotonic()
        path = Path(query_blocks_path)
        query_blocks = self.store.read_json(path)
        output = self._infer_structured(
            task="根据 QueryBlock 的表集合相似度、ETL 宽表覆盖关系和安全门规则聚合 QueryFamily。",
            context={"run_id": self.store.run_id},
            input_artifacts=query_blocks,
            output_model=FamilyOutput,
        )
        families_path = self.store.write_json("02_families/query_families.json", output)
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[path],
            output_artifact_paths=[families_path],
            elapsed_ms=self._elapsed_ms(started_at),
        )
        return families_path
