from __future__ import annotations

import time
from pathlib import Path

from llm_demo.src.agents.batch_mv_agent import BatchMVAgent
from llm_demo.src.agents.executor_agent import ExecutorAgent
from llm_demo.src.agents.rewrite_agent import RewriteAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient


class BatchWorkflowRunner:
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        self.store = store
        self.llm_client = llm_client

    def run_all_batches(
        self,
        complexity_batches_path: str | Path,
        sql_manifest_path: str | Path,
        query_blocks_path: str | Path,
        families_path: str | Path | None = None,
        materialized_mvs_path: str | Path | None = None,
    ) -> list[Path]:
        started_at = time.monotonic()
        batches_path = Path(complexity_batches_path)
        manifest_path = Path(sql_manifest_path)
        qb_path = Path(query_blocks_path)
        family_path = Path(families_path) if families_path else None
        mv_state_path = Path(materialized_mvs_path) if materialized_mvs_path else self.store.materialized_mvs_path
        output_paths: list[Path] = []

        batches = self.store.read_json(batches_path).get("complexity_batches", [])
        rewrite_agent = RewriteAgent(self.store, self.llm_client)
        batch_mv_agent = BatchMVAgent(self.store, self.llm_client)
        executor_agent = ExecutorAgent(self.store)

        for batch in batches:
            batch_id = batch["batch_id"]
            if not batch.get("query_ids"):
                continue
            historical_rewrite_dir = rewrite_agent.run(
                batch_id=batch_id,
                rewrite_stage="historical",
                complexity_batches_path=batches_path,
                sql_manifest_path=manifest_path,
                query_blocks_path=qb_path,
                materialized_mvs_path=mv_state_path,
            )
            mv_candidates_path = batch_mv_agent.run(
                batch_id=batch_id,
                complexity_batches_path=batches_path,
                query_blocks_path=qb_path,
                families_path=family_path,
                historical_rewrite_dir=historical_rewrite_dir,
                materialized_mvs_path=mv_state_path,
            )
            mv_state_path = executor_agent.materialize_mvs(
                batch_id=batch_id,
                mv_candidates_path=mv_candidates_path,
                mv_build_sql_path=self.store.batch_mv_build_sql_path(batch_id),
                materialized_mvs_path=mv_state_path,
            )
            final_rewrite_dir = rewrite_agent.run(
                batch_id=batch_id,
                rewrite_stage="final",
                complexity_batches_path=batches_path,
                sql_manifest_path=manifest_path,
                query_blocks_path=qb_path,
                materialized_mvs_path=mv_state_path,
            )
            execution_order_path = executor_agent.run_queries(
                batch_id=batch_id,
                final_rewrite_dir=final_rewrite_dir,
                complexity_batches_path=batches_path,
            )
            output_paths.extend([historical_rewrite_dir, mv_candidates_path, mv_state_path, final_rewrite_dir, execution_order_path])

        self.store.append_run_log(
            agent_name="BatchWorkflowRunner",
            event="success",
            input_artifact_paths=[
                path
                for path in [batches_path, manifest_path, qb_path, family_path]
                if path is not None
            ],
            output_artifact_paths=output_paths,
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            details={"processed_batch_ids": [batch["batch_id"] for batch in batches if batch.get("query_ids")]},
        )
        return output_paths
