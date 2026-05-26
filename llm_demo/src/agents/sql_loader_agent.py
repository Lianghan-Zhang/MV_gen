from __future__ import annotations

import time
from pathlib import Path

from llm_demo.src.core.agent_base import BaseAgent
from llm_demo.src.core.artifact_store import ArtifactStore


class SQLLoaderAgent(BaseAgent):
    def __init__(self, store: ArtifactStore) -> None:
        super().__init__(store=store, agent_name="SQLLoaderAgent")

    def run(self, sql_paths: list[str | Path]) -> Path:
        started_at = time.monotonic()
        raw_sql_dir = self.store.ensure_dir("00_raw_sql")
        output_paths: list[Path] = []
        for sql_path in sql_paths:
            path = Path(sql_path)
            query_id = path.stem
            sql_text = path.read_text(encoding="utf-8")
            output_paths.append(self.store.write_text(f"00_raw_sql/{query_id}.sql", sql_text))

        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[Path(path) for path in sql_paths],
            output_artifact_paths=output_paths,
            elapsed_ms=self._elapsed_ms(started_at),
        )
        return raw_sql_dir
