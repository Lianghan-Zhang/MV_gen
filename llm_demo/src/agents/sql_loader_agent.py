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
        self.store.ensure_dir("00_raw_sql")
        queries: list[dict[str, object]] = []
        for sql_path in sql_paths:
            path = Path(sql_path).expanduser().resolve()
            if not path.is_file():
                raise FileNotFoundError(f"SQL file not found: {path}")
            query_id = path.stem
            try:
                relative_path = str(path.relative_to(self.store.project_root))
            except ValueError:
                relative_path = None
            queries.append(
                {
                    "query_id": query_id,
                    "sql_path": str(path),
                    "sql_path_relative": relative_path,
                    "size_bytes": path.stat().st_size,
                }
            )

        manifest_path = self.store.write_json(
            "00_raw_sql/sql_manifest.json",
            {
                "version": 1,
                "queries": queries,
            },
        )

        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[Path(path) for path in sql_paths],
            output_artifact_paths=[manifest_path],
            elapsed_ms=self._elapsed_ms(started_at),
        )
        return manifest_path
