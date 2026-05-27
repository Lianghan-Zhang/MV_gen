from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


def default_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


class ArtifactStore:
    def __init__(
        self,
        run_id: str,
        artifact_root: str | Path | None = None,
    ) -> None:
        self.project_root = default_project_root()
        self.run_id = run_id
        self.artifact_root = Path(artifact_root) if artifact_root else self.project_root / "llm_demo" / "artifacts"
        self.run_dir = self.artifact_root / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

    @property
    def run_log_path(self) -> Path:
        return self.run_dir / "06_execution_logs" / "run_log.jsonl"

    def path(self, relative_path: str | Path) -> Path:
        return self.run_dir / Path(relative_path)

    def ensure_dir(self, relative_path: str | Path) -> Path:
        path = self.path(relative_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_text(self, relative_path: str | Path, text: str) -> Path:
        path = self.path(relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def read_text(self, path: str | Path) -> str:
        return Path(path).read_text(encoding="utf-8")

    def write_json(self, relative_path: str | Path, data: dict[str, Any] | list[Any]) -> Path:
        path = self.path(relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def read_json(self, path: str | Path) -> Any:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def append_run_log(
        self,
        *,
        agent_name: str,
        event: str,
        input_artifact_paths: list[str | Path],
        output_artifact_paths: list[str | Path],
        elapsed_ms: int,
        batch_id: int | None = None,
        candidate_id: str | None = None,
        error: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.run_log_path.parent.mkdir(parents=True, exist_ok=True)
        scope = f"batch_{batch_id}" if batch_id is not None else "global"
        event_id = f"{self.run_id}:{agent_name}:{scope}:{self._next_event_seq():04d}"
        record = {
            "run_id": self.run_id,
            "event_id": event_id,
            "agent_name": agent_name,
            "batch_id": batch_id,
            "candidate_id": candidate_id,
            "input_artifact_paths": [str(path) for path in input_artifact_paths],
            "output_artifact_paths": [str(path) for path in output_artifact_paths],
            "elapsed_ms": elapsed_ms,
            "event": event,
            "error": error,
            "details": details or {},
            "created_at_ms": int(time.time() * 1000),
        }
        with self.run_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def _next_event_seq(self) -> int:
        if not self.run_log_path.exists():
            return 1
        with self.run_log_path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip()) + 1
