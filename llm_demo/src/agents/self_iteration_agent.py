from __future__ import annotations

import json
import time
from pathlib import Path

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.schemas import SelfIterationFeedback


class SelfIterationAgent(LLMRulesAgent):
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="SelfIterationAgent")

    def run(self, run_log_path: str | Path) -> Path:
        started_at = time.monotonic()
        path = Path(run_log_path)
        run_log_text = path.read_text(encoding="utf-8")
        run_id = self._extract_run_id(run_log_text)
        output = self._infer_structured(
            task=(
                "读取 run_log.jsonl 与同 run 的 coverage、family candidates、MV candidates、rewrite meta、"
                "execution order，输出按 target Agent 分组的 rules 反馈建议。"
            ),
            context={"run_id": run_id},
            input_artifacts={"run_log_jsonl": run_log_text, **self._related_artifacts()},
            output_model=SelfIterationFeedback,
        )
        if output["run_id"] != run_id:
            raise ValueError(f"SelfIterationAgent returned run_id {output['run_id']} but expected {run_id}")
        self._validate_evidence_refs(output)

        feedback_path = self.store.write_json(f"07_feedback/feedback_rules_{run_id}.json", output)
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[path],
            output_artifact_paths=[feedback_path],
            elapsed_ms=self._elapsed_ms(started_at),
        )
        return feedback_path

    def _related_artifacts(self) -> dict:
        artifacts: dict[str, object] = {}
        for name, path in (
            ("coverage_summary", self.store.coverage_summary_path),
            ("family_candidates", self.store.family_candidates_path),
            ("query_families", self.store.query_families_path),
        ):
            if path.exists():
                artifacts[name] = self.store.read_json(path)

        artifacts["mv_candidates"] = [
            self.store.read_json(path)
            for path in sorted(self.store.run_dir.glob("04_batch_mvs/batch_*_mv_candidates.json"))
        ]
        artifacts["rewrite_meta"] = [
            self.store.read_json(path)
            for path in sorted(self.store.run_dir.glob("05_rewritten_sql/batch_*/*_rewrite/*_rewrite_meta.json"))
        ]
        artifacts["execution_orders"] = [
            self.store.read_json(path)
            for path in sorted(self.store.run_dir.glob("06_execution_logs/batch_*_execution_order.json"))
        ]
        return artifacts

    def _extract_run_id(self, run_log_text: str) -> str:
        for line in run_log_text.splitlines():
            if line.strip():
                record = json.loads(line)
                run_id = record.get("run_id")
                if run_id:
                    return run_id
        return self.store.run_id

    def _validate_evidence_refs(self, output: dict) -> None:
        for group in output.get("agent_rule_suggestions", {}).values():
            for suggestion in group.get("suggestions", []):
                evidence_refs = suggestion.get("evidence_refs", [])
                if not evidence_refs:
                    raise ValueError("Every suggestion must include evidence_refs")
                for evidence in evidence_refs:
                    if evidence.get("artifact") and "run_log.jsonl" in evidence["artifact"] and not evidence.get("event_id"):
                        raise ValueError("Evidence refs to run_log.jsonl must include event_id")
