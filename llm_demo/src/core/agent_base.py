from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .artifact_store import ArtifactStore, default_project_root
from .llm_client import LLMClient
from .schemas import model_schema, model_to_dict, validate_model


class BaseAgent:
    def __init__(self, store: ArtifactStore, agent_name: str | None = None) -> None:
        self.store = store
        self.agent_name = agent_name or self.__class__.__name__

    def _elapsed_ms(self, started_at: float) -> int:
        return int((time.monotonic() - started_at) * 1000)


class LLMRulesAgent(BaseAgent):
    def __init__(
        self,
        store: ArtifactStore,
        llm_client: LLMClient,
        *,
        agent_name: str | None = None,
        rules_dir: str | Path | None = None,
    ) -> None:
        super().__init__(store=store, agent_name=agent_name)
        self.llm_client = llm_client
        root = default_project_root()
        self.rules_dir = Path(rules_dir) if rules_dir else root / "llm_demo" / "rules"

    def _infer_structured(
        self,
        *,
        task: str,
        context: dict[str, Any],
        input_artifacts: dict[str, Any],
        output_model: type,
    ) -> dict[str, Any]:
        prompt = self._build_prompt(
            task=task,
            context=context,
            input_artifacts=input_artifacts,
            output_schema=model_schema(output_model),
        )
        raw_output = self.llm_client.infer(prompt, load_json=True)
        validated = validate_model(output_model, raw_output)
        return model_to_dict(validated)

    def _build_prompt(
        self,
        *,
        task: str,
        context: dict[str, Any],
        input_artifacts: dict[str, Any],
        output_schema: dict[str, Any],
    ) -> str:
        template = (self.rules_dir / "_prompt_template.md").read_text(encoding="utf-8")
        rules_path = self.rules_dir / f"{self._rules_name()}.md"
        agent_rules_md = rules_path.read_text(encoding="utf-8")
        return template.format(
            agent_name=self.agent_name,
            task=task,
            agent_rules_md=agent_rules_md,
            context_json=json.dumps(context, ensure_ascii=False, indent=2),
            input_artifacts_json=json.dumps(input_artifacts, ensure_ascii=False, indent=2),
            output_schema_json=json.dumps(output_schema, ensure_ascii=False, indent=2),
        )

    def _rules_name(self) -> str:
        name = self.agent_name
        chars: list[str] = []
        for index, char in enumerate(name):
            if char.isupper() and index > 0:
                chars.append("_")
            chars.append(char.lower())
        snake = "".join(chars)
        return snake.replace("_agent", "_agent")
