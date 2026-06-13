from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import httpx
from openai import OpenAI


def default_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


class LLMClient:
    def __init__(self, project_root: str | Path | None = None) -> None:
        self.project_root = Path(project_root) if project_root else default_project_root()
        load_dotenv(self.project_root / ".env", override=False)

        self.api_key = self._required_env("DEEPSEEK_API_KEY")
        self.base_url = self._required_env("DEEPSEEK_BASE_URL")
        self.model = self._required_env("DEEPSEEK_MODEL")
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "2"))
        self.timeout_seconds = float(os.getenv("LLM_TIMEOUT_SECONDS", "120"))
        self.connect_timeout_seconds = float(os.getenv("LLM_CONNECT_TIMEOUT_SECONDS", "60"))
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=httpx.Timeout(self.timeout_seconds, connect=self.connect_timeout_seconds),
        )

    def infer(self, prompt: str, load_json: bool = True) -> str | dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    temperature=self.temperature,
                    messages=[{"role": "user", "content": prompt}],
                )
                content = response.choices[0].message.content or ""
                if load_json:
                    return self._parse_json(content)
                return content
            except Exception as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(1 + attempt)
        raise RuntimeError(
            "LLM inference failed after "
            f"{self.max_retries + 1} attempts "
            f"(model={self.model}, base_url={self.base_url}, "
            f"timeout={self.timeout_seconds}s, connect_timeout={self.connect_timeout_seconds}s): "
            f"{last_error}"
        ) from last_error

    def _required_env(self, name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return value

    def _parse_json(self, content: str) -> dict[str, Any]:
        text = content.strip()
        if text.startswith("```json") and text.endswith("```"):
            text = text[7:-3].strip()
        elif text.startswith("```") and text.endswith("```"):
            text = text[3:-3].strip()
        else:
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and start < end:
                text = text[start : end + 1]
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("LLM response must be a JSON object")
        return parsed
