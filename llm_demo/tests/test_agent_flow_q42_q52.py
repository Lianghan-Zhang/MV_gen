from __future__ import annotations

import json
import time
from pathlib import Path

import nbformat

from llm_demo.src.agents.family_agent import FamilyAgent
from llm_demo.src.agents.feature_agent import FeatureAgent
from llm_demo.src.agents.self_iteration_agent import SelfIterationAgent
from llm_demo.src.agents.sql_loader_agent import SQLLoaderAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.llm_client import LLMClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_PATHS = [
    PROJECT_ROOT / "tpcds-spark" / "q42.sql",
    PROJECT_ROOT / "tpcds-spark" / "q52.sql",
]


def make_store(name: str) -> ArtifactStore:
    return ArtifactStore(
        run_id=f"{name}_{int(time.time() * 1000)}",
        artifact_root=PROJECT_ROOT / "llm_demo" / "artifacts",
    )


def test_sql_loader_writes_raw_sql_and_run_log() -> None:
    store = make_store("test_sql_loader")
    raw_sql_dir = SQLLoaderAgent(store).run(SQL_PATHS)

    assert (raw_sql_dir / "q42.sql").read_text(encoding="utf-8").startswith("SELECT")
    assert (raw_sql_dir / "q52.sql").read_text(encoding="utf-8").startswith("SELECT")

    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1
    assert records[0]["agent_name"] == "SQLLoaderAgent"
    assert records[0]["event_id"].startswith(f"{store.run_id}:SQLLoaderAgent:global:")


def test_live_feature_family_and_self_iteration_agents_produce_artifacts() -> None:
    store = make_store("test_live_agents")
    llm_client = LLMClient(project_root=PROJECT_ROOT)

    raw_sql_dir = SQLLoaderAgent(store).run(SQL_PATHS)
    query_blocks_path = FeatureAgent(store, llm_client).run(raw_sql_dir)
    query_blocks = store.read_json(query_blocks_path)["query_blocks"]

    by_query = {}
    for block in query_blocks:
        by_query.setdefault(block["query_id"], []).append(block)

    for query_id in ("q42", "q52"):
        assert query_id in by_query
        outer_blocks = [block for block in by_query[query_id] if block["qb_id"] == f"{query_id}.outer"]
        assert outer_blocks
        block = outer_blocks[0]
        assert {"date_dim", "store_sales", "item"}.issubset(set(block["tables"]))
        assert block["complexity_type"] == "join_filter_groupby"

    families_path = FamilyAgent(store, llm_client).run(query_blocks_path)
    families = store.read_json(families_path)["query_families"]
    assert any({"q42.outer", "q52.outer"}.issubset(set(family["members"])) for family in families)

    feedback_path = SelfIterationAgent(store, llm_client).run(store.run_log_path)
    feedback = store.read_json(feedback_path)
    assert feedback["run_id"] == store.run_id
    assert isinstance(feedback["agent_rule_suggestions"], dict)
    for group in feedback["agent_rule_suggestions"].values():
        for suggestion in group.get("suggestions", []):
            assert suggestion["evidence_refs"]
            for evidence in suggestion["evidence_refs"]:
                if evidence.get("artifact") and "run_log.jsonl" in evidence["artifact"]:
                    assert evidence.get("event_id")


def test_notebook_skeleton_documents_first_flow() -> None:
    notebook_path = PROJECT_ROOT / "llm_demo" / "notebooks" / "etl_agent_flow.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)
    source = "\n".join(cell.get("source", "") for cell in notebook.cells)

    assert "load_dotenv" in source
    assert "ArtifactStore" in source
    assert "SQLLoaderAgent" in source
    assert "FeatureAgent" in source
    assert "FamilyAgent" in source
    assert "SelfIterationAgent" in source
