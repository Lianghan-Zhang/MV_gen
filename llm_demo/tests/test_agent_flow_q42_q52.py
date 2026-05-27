from __future__ import annotations

import json
import time
from pathlib import Path

import nbformat

from llm_demo.src.agents.batch_cluster_agent import BatchClusterAgent
from llm_demo.src.agents.batch_mv_agent import BatchMVAgent
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


def write_original_equivalent_historical_rewrite(store: ArtifactStore, batch_id: int, query_ids: list[str]) -> Path:
    rewrite_dir = store.ensure_dir(f"05_rewritten_sql/batch_{batch_id}/historical_rewrite")
    for query_id in query_ids:
        original_sql_path = PROJECT_ROOT / "tpcds-spark" / f"{query_id}.sql"
        rewritten_sql_path = rewrite_dir / f"{query_id}_rewritten.sql"
        rewritten_sql_path.write_text(original_sql_path.read_text(encoding="utf-8"), encoding="utf-8")
        meta_path = rewrite_dir / f"{query_id}_rewrite_meta.json"
        meta_path.write_text(
            json.dumps(
                {
                    "query_id": query_id,
                    "rewrite_stage": "historical",
                    "status": "fallback",
                    "used_mv_ids": [],
                    "fallback_reason": "no_available_historical_mv",
                    "original_sql_path": str(original_sql_path),
                    "rewritten_sql_path": str(rewritten_sql_path),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return rewrite_dir


def test_sql_loader_writes_manifest_and_run_log() -> None:
    store = make_store("test_sql_loader")
    manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)

    assert manifest_path == store.path("00_raw_sql/sql_manifest.json")
    assert not (store.path("00_raw_sql") / "q42.sql").exists()
    assert not (store.path("00_raw_sql") / "q52.sql").exists()

    manifest = store.read_json(manifest_path)
    queries = {query["query_id"]: query for query in manifest["queries"]}
    assert set(queries) == {"q42", "q52"}
    assert queries["q42"]["sql_path"].endswith("tpcds-spark/q42.sql")
    assert queries["q52"]["sql_path"].endswith("tpcds-spark/q52.sql")
    assert queries["q42"]["size_bytes"] > 0
    assert "sha256" not in queries["q42"]

    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1
    assert records[0]["agent_name"] == "SQLLoaderAgent"
    assert records[0]["event_id"].startswith(f"{store.run_id}:SQLLoaderAgent:global:")
    assert records[0]["output_artifact_paths"] == [str(manifest_path)]


def test_live_feature_family_and_self_iteration_agents_produce_artifacts() -> None:
    store = make_store("test_live_agents")
    llm_client = LLMClient(project_root=PROJECT_ROOT)

    sql_manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)
    query_blocks_path = FeatureAgent(store, llm_client).run(sql_manifest_path)
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
        serialized_block = json.dumps(block, ensure_ascii=False)
        assert "dt." not in serialized_block

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


def test_live_batch_cluster_batch_mv_and_self_iteration_agents_produce_artifacts() -> None:
    store = make_store("test_live_batch_agents")
    llm_client = LLMClient(project_root=PROJECT_ROOT)

    sql_manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)
    query_blocks_path = FeatureAgent(store, llm_client).run(sql_manifest_path)
    families_path = FamilyAgent(store, llm_client).run(query_blocks_path)

    batches_path = BatchClusterAgent(store, llm_client).run(query_blocks_path, families_path)
    batches = store.read_json(batches_path)["complexity_batches"]
    by_type = {batch["batch_type"]: batch for batch in batches}

    target_batch = by_type["join_filter_groupby"]
    assert target_batch["batch_id"] == 3
    assert {"q42", "q52"}.issubset(set(target_batch["query_ids"]))
    assert any(
        {"q42", "q52"}.issubset(set(group["query_ids"]))
        and {"q42.outer", "q52.outer"}.issubset(set(group["qb_ids"]))
        for group in target_batch["family_groups"]
    )

    historical_rewrite_dir = write_original_equivalent_historical_rewrite(store, 3, ["q42", "q52"])
    mv_candidates_path = BatchMVAgent(store, llm_client).run(
        3,
        batches_path,
        query_blocks_path,
        families_path,
        historical_rewrite_dir,
    )
    mv_output = store.read_json(mv_candidates_path)
    assert mv_output["batch_id"] == 3
    assert mv_output["mv_candidates"]
    assert store.path("04_batch_mvs/batch_3_mv_build.sql").exists()
    assert store.path("04_batch_mvs/materialized_mvs.json").exists()

    for candidate in mv_output["mv_candidates"]:
        assert candidate["source_batch_id"] == 3
        assert candidate["family_id"]
        assert set(candidate["source_query_ids"]).issubset(set(target_batch["query_ids"]))
        assert set(candidate["target_queries"]).issubset(set(target_batch["query_ids"]))
        assert not any(query_id.startswith("downstream") for query_id in candidate["target_queries"])

    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    batch_mv_records = [record for record in records if record["agent_name"] == "BatchMVAgent"]
    assert any(record["details"].get("candidate_ids") for record in batch_mv_records)
    assert any(record.get("candidate_id") for record in batch_mv_records)

    feedback_path = SelfIterationAgent(store, llm_client).run(store.run_log_path)
    feedback = store.read_json(feedback_path)
    assert feedback["run_id"] == store.run_id
    assert isinstance(feedback["agent_rule_suggestions"], dict)
    for group in feedback["agent_rule_suggestions"].values():
        for suggestion in group.get("suggestions", []):
            assert suggestion["evidence_refs"]


def test_feature_agent_normalizes_aliases_to_physical_table_names() -> None:
    class EvaluatingLLM:
        def __init__(self) -> None:
            self.calls = 0

        def infer(self, prompt: str, load_json: bool = True) -> dict:
            self.calls += 1
            if self.calls == 2:
                return {
                    "query_blocks": [
                        {
                            "qb_id": "q42.outer",
                            "query_id": "q42",
                            "scope_type": "outer",
                            "tables": ["date_dim", "store_sales", "item"],
                            "join_edges": [
                                "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
                                "store_sales.ss_item_sk = item.i_item_sk",
                            ],
                            "predicates": ["date_dim.d_moy = 11", "date_dim.d_year = 2000", "item.i_manager_id = 1"],
                            "group_by_exprs": ["date_dim.d_year", "item.i_category_id"],
                            "aggregate_exprs": ["sum(ss_ext_sales_price)"],
                            "complexity_type": "join_filter_groupby",
                            "family_key": "store_sales-date_dim-item",
                            "unsupported_reasons": [],
                        }
                    ],
                    "query_to_qbs": {"q42": ["q42.outer"]},
                    "qb_to_query": {"q42.outer": "q42"},
                }
            return {
                "query_blocks": [
                    {
                        "qb_id": "q42.outer",
                        "query_id": "q42",
                        "scope_type": "outer",
                        "tables": ["dt", "store_sales", "item"],
                        "join_edges": [
                            "dt.d_date_sk = store_sales.ss_sold_date_sk",
                            "store_sales.ss_item_sk = item.i_item_sk",
                        ],
                        "predicates": ["dt.d_moy = 11", "dt.d_year = 2000", "item.i_manager_id = 1"],
                        "group_by_exprs": ["dt.d_year", "item.i_category_id"],
                        "aggregate_exprs": ["sum(ss_ext_sales_price)"],
                        "complexity_type": "join_filter_groupby",
                        "family_key": "store_sales-dt-item",
                        "unsupported_reasons": [],
                    }
                ],
                "query_to_qbs": {"q42": ["q42.outer"]},
                "qb_to_query": {"q42.outer": "q42"},
            }

    store = make_store("test_feature_aliases")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q42.sql"])
    llm = EvaluatingLLM()
    query_blocks_path = FeatureAgent(store, llm).run(sql_manifest_path)
    block = store.read_json(query_blocks_path)["query_blocks"][0]

    assert llm.calls == 2
    assert block["tables"] == ["date_dim", "store_sales", "item"]
    assert block["family_key"] == "store_sales-date_dim-item"
    serialized_block = json.dumps(block, ensure_ascii=False)
    assert "dt." not in serialized_block
    assert "date_dim.d_year" in serialized_block


def test_notebook_skeleton_documents_first_flow() -> None:
    notebook_path = PROJECT_ROOT / "llm_demo" / "notebooks" / "etl_agent_flow.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)
    source = "\n".join(cell.get("source", "") for cell in notebook.cells)

    assert "load_dotenv" in source
    assert "ArtifactStore" in source
    assert "SQLLoaderAgent" in source
    assert "FeatureAgent" in source
    assert "FamilyAgent" in source
    assert "BatchClusterAgent" in source
    assert "BatchMVAgent" in source
    assert "SelfIterationAgent" in source
