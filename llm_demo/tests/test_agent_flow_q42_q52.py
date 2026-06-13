from __future__ import annotations

import json
import time
from pathlib import Path

import nbformat
import pytest

from llm_demo.src.agents.batch_cluster_agent import BatchClusterAgent
from llm_demo.src.agents.batch_mv_agent import BatchMVAgent
from llm_demo.src.agents.executor_agent import ExecutorAgent
from llm_demo.src.agents.family_agent import FamilyAgent
from llm_demo.src.agents.feature_agent import FeatureAgent
from llm_demo.src.agents.rewrite_agent import RewriteAgent
from llm_demo.src.agents.self_iteration_agent import SelfIterationAgent
from llm_demo.src.agents.sql_loader_agent import SQLLoaderAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.batch_workflow_runner import BatchWorkflowRunner
from llm_demo.src.core.coverage_summary_builder import CoverageSummaryBuilder
from llm_demo.src.core.family_candidate_builder import FamilyCandidateBuilder
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.query_analysis_runner import QueryAnalysisRunner


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_PATHS = [
    PROJECT_ROOT / "tpcds-spark" / "q42.sql",
    PROJECT_ROOT / "tpcds-spark" / "q52.sql",
]


class StaticLLM:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.calls = 0

    def infer(self, prompt: str, load_json: bool = True) -> dict:
        self.calls += 1
        if not self.responses:
            raise AssertionError("StaticLLM has no remaining responses")
        return json.loads(json.dumps(self.responses.pop(0)))


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


def write_minimal_complexity_batches(store: ArtifactStore, batch_id: int, batch_type: str, query_ids: list[str]) -> Path:
    batches = []
    for current_id, current_type in ((1, "join"), (2, "join_filter"), (3, "join_filter_groupby"), (4, "other")):
        batches.append(
            {
                "batch_id": current_id,
                "batch_type": current_type,
                "query_ids": query_ids if current_id == batch_id else [],
                "family_groups": [],
            }
        )
    return store.write_json("03_batches/complexity_batches.json", {"complexity_batches": batches})


def write_minimal_batch_mv_inputs(store: ArtifactStore, query_ids: list[str]) -> tuple[Path, Path, Path, Path]:
    batches_path = store.write_json(
        "03_batches/complexity_batches.json",
        {
            "complexity_batches": [
                {"batch_id": 1, "batch_type": "join", "query_ids": [], "family_groups": []},
                {"batch_id": 2, "batch_type": "join_filter", "query_ids": [], "family_groups": []},
                {
                    "batch_id": 3,
                    "batch_type": "join_filter_groupby",
                    "query_ids": query_ids,
                    "family_groups": [
                        {
                            "family_id": "family_fact_ss_dd_item_manager_moy_year",
                            "query_ids": query_ids,
                            "qb_ids": [f"{query_id}.outer" for query_id in query_ids],
                        }
                    ],
                },
                {"batch_id": 4, "batch_type": "other", "query_ids": [], "family_groups": []},
            ]
        },
    )
    query_blocks_path = store.write_json(
        "01_query_blocks/query_blocks.json",
        {
            "query_blocks": [
                {
                    "qb_id": f"{query_id}.outer",
                    "query_id": query_id,
                    "scope_type": "outer",
                    "tables": ["date_dim", "store_sales", "item"],
                    "join_edges": [
                        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
                        "store_sales.ss_item_sk = item.i_item_sk",
                    ],
                    "predicates": ["date_dim.d_year = 2000"],
                    "group_by_exprs": ["date_dim.d_year"],
                    "aggregate_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                    "complexity_type": "join_filter_groupby",
                    "family_key": "store_sales-date_dim-item",
                    "unsupported_reasons": [],
                }
                for query_id in query_ids
            ]
        },
    )
    store.write_json("01_query_blocks/query_to_qbs.json", {query_id: [f"{query_id}.outer"] for query_id in query_ids})
    store.write_json("01_query_blocks/qb_to_query.json", {f"{query_id}.outer": query_id for query_id in query_ids})
    families_path = store.write_json(
        "02_families/query_families.json",
        {
            "query_families": [
                {
                    "family_id": "family_fact_ss_dd_item_manager_moy_year",
                    "family_key": "store_sales-date_dim-item",
                    "members": [f"{query_id}.outer" for query_id in query_ids],
                    "common_tables": ["date_dim", "store_sales", "item"],
                    "common_join_skeleton": [],
                    "common_predicates": [],
                    "predicate_shapes": [],
                    "union_group_by_exprs": ["date_dim.d_year"],
                    "union_measure_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                }
            ]
        },
    )
    historical_rewrite_dir = write_original_equivalent_historical_rewrite(store, 3, query_ids)
    return batches_path, query_blocks_path, families_path, historical_rewrite_dir


def mv_rewrite_state(store: ArtifactStore, mv_id: str = "mv_ok") -> Path:
    return store.write_json(
        "04_batch_mvs/materialized_mvs.json",
        {
            "materialized_mvs": [
                {
                    "mv_id": mv_id,
                    "table_name": mv_id,
                    "source_candidate_id": "cand_ok",
                    "source_batch_id": 3,
                    "available_from_batch": 3,
                    "family_id": "family_fact_ss_dd_item_manager_moy_year",
                    "target_queries": ["q42", "q52"],
                    "depends_on_mv_ids": [],
                    "mv_type": "fine_grain_aggregate",
                    "output_columns": [
                        "d_year",
                        "i_category_id",
                        "i_category",
                        "i_brand_id",
                        "i_brand",
                        "sum_ss_ext_sales_price",
                    ],
                    "column_mappings": [
                        {
                            "source_expr": "date_dim.d_year",
                            "source_table": "date_dim",
                            "source_column": "d_year",
                            "mv_column": "d_year",
                            "role": "dimension",
                        },
                        {
                            "source_expr": "item.i_category_id",
                            "source_table": "item",
                            "source_column": "i_category_id",
                            "mv_column": "i_category_id",
                            "role": "dimension",
                        },
                        {
                            "source_expr": "item.i_category",
                            "source_table": "item",
                            "source_column": "i_category",
                            "mv_column": "i_category",
                            "role": "dimension",
                        },
                        {
                            "source_expr": "item.i_brand_id",
                            "source_table": "item",
                            "source_column": "i_brand_id",
                            "mv_column": "i_brand_id",
                            "role": "dimension",
                        },
                        {
                            "source_expr": "item.i_brand",
                            "source_table": "item",
                            "source_column": "i_brand",
                            "mv_column": "i_brand",
                            "role": "dimension",
                        },
                        {
                            "source_expr": "SUM(store_sales.ss_ext_sales_price)",
                            "source_table": "store_sales",
                            "source_column": "ss_ext_sales_price",
                            "mv_column": "sum_ss_ext_sales_price",
                            "role": "measure",
                        },
                    ],
                    "build_sql_path": str(store.path("04_batch_mvs/batch_3_mv_build.sql")),
                }
            ]
        },
    )


def rewrite_record(query_id: str, sql: str, status: str = "rewritten") -> dict:
    return {
        "query_id": query_id,
        "rewrite_stage": "final",
        "status": status,
        "used_mv_ids": ["mv_ok"] if status == "rewritten" else [],
        "original_sql_path": str(PROJECT_ROOT / "tpcds-spark" / f"{query_id}.sql"),
        "rewritten_sql_path": "",
        "rewrite_meta_path": "",
        "rewrite_mode": "mv_filter_projection_rollup" if status == "rewritten" else "original_equivalent",
        "rewritten_sql": sql,
        "residual_filters": [],
        "rollup_exprs": [],
        "semantic_check": {"status": "pass" if status == "rewritten" else "fallback", "reason": "test"},
        "fallback_reason": None if status == "rewritten" else "no_matching_mv",
    }


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


def test_sql_loader_accepts_workflow_directory_with_stable_manifest_order() -> None:
    store = make_store("test_sql_loader_directory")
    manifest_path = SQLLoaderAgent(store).run(PROJECT_ROOT / "tpcds-spark")

    manifest = store.read_json(manifest_path)
    query_ids = [query["query_id"] for query in manifest["queries"]]
    assert len(query_ids) > len(SQL_PATHS)
    assert query_ids == sorted(query_ids)
    assert "sql_text" not in manifest["queries"][0]
    assert not any((store.path("00_raw_sql") / f"{query_id}.sql").exists() for query_id in query_ids[:3])


def feature_output(query_id: str, unsupported_reasons: list[str] | None = None) -> dict:
    return {
        "query_blocks": [
            {
                "qb_id": f"{query_id}.outer",
                "query_id": query_id,
                "block_name": "outer",
                "scope_type": "outer",
                "parent_qb_id": None,
                "depends_on_qb_ids": [],
                "structural_flags": [],
                "tables": ["date_dim", "store_sales", "item"],
                "join_edges": [
                    "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
                    "store_sales.ss_item_sk = item.i_item_sk",
                ],
                "predicates": ["date_dim.d_year = 2000"],
                "group_by_exprs": ["date_dim.d_year"],
                "aggregate_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                "complexity_type": "join_filter_groupby",
                "family_key": "store_sales-date_dim-item",
                "unsupported_reasons": unsupported_reasons or [],
            }
        ],
        "query_to_qbs": {query_id: [f"{query_id}.outer"]},
        "qb_to_query": {f"{query_id}.outer": query_id},
    }


def test_query_analysis_runner_serial_retry_and_resume_feature() -> None:
    class ScriptedFeatureAgent:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int]] = []

        def extract_one(self, query: dict, attempt: int = 1) -> dict:
            self.calls.append((query["query_id"], attempt))
            if query["query_id"] == "q52" and attempt == 1:
                raise RuntimeError("transient LLM timeout")
            return feature_output(query["query_id"])

    store = make_store("test_query_analysis_runner")
    manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)
    feature_agent = ScriptedFeatureAgent()

    query_blocks_path = QueryAnalysisRunner(store, feature_agent).run_all(manifest_path)

    assert feature_agent.calls == [("q42", 1), ("q52", 1), ("q52", 2)]
    assert query_blocks_path == store.query_blocks_path
    status = store.read_json(store.feature_status_path)
    assert {item["query_id"]: item["status"] for item in status["queries"]} == {"q42": "success", "q52": "success"}
    assert store.read_json(store.query_to_qbs_path) == {"q42": ["q42.outer"], "q52": ["q52.outer"]}
    assert store.read_json(store.qb_to_query_path) == {"q42.outer": "q42", "q52.outer": "q52"}

    class FailingFeatureAgent:
        def extract_one(self, query: dict, attempt: int = 1) -> dict:
            raise AssertionError("resume_feature should skip successful queries")

    QueryAnalysisRunner(store, FailingFeatureAgent()).run_all(manifest_path, resume_feature=True)


def test_family_candidate_builder_groups_query_blocks_without_pair_matrix() -> None:
    store = make_store("test_family_candidate_builder")
    query_blocks_path = store.write_json(
        "01_query_blocks/query_blocks.json",
        {
            "query_blocks": [
                feature_output("q42")["query_blocks"][0],
                feature_output("q52")["query_blocks"][0],
                {
                    **feature_output("q_bad", ["has_correlated_subquery"])["query_blocks"][0],
                    "tables": ["date_dim", "store_sales", "item"],
                },
            ]
        },
    )

    candidates_path = FamilyCandidateBuilder(store).run(query_blocks_path)
    candidates = store.read_json(candidates_path)["family_candidates"]

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["core_fact_table_set"] == ["store_sales"]
    assert {member["qb_id"] for member in candidate["members"]} == {"q42.outer", "q52.outer"}
    assert candidate["unsupported_qb_ids"] == ["q_bad.outer"]
    assert len(candidate["pair_evidence"]) == 1


def write_batch_cluster_inputs(
    store: ArtifactStore,
    query_blocks: list[dict],
    query_to_qbs: dict[str, list[str]],
    query_families: list[dict],
) -> tuple[Path, Path]:
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": query_blocks})
    store.write_json("01_query_blocks/query_to_qbs.json", query_to_qbs)
    store.write_json(
        "01_query_blocks/qb_to_query.json",
        {qb_id: query_id for query_id, qb_ids in query_to_qbs.items() for qb_id in qb_ids},
    )
    families_path = store.write_json("02_families/query_families.json", {"query_families": query_families})
    return query_blocks_path, families_path


def batch_cluster_output(
    assignments: dict[int, list[str]],
    family_groups_by_batch: dict[int, list[dict]] | None = None,
) -> dict:
    family_groups_by_batch = family_groups_by_batch or {}
    batches = []
    for batch_id, batch_type in (
        (1, "Batch-1"),
        (2, "Batch-2"),
        (3, "Batch-3"),
        (4, "Batch-4"),
        (5, "Batch-5"),
        (6, "Other"),
    ):
        batches.append(
            {
                "batch_id": batch_id,
                "batch_type": batch_type,
                "query_ids": assignments.get(batch_id, []),
                "family_groups": family_groups_by_batch.get(batch_id, []),
            }
        )
    return {"complexity_batches": batches}


def test_batch_cluster_uses_classification_csv_labels_as_batch_type() -> None:
    store = make_store("test_batch_cluster_csv_labels")
    sql_manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)
    query_blocks_path, families_path = write_batch_cluster_inputs(
        store,
        query_blocks=[
            {
                "qb_id": "q42.outer",
                "query_id": "q42",
                "scope_type": "outer",
                "tables": ["date_dim", "store_sales", "item"],
                "join_edges": ["store_sales.ss_item_sk = item.i_item_sk"],
                "predicates": ["date_dim.d_year = 2000"],
                "group_by_exprs": ["date_dim.d_year"],
                "aggregate_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                "complexity_type": "join_filter_groupby",
                "family_key": "store_sales-date_dim-item",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q52.outer",
                "query_id": "q52",
                "scope_type": "outer",
                "tables": ["date_dim", "store_sales", "item"],
                "join_edges": ["store_sales.ss_item_sk = item.i_item_sk"],
                "predicates": ["date_dim.d_year = 2000"],
                "group_by_exprs": ["date_dim.d_year"],
                "aggregate_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                "complexity_type": "join_filter_groupby",
                "family_key": "store_sales-date_dim-item",
                "unsupported_reasons": [],
            },
        ],
        query_to_qbs={"q42": ["q42.outer"], "q52": ["q52.outer"]},
        query_families=[
            {
                "family_id": "family_store_sales_date_dim_item",
                "family_key": "store_sales-date_dim-item",
                "members": ["q42.outer", "q52.outer"],
            }
        ],
    )
    classification_csv_path = store.write_text(
        "classification.csv",
        "SQL,Batch,Reason\n"
        "q42.sql,Batch-1,seed query\n"
        "q52.sql,Batch-3,changed batch method\n",
    )

    batches_path = BatchClusterAgent(store, StaticLLM([])).run_from_classification_csv(
        classification_csv_path=classification_csv_path,
        sql_manifest_path=sql_manifest_path,
        query_blocks_path=query_blocks_path,
        families_path=families_path,
    )

    batches = {batch["batch_id"]: batch for batch in store.read_json(batches_path)["complexity_batches"]}
    assert [(batch_id, batches[batch_id]["batch_type"]) for batch_id in sorted(batches)] == [
        (1, "Batch-1"),
        (2, "Batch-2"),
        (3, "Batch-3"),
        (4, "Batch-4"),
        (5, "Batch-5"),
        (6, "Other"),
    ]
    assert batches[1]["query_ids"] == ["q42"]
    assert batches[3]["query_ids"] == ["q52"]
    assert batches[1]["family_groups"] == [
        {
            "family_id": "family_store_sales_date_dim_item",
            "query_ids": ["q42"],
            "qb_ids": ["q42.outer"],
        }
    ]
    assert batches[3]["family_groups"] == [
        {
            "family_id": "family_store_sales_date_dim_item",
            "query_ids": ["q52"],
            "qb_ids": ["q52.outer"],
        }
    ]
    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    batch_record = next(record for record in records if record["agent_name"] == "BatchClusterAgent")
    assert batch_record["details"]["mode"] == "classification_csv"
    assert batch_record["details"]["sqlglot_parsed_query_count"] == 2


def test_batch_cluster_csv_does_not_require_families_path() -> None:
    store = make_store("test_batch_cluster_csv_without_families")
    sql_manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)
    query_blocks_path, _families_path = write_batch_cluster_inputs(
        store,
        query_blocks=[
            {
                "qb_id": "q42.outer",
                "query_id": "q42",
                "scope_type": "outer",
                "tables": ["date_dim", "store_sales", "item"],
                "join_edges": ["store_sales.ss_item_sk = item.i_item_sk"],
                "predicates": ["date_dim.d_year = 2000"],
                "group_by_exprs": ["date_dim.d_year"],
                "aggregate_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q52.outer",
                "query_id": "q52",
                "scope_type": "outer",
                "tables": ["date_dim", "store_sales", "item"],
                "join_edges": ["store_sales.ss_item_sk = item.i_item_sk"],
                "predicates": ["date_dim.d_year = 2000"],
                "group_by_exprs": ["date_dim.d_year"],
                "aggregate_exprs": ["SUM(store_sales.ss_ext_sales_price)"],
                "unsupported_reasons": [],
            },
        ],
        query_to_qbs={"q42": ["q42.outer"], "q52": ["q52.outer"]},
        query_families=[],
    )
    classification_csv_path = store.write_text(
        "classification.csv",
        "SQL,Batch,Reason\n"
        "q42.sql,Batch-1,seed query\n"
        "q52.sql,Batch-3,changed batch method\n",
    )

    batches_path = BatchClusterAgent(store, StaticLLM([])).run_from_classification_csv(
        classification_csv_path=classification_csv_path,
        sql_manifest_path=sql_manifest_path,
        query_blocks_path=query_blocks_path,
    )

    batches = {batch["batch_id"]: batch for batch in store.read_json(batches_path)["complexity_batches"]}
    assert batches[1]["query_ids"] == ["q42"]
    assert batches[3]["query_ids"] == ["q52"]
    assert all(not batch["family_groups"] for batch in batches.values())


def test_batch_cluster_accepts_q1_batch_3_from_rules() -> None:
    store = make_store("test_batch_cluster_q1_rules")
    query_blocks_path, families_path = write_batch_cluster_inputs(
        store,
        query_blocks=[
            {
                "qb_id": "q1.cte.customer_total_return",
                "query_id": "q1",
                "scope_type": "cte",
                "tables": ["store_returns", "date_dim"],
                "join_edges": ["store_returns.sr_returned_date_sk = date_dim.d_date_sk"],
                "predicates": ["date_dim.d_year = 2000"],
                "group_by_exprs": ["store_returns.sr_customer_sk", "store_returns.sr_store_sk"],
                "aggregate_exprs": ["sum(store_returns.sr_return_amt)"],
                "complexity_type": "join_filter_groupby",
                "family_key": "date_dim-store_returns",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q1.outer",
                "query_id": "q1",
                "scope_type": "outer",
                "tables": ["customer_total_return", "store", "customer"],
                "join_edges": ["store.s_store_sk = customer_total_return.ctr_store_sk"],
                "predicates": ["customer_total_return.ctr_total_return > scalar_subquery"],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join_filter",
                "family_key": "customer-date_dim-store-store_returns",
                "unsupported_reasons": ["CTE-derived correlated subquery cannot be safely normalized"],
            },
        ],
        query_to_qbs={"q1": ["q1.cte.customer_total_return", "q1.outer"]},
        query_families=[
            {
                "family_id": "family_store_returns_date_dim",
                "family_key": "date_dim-store_returns",
                "members": ["q1.cte.customer_total_return", "q1.outer"],
            }
        ],
    )

    llm_output = batch_cluster_output(
        {3: ["q1"]},
        {
            3: [
                {
                    "family_id": "family_store_returns_date_dim",
                    "query_ids": ["q1"],
                    "qb_ids": ["q1.cte.customer_total_return"],
                }
            ]
        },
    )

    batches_path = BatchClusterAgent(store, StaticLLM([llm_output, llm_output])).run(
        query_blocks_path,
        families_path,
    )

    batches = {batch["batch_id"]: batch for batch in store.read_json(batches_path)["complexity_batches"]}
    assert batches[3]["query_ids"] == ["q1"]
    assert batches[4]["query_ids"] == []
    assert batches[3]["family_groups"] == [
        {
            "family_id": "family_store_returns_date_dim",
            "query_ids": ["q1"],
            "qb_ids": ["q1.cte.customer_total_return"],
        }
    ]
    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    batch_record = next(record for record in records if record["agent_name"] == "BatchClusterAgent")
    assert "batch_rules_path" not in batch_record["details"]
    assert str(PROJECT_ROOT / "batch_rules.md") not in batch_record["input_artifact_paths"]


def test_batch_cluster_accepts_q11_cross_channel_sales_as_batch_4() -> None:
    store = make_store("test_batch_cluster_q11_rules")
    query_blocks_path, families_path = write_batch_cluster_inputs(
        store,
        query_blocks=[
            {
                "qb_id": "q11.outer",
                "query_id": "q11",
                "scope_type": "outer",
                "tables": ["year_total"],
                "join_edges": ["year_total.customer_id = year_total.customer_id"],
                "predicates": ["year_total.sale_type = 's'"],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join_filter",
                "family_key": "customer-date_dim-store_sales-web_sales",
                "unsupported_reasons": ["CTE year_total is not a physical table"],
            },
            {
                "qb_id": "q11.year_total.branch1",
                "query_id": "q11",
                "scope_type": "set_branch",
                "tables": ["customer", "store_sales", "date_dim"],
                "join_edges": [
                    "customer.c_customer_sk = store_sales.ss_customer_sk",
                    "store_sales.ss_sold_date_sk = date_dim.d_date_sk",
                ],
                "predicates": [],
                "group_by_exprs": ["customer.c_customer_id", "date_dim.d_year"],
                "aggregate_exprs": ["sum(store_sales.ss_ext_list_price - store_sales.ss_ext_discount_amt)"],
                "complexity_type": "other",
                "family_key": "customer-date_dim-store_sales",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q11.year_total.branch2",
                "query_id": "q11",
                "scope_type": "set_branch",
                "tables": ["customer", "web_sales", "date_dim"],
                "join_edges": [
                    "customer.c_customer_sk = web_sales.ws_bill_customer_sk",
                    "web_sales.ws_sold_date_sk = date_dim.d_date_sk",
                ],
                "predicates": [],
                "group_by_exprs": ["customer.c_customer_id", "date_dim.d_year"],
                "aggregate_exprs": ["sum(web_sales.ws_ext_list_price - web_sales.ws_ext_discount_amt)"],
                "complexity_type": "other",
                "family_key": "customer-date_dim-web_sales",
                "unsupported_reasons": [],
            },
        ],
        query_to_qbs={"q11": ["q11.outer", "q11.year_total.branch1", "q11.year_total.branch2"]},
        query_families=[
            {
                "family_id": "family_customer_store_sales_date_dim",
                "family_key": "customer-date_dim-store_sales",
                "members": ["q11.year_total.branch1"],
            },
            {
                "family_id": "family_customer_web_sales_date_dim",
                "family_key": "customer-date_dim-web_sales",
                "members": ["q11.year_total.branch2"],
            },
        ],
    )

    llm_output = batch_cluster_output(
        {4: ["q11"]},
        {
            4: [
                {
                    "family_id": "family_customer_store_sales_date_dim",
                    "query_ids": ["q11"],
                    "qb_ids": ["q11.year_total.branch1"],
                },
                {
                    "family_id": "family_customer_web_sales_date_dim",
                    "query_ids": ["q11"],
                    "qb_ids": ["q11.year_total.branch2"],
                },
            ]
        },
    )

    batches_path = BatchClusterAgent(store, StaticLLM([llm_output, llm_output])).run(
        query_blocks_path,
        families_path,
    )

    batches = {batch["batch_id"]: batch for batch in store.read_json(batches_path)["complexity_batches"]}
    assert batches[3]["query_ids"] == []
    assert batches[4]["query_ids"] == ["q11"]
    assert {
        group["family_id"]: group["qb_ids"]
        for group in batches[4]["family_groups"]
    } == {
        "family_customer_store_sales_date_dim": ["q11.year_total.branch1"],
        "family_customer_web_sales_date_dim": ["q11.year_total.branch2"],
    }


def test_batch_cluster_uses_full_sql_complexity_even_when_mv_blocks_are_unsupported() -> None:
    store = make_store("test_batch_cluster_full_sql_complexity")
    query_blocks_path, families_path = write_batch_cluster_inputs(
        store,
        query_blocks=[
            {
                "qb_id": "q44.outer",
                "query_id": "q44",
                "scope_type": "outer",
                "tables": ["item"],
                "join_edges": ["item.i_item_sk = asceding.item_sk"],
                "predicates": [],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join",
                "family_key": "item-derived_rank",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q44.subquery.rank_source",
                "query_id": "q44",
                "scope_type": "subquery",
                "tables": ["store_sales"],
                "join_edges": [],
                "predicates": ["store_sales.ss_store_sk = 4"],
                "group_by_exprs": ["store_sales.ss_item_sk"],
                "aggregate_exprs": ["avg(store_sales.ss_net_profit)"],
                "structural_flags": ["has_window", "has_having"],
                "complexity_type": "other",
                "family_key": "store_sales",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q5.cte.ssr",
                "query_id": "q5",
                "scope_type": "cte",
                "tables": ["store_sales", "store_returns", "date_dim", "store"],
                "join_edges": [
                    "salesreturns.date_sk = date_dim.d_date_sk",
                    "salesreturns.store_sk = store.s_store_sk",
                ],
                "predicates": ["date_dim.d_date BETWEEN start_date AND end_date"],
                "group_by_exprs": ["store.s_store_id"],
                "aggregate_exprs": ["sum(sales_price)"],
                "structural_flags": ["has_group_by"],
                "complexity_type": "join_filter_groupby",
                "family_key": "store_sales-store_returns-date_dim-store",
                "unsupported_reasons": ["salesreturns is a derived union output"],
            },
            {
                "qb_id": "q5.wsr.salesreturns.branch2",
                "query_id": "q5",
                "scope_type": "set_branch",
                "tables": ["web_returns", "web_sales"],
                "join_edges": [
                    "web_returns.wr_item_sk = web_sales.ws_item_sk",
                    "web_returns.wr_order_number = web_sales.ws_order_number",
                ],
                "predicates": [],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join",
                "family_key": "web_returns-web_sales",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q38.outer",
                "query_id": "q38",
                "scope_type": "outer",
                "tables": [],
                "join_edges": [],
                "predicates": [],
                "group_by_exprs": [],
                "aggregate_exprs": ["count(*)"],
                "complexity_type": "other",
                "family_key": "",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q38.set_branch.store",
                "query_id": "q38",
                "scope_type": "set_branch",
                "tables": ["store_sales", "date_dim", "customer"],
                "join_edges": [
                    "store_sales.ss_sold_date_sk = date_dim.d_date_sk",
                    "store_sales.ss_customer_sk = customer.c_customer_sk",
                ],
                "predicates": ["date_dim.d_month_seq BETWEEN 1200 AND 1211"],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join_filter",
                "family_key": "store_sales-date_dim-customer",
                "unsupported_reasons": [],
            },
        ],
        query_to_qbs={
            "q44": ["q44.outer", "q44.subquery.rank_source"],
            "q5": ["q5.cte.ssr", "q5.wsr.salesreturns.branch2"],
            "q38": ["q38.outer", "q38.set_branch.store"],
        },
        query_families=[
            {
                "family_id": "family_empty_no_tables",
                "family_key": "",
                "members": ["q38.outer"],
            },
            {
                "family_id": "family_item_rank_join",
                "family_key": "item-derived_rank",
                "members": ["q44.outer"],
            },
            {
                "family_id": "family_q5_unsupported_cte",
                "family_key": "store_sales-store_returns-date_dim-store",
                "members": ["q5.cte.ssr"],
            },
            {
                "family_id": "family_web_returns_web_sales",
                "family_key": "web_returns-web_sales",
                "members": ["q5.wsr.salesreturns.branch2"],
            },
            {
                "family_id": "family_store_sales_date_dim_customer",
                "family_key": "store_sales-date_dim-customer",
                "members": ["q38.set_branch.store"],
            },
        ],
    )

    llm_output = batch_cluster_output(
        {5: ["q38", "q44", "q5"]},
        {
            5: [
                {
                    "family_id": "family_item_rank_join",
                    "query_ids": ["q44"],
                    "qb_ids": ["q44.outer"],
                },
                {
                    "family_id": "family_web_returns_web_sales",
                    "query_ids": ["q5"],
                    "qb_ids": ["q5.wsr.salesreturns.branch2"],
                },
                {
                    "family_id": "family_store_sales_date_dim_customer",
                    "query_ids": ["q38"],
                    "qb_ids": ["q38.set_branch.store"],
                },
            ]
        },
    )

    batches_path = BatchClusterAgent(store, StaticLLM([llm_output, llm_output])).run(
        query_blocks_path,
        families_path,
    )

    batches = {batch["batch_id"]: batch for batch in store.read_json(batches_path)["complexity_batches"]}
    assert batches[1]["query_ids"] == []
    assert batches[2]["query_ids"] == []
    assert batches[5]["query_ids"] == ["q38", "q44", "q5"]
    assert {
        group["family_id"]: group["qb_ids"]
        for group in batches[5]["family_groups"]
    } == {
        "family_item_rank_join": ["q44.outer"],
        "family_web_returns_web_sales": ["q5.wsr.salesreturns.branch2"],
        "family_store_sales_date_dim_customer": ["q38.set_branch.store"],
    }
    assert all(
        "q5.cte.ssr" not in group["qb_ids"]
        for batch in batches.values()
        for group in batch.get("family_groups", [])
    )
    assert all(
        "q38.outer" not in group["qb_ids"]
        for group in batches[5].get("family_groups", [])
    )


def test_batch_cluster_normalizes_duplicate_family_ids_without_merging_different_families() -> None:
    store = make_store("test_batch_cluster_family_collision")
    query_blocks_path, families_path = write_batch_cluster_inputs(
        store,
        query_blocks=[
            {
                "qb_id": "q34.outer",
                "query_id": "q34",
                "scope_type": "outer",
                "tables": ["customer", "store_sales"],
                "join_edges": ["store_sales.ss_customer_sk = customer.c_customer_sk"],
                "predicates": ["cnt BETWEEN 15 AND 20"],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join_filter",
                "family_key": "customer-store_sales",
                "unsupported_reasons": [],
            },
            {
                "qb_id": "q46.outer",
                "query_id": "q46",
                "scope_type": "outer",
                "tables": ["customer", "customer_address", "store_sales"],
                "join_edges": [
                    "customer.c_current_addr_sk = customer_address.ca_address_sk",
                    "store_sales.ss_customer_sk = customer.c_customer_sk",
                ],
                "predicates": ["customer_address.ca_city <> bought_city"],
                "group_by_exprs": [],
                "aggregate_exprs": [],
                "complexity_type": "join_filter",
                "family_key": "customer-customer_address-store_sales",
                "unsupported_reasons": [],
            },
        ],
        query_to_qbs={"q34": ["q34.outer"], "q46": ["q46.outer"]},
        query_families=[
            {
                "family_id": "family_customer_customer_address_dn",
                "family_key": "customer-store_sales",
                "members": ["q34.outer"],
            },
            {
                "family_id": "family_customer_customer_address_dn",
                "family_key": "customer-customer_address-store_sales",
                "members": ["q46.outer"],
            },
        ],
    )

    llm_output = batch_cluster_output(
        {2: ["q34", "q46"]},
        {
            2: [
                {
                    "family_id": "family_customer_customer_address_dn",
                    "query_ids": ["q34"],
                    "qb_ids": ["q34.outer"],
                },
                {
                    "family_id": "family_customer_customer_address_dn__2",
                    "query_ids": ["q46"],
                    "qb_ids": ["q46.outer"],
                },
            ]
        },
    )

    batches_path = BatchClusterAgent(store, StaticLLM([llm_output, llm_output])).run(
        query_blocks_path,
        families_path,
    )

    normalized_families = store.read_json(families_path)["query_families"]
    assert [family["family_id"] for family in normalized_families] == [
        "family_customer_customer_address_dn",
        "family_customer_customer_address_dn__2",
    ]
    batches = {batch["batch_id"]: batch for batch in store.read_json(batches_path)["complexity_batches"]}
    assert {
        group["family_id"]: group["qb_ids"]
        for group in batches[2]["family_groups"]
    } == {
        "family_customer_customer_address_dn": ["q34.outer"],
        "family_customer_customer_address_dn__2": ["q46.outer"],
    }
    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    batch_record = next(record for record in records if record["agent_name"] == "BatchClusterAgent")
    assert batch_record["details"]["family_normalization_events"] == [
        {
            "action": "family_id_collision_fallback_rename",
            "original_family_id": "family_customer_customer_address_dn",
            "normalized_family_id": "family_customer_customer_address_dn__2",
            "members": ["q46.outer"],
        }
    ]


def test_executor_agent_dry_run_materializes_available_mvs_and_orders_queries() -> None:
    store = make_store("test_executor_dry_run")
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q52", "q42"])
    candidates_path = store.write_json(
        "04_batch_mvs/batch_3_mv_candidates.json",
        {
            "batch_id": 3,
            "mv_candidates": [
                {
                    "candidate_id": "cand_ok",
                    "mv_id": "mv_ok",
                    "source_batch_id": 3,
                    "source_query_ids": ["q42"],
                    "source_qb_ids": ["q42.outer"],
                    "family_id": "family_fact_ss_dd_item_manager_moy_year",
                    "mv_type": "fine_grain_aggregate",
                    "target_table_name": "mv_ok",
                    "target_queries": ["q42"],
                    "target_qb_ids": ["q42.outer"],
                    "depends_on_mv_ids": [],
                    "mv_predicates": ["date_dim.d_year = 2000"],
                    "generalized_predicates": [
                        {
                            "predicate_shape": "date_dim.d_year = <CONST>",
                            "covered_values": [2000],
                            "source_query_ids": ["q42"],
                        }
                    ],
                    "residual_filters": [{"query_id": "q42", "predicates": []}],
                    "group_by_exprs": ["date_dim.d_year"],
                    "measure_exprs": ["SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price"],
                    "output_columns": ["d_year", "sum_ss_ext_sales_price"],
                    "column_mappings": [
                        {
                            "source_expr": "date_dim.d_year",
                            "source_table": "date_dim",
                            "source_column": "d_year",
                            "mv_column": "d_year",
                            "role": "dimension",
                        },
                        {
                            "source_expr": "SUM(store_sales.ss_ext_sales_price)",
                            "source_table": "store_sales",
                            "source_column": "ss_ext_sales_price",
                            "mv_column": "sum_ss_ext_sales_price",
                            "role": "measure",
                        },
                    ],
                    "build_sql": "CREATE TABLE mv_ok AS SELECT 1",
                    "decision": "materialize",
                    "reason": "dry-run success",
                },
                {
                    "candidate_id": "cand_missing_dep",
                    "mv_id": "mv_missing_dep",
                    "source_batch_id": 3,
                    "source_query_ids": ["q52"],
                    "source_qb_ids": ["q52.outer"],
                    "family_id": "family_fact_ss_dd_item_manager_moy_year",
                    "target_table_name": "mv_missing_dep",
                    "target_queries": ["q52"],
                    "target_qb_ids": ["q52.outer"],
                    "depends_on_mv_ids": ["mv_not_available"],
                    "build_sql": "CREATE TABLE mv_missing_dep AS SELECT 1",
                    "decision": "materialize",
                    "reason": "missing dependency",
                },
            ],
        },
    )
    build_sql_path = store.write_text("04_batch_mvs/batch_3_mv_build.sql", "CREATE TABLE mv_ok AS SELECT 1")

    materialized_mvs_path = ExecutorAgent(store).materialize_mvs(3, candidates_path, build_sql_path)
    materialized = store.read_json(materialized_mvs_path)["materialized_mvs"]
    assert [mv["mv_id"] for mv in materialized] == ["mv_ok"]
    assert materialized[0]["mv_predicates"] == ["date_dim.d_year = 2000"]
    assert materialized[0]["generalized_predicates"] == [
        {
            "predicate_shape": "date_dim.d_year = <CONST>",
            "covered_values": [2000],
            "source_query_ids": ["q42"],
        }
    ]
    assert materialized[0]["residual_filters"] == [{"query_id": "q42", "predicates": []}]

    final_rewrite_dir = store.ensure_dir("05_rewritten_sql/batch_3/final_rewrite")
    for query_id in ("q42", "q52"):
        rewritten_sql_path = final_rewrite_dir / f"{query_id}_rewritten.sql"
        rewritten_sql_path.write_text(f"SELECT '{query_id}'", encoding="utf-8")
        used_mv_ids = ["mv_ok"] if query_id == "q42" else []
        (final_rewrite_dir / f"{query_id}_rewrite_meta.json").write_text(
            json.dumps(
                {
                    "query_id": query_id,
                    "rewrite_stage": "final",
                    "status": "rewritten" if used_mv_ids else "fallback",
                    "used_mv_ids": used_mv_ids,
                    "fallback_reason": None if used_mv_ids else "no_matching_mv",
                    "rewritten_sql_path": str(rewritten_sql_path),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    order_path = ExecutorAgent(store).run_queries(3, final_rewrite_dir, batches_path)
    order = store.read_json(order_path)
    query_steps = [step for step in order["steps"] if step["step_type"] == "run_query"]
    assert [step["query_id"] for step in query_steps] == ["q52", "q42"]
    assert {step["query_id"]: step["depends_on_mv_ids"] for step in query_steps} == {
        "q52": [],
        "q42": ["mv_ok"],
    }
    mv_steps = [step for step in order["steps"] if step["step_type"] == "materialize_mv"]
    assert {step["candidate_id"]: step["status"] for step in mv_steps} == {
        "cand_ok": "success",
        "cand_missing_dep": "failed",
    }


def test_executor_agent_requires_used_mv_ids_in_rewrite_meta() -> None:
    store = make_store("test_executor_missing_used_mv_ids")
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    final_rewrite_dir = store.ensure_dir("05_rewritten_sql/batch_3/final_rewrite")
    (final_rewrite_dir / "q42_rewritten.sql").write_text("SELECT 1", encoding="utf-8")
    (final_rewrite_dir / "q42_rewrite_meta.json").write_text(
        json.dumps({"query_id": "q42", "rewrite_stage": "final", "status": "fallback"}, ensure_ascii=False),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="used_mv_ids"):
        ExecutorAgent(store).run_queries(3, final_rewrite_dir, batches_path)


def test_coverage_summary_builder_handles_partial_artifacts() -> None:
    store = make_store("test_coverage_summary")
    SQLLoaderAgent(store).run(SQL_PATHS)
    store.write_json(
        "01_query_blocks/feature_extract_status.json",
        {
            "run_id": store.run_id,
            "queries": [
                {"query_id": "q42", "status": "success", "attempts": 1, "qb_ids": ["q42.outer"]},
                {
                    "query_id": "q52",
                    "status": "feature_failed",
                    "attempts": 2,
                    "qb_ids": [],
                    "error_type": "RuntimeError",
                    "error_message": "timeout",
                },
            ],
        },
    )
    store.write_json("02_families/family_candidates.json", {"family_candidates": [{"candidate_family_id": "candidate_1"}]})
    store.write_json("02_families/query_families.json", {"query_families": [{"family_id": "family_1", "members": ["q42.outer"]}]})
    write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])

    coverage_path = CoverageSummaryBuilder(store).run()
    coverage = store.read_json(coverage_path)

    assert coverage["run_id"] == store.run_id
    assert coverage["total_queries"] == 2
    assert coverage["feature_status_counts"] == {"success": 1, "feature_failed": 1}
    assert coverage["family_candidate_count"] == 1
    assert coverage["family_count"] == 1
    assert coverage["batch_query_counts"]["3"] == 1
    assert coverage["notes"] == ["feature_failed_or_unsupported_queries=1"]


def test_batch_mv_agent_rejects_materialize_candidate_without_column_mappings() -> None:
    store = make_store("test_batch_mv_missing_mappings")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_missing_mappings",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "source_qb_ids": ["q42.outer"],
                "family_id": "family_fact_ss_dd_item_manager_moy_year",
                "target_queries": ["q42"],
                "target_qb_ids": ["q42.outer"],
                "decision": "materialize",
                "reason": "test",
                "mv_id": "mv_missing_mappings",
                "mv_type": "fine_grain_aggregate",
                "target_table_name": "mv_missing_mappings",
                "depends_on_mv_ids": [],
                "output_columns": ["d_year"],
                "group_by_exprs": ["date_dim.d_year"],
                "measure_exprs": [],
                "build_sql": "CREATE TABLE mv_missing_mappings AS SELECT date_dim.d_year FROM date_dim",
            }
        ],
    }

    with pytest.raises(ValueError, match="column_mappings"):
        BatchMVAgent(store, StaticLLM([output, output])).run(
            3,
            batches_path,
            query_blocks_path,
            families_path,
            historical_rewrite_dir,
        )


def test_batch_mv_agent_rejects_dotted_mv_output_columns() -> None:
    store = make_store("test_batch_mv_dotted_columns")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_dotted_columns",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "source_qb_ids": ["q42.outer"],
                "family_id": "family_fact_ss_dd_item_manager_moy_year",
                "target_queries": ["q42"],
                "target_qb_ids": ["q42.outer"],
                "decision": "materialize",
                "reason": "test",
                "mv_id": "mv_dotted_columns",
                "mv_type": "fine_grain_aggregate",
                "target_table_name": "mv_dotted_columns",
                "depends_on_mv_ids": [],
                "output_columns": ["date_dim.d_year"],
                "column_mappings": [
                    {
                        "source_expr": "date_dim.d_year",
                        "source_table": "date_dim",
                        "source_column": "d_year",
                        "mv_column": "d_year",
                        "role": "dimension",
                    }
                ],
                "group_by_exprs": ["date_dim.d_year"],
                "measure_exprs": [],
                "build_sql": "CREATE TABLE mv_dotted_columns AS SELECT date_dim.d_year FROM date_dim",
            }
        ],
    }

    with pytest.raises(ValueError, match="output_columns"):
        BatchMVAgent(store, StaticLLM([output, output])).run(
            3,
            batches_path,
            query_blocks_path,
            families_path,
            historical_rewrite_dir,
        )


def test_batch_mv_agent_rejects_create_or_replace_table() -> None:
    store = make_store("test_batch_mv_create_table")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_create_table",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "source_qb_ids": ["q42.outer"],
                "family_id": "family_fact_ss_dd_item_manager_moy_year",
                "target_queries": ["q42"],
                "target_qb_ids": ["q42.outer"],
                "decision": "materialize",
                "reason": "test",
                "mv_id": "mv_create_table",
                "mv_type": "fine_grain_aggregate",
                "target_table_name": "mv_create_table",
                "depends_on_mv_ids": [],
                "output_columns": ["d_year"],
                "column_mappings": [
                    {
                        "source_expr": "date_dim.d_year",
                        "source_table": "date_dim",
                        "source_column": "d_year",
                        "mv_column": "d_year",
                        "role": "dimension",
                    }
                ],
                "group_by_exprs": ["date_dim.d_year"],
                "measure_exprs": [],
                "build_sql": "CREATE OR REPLACE TABLE mv_create_table AS SELECT date_dim.d_year FROM date_dim",
            }
        ],
    }

    with pytest.raises(ValueError, match="CREATE OR REPLACE TABLE"):
        BatchMVAgent(store, StaticLLM([output, output])).run(
            3,
            batches_path,
            query_blocks_path,
            families_path,
            historical_rewrite_dir,
        )


def test_batch_mv_agent_rejects_measure_alias_without_source_column_prefix() -> None:
    store = make_store("test_batch_mv_measure_alias")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_bad_measure_alias",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "source_qb_ids": ["q42.outer"],
                "family_id": "family_fact_ss_dd_item_manager_moy_year",
                "target_queries": ["q42"],
                "target_qb_ids": ["q42.outer"],
                "decision": "materialize",
                "reason": "test",
                "mv_id": "mv_bad_measure_alias",
                "mv_type": "fine_grain_aggregate",
                "target_table_name": "mv_bad_measure_alias",
                "depends_on_mv_ids": [],
                "output_columns": ["d_year", "sum_ext_sales_price"],
                "column_mappings": [
                    {
                        "source_expr": "date_dim.d_year",
                        "source_table": "date_dim",
                        "source_column": "d_year",
                        "mv_column": "d_year",
                        "role": "dimension",
                    },
                    {
                        "source_expr": "SUM(store_sales.ss_ext_sales_price)",
                        "source_table": "store_sales",
                        "source_column": "ss_ext_sales_price",
                        "mv_column": "sum_ext_sales_price",
                        "role": "measure",
                    },
                ],
                "group_by_exprs": ["date_dim.d_year"],
                "measure_exprs": ["SUM(store_sales.ss_ext_sales_price) AS sum_ext_sales_price"],
                "build_sql": (
                    "CREATE TABLE mv_bad_measure_alias AS SELECT date_dim.d_year, "
                    "SUM(store_sales.ss_ext_sales_price) AS sum_ext_sales_price FROM date_dim "
                    "JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk GROUP BY date_dim.d_year"
                ),
            }
        ],
    }

    with pytest.raises(ValueError, match="sum_ss_ext_sales_price"):
        BatchMVAgent(store, StaticLLM([output, output])).run(
            3,
            batches_path,
            query_blocks_path,
            families_path,
            historical_rewrite_dir,
        )


def test_batch_mv_agent_rejects_unsupported_source_query_block() -> None:
    store = make_store("test_batch_mv_unsupported_qb")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    query_blocks = store.read_json(query_blocks_path)
    query_blocks["query_blocks"][0]["unsupported_reasons"] = ["has_correlated_subquery"]
    store.write_json("01_query_blocks/query_blocks.json", query_blocks)
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_unsupported_qb",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "source_qb_ids": ["q42.outer"],
                "family_id": "family_fact_ss_dd_item_manager_moy_year",
                "target_queries": ["q42"],
                "target_qb_ids": ["q42.outer"],
                "decision": "materialize",
                "reason": "test",
                "mv_id": "mv_unsupported_qb",
                "mv_type": "fine_grain_aggregate",
                "target_table_name": "mv_unsupported_qb",
                "depends_on_mv_ids": [],
                "output_columns": ["d_year"],
                "column_mappings": [
                    {
                        "source_expr": "date_dim.d_year",
                        "source_table": "date_dim",
                        "source_column": "d_year",
                        "mv_column": "d_year",
                        "role": "dimension",
                    }
                ],
                "group_by_exprs": ["date_dim.d_year"],
                "measure_exprs": [],
                "build_sql": "CREATE TABLE mv_unsupported_qb AS SELECT date_dim.d_year FROM date_dim",
            }
        ],
    }

    with pytest.raises(ValueError, match="unsupported QueryBlock"):
        BatchMVAgent(store, StaticLLM([output, output])).run(
            3,
            batches_path,
            query_blocks_path,
            families_path,
            historical_rewrite_dir,
        )


def test_batch_mv_agent_rejects_mv_dependency_cycle() -> None:
    store = make_store("test_batch_mv_dependency_cycle")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    materialized_mvs_path = store.write_json(
        "04_batch_mvs/materialized_mvs.json",
        {
            "materialized_mvs": [
                {
                    "mv_id": "mv_parent",
                    "available_from_batch": 2,
                    "depends_on_mv_ids": ["mv_cycle"],
                    "output_columns": ["d_year"],
                    "column_mappings": [],
                }
            ]
        },
    )
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_cycle",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "source_qb_ids": ["q42.outer"],
                "family_id": "family_fact_ss_dd_item_manager_moy_year",
                "target_queries": ["q42"],
                "target_qb_ids": ["q42.outer"],
                "decision": "materialize",
                "reason": "test",
                "mv_id": "mv_cycle",
                "mv_type": "fine_grain_aggregate",
                "target_table_name": "mv_cycle",
                "depends_on_mv_ids": ["mv_parent"],
                "output_columns": ["d_year"],
                "column_mappings": [
                    {
                        "source_expr": "date_dim.d_year",
                        "source_table": "date_dim",
                        "source_column": "d_year",
                        "mv_column": "d_year",
                        "role": "dimension",
                    }
                ],
                "group_by_exprs": ["date_dim.d_year"],
                "measure_exprs": [],
                "build_sql": "CREATE TABLE mv_cycle AS SELECT date_dim.d_year FROM date_dim",
            }
        ],
    }

    with pytest.raises(ValueError, match="dependency cycle"):
        BatchMVAgent(store, StaticLLM([output, output])).run(
            3,
            batches_path,
            query_blocks_path,
            families_path,
            historical_rewrite_dir,
            materialized_mvs_path,
        )


def test_batch_mv_agent_accepts_batch_local_candidate_without_family_groups() -> None:
    store = make_store("test_batch_mv_batch_local_no_family_groups")
    batches_path = store.write_json(
        "03_batches/complexity_batches.json",
        {
            "complexity_batches": [
                {"batch_id": 1, "batch_type": "Batch-1", "query_ids": [], "family_groups": []},
                {"batch_id": 2, "batch_type": "Batch-2", "query_ids": [], "family_groups": []},
                {"batch_id": 3, "batch_type": "Batch-3", "query_ids": [], "family_groups": []},
                {"batch_id": 4, "batch_type": "Batch-4", "query_ids": [], "family_groups": []},
                {"batch_id": 5, "batch_type": "Batch-5", "query_ids": [], "family_groups": []},
                {"batch_id": 6, "batch_type": "Other", "query_ids": ["q41"], "family_groups": []},
            ]
        },
    )
    query_blocks_path = store.write_json(
        "01_query_blocks/query_blocks.json",
        {
            "query_blocks": [
                {
                    "qb_id": "q41.outer",
                    "query_id": "q41",
                    "scope_type": "outer",
                    "tables": ["item"],
                    "join_edges": [],
                    "predicates": ["item.i_manufact_id BETWEEN 738 AND 778"],
                    "group_by_exprs": [],
                    "aggregate_exprs": [],
                    "unsupported_reasons": [],
                }
            ]
        },
    )
    store.write_json("01_query_blocks/query_to_qbs.json", {"q41": ["q41.outer"]})
    store.write_json("01_query_blocks/qb_to_query.json", {"q41.outer": "q41"})
    historical_rewrite_dir = write_original_equivalent_historical_rewrite(store, 6, ["q41"])
    batch_local_output = {
        "batch_id": 6,
        "mv_candidates": [
            {
                "candidate_id": "cand_batch_6_item_q41_0001",
                "source_batch_id": 6,
                "source_query_ids": ["q41"],
                "source_qb_ids": ["q41.outer"],
                "candidate_group_id": "batch_local_group_0001",
                "target_queries": ["q41"],
                "target_qb_ids": ["q41.outer"],
                "decision": "materialize",
                "reason": "batch-local dimension-only candidate without QueryFamily",
                "mv_id": "mv_item_q41",
                "mv_type": "dimension_only",
                "target_table_name": "mv_item_q41",
                "depends_on_mv_ids": [],
                "output_columns": ["i_product_name"],
                "column_mappings": [
                    {
                        "source_expr": "item.i_product_name",
                        "source_table": "item",
                        "source_column": "i_product_name",
                        "mv_column": "i_product_name",
                        "role": "projection",
                    }
                ],
                "build_sql": "CREATE TABLE mv_item_q41 AS SELECT item.i_product_name FROM item",
            }
        ],
    }

    mv_candidates_path = BatchMVAgent(store, StaticLLM([batch_local_output, batch_local_output])).run(
        6,
        batches_path,
        query_blocks_path,
        None,
        historical_rewrite_dir,
    )

    candidates = store.read_json(mv_candidates_path)["mv_candidates"]
    assert [candidate["candidate_id"] for candidate in candidates] == ["cand_batch_6_item_q41_0001"]
    assert candidates[0]["family_id"] is None
    assert candidates[0]["candidate_group_id"] == "batch_local_group_0001"
    assert "CREATE TABLE mv_item_q41" in store.read_text(store.path("04_batch_mvs/batch_6_mv_build.sql"))
    records = [json.loads(line) for line in store.run_log_path.read_text(encoding="utf-8").splitlines()]
    batch_mv_record = next(record for record in records if record["agent_name"] == "BatchMVAgent")
    assert batch_mv_record["details"]["scope_normalization_events"] == []


def test_rewrite_agent_falls_back_when_mv_rewrite_uses_source_qualified_columns() -> None:
    store = make_store("test_rewrite_source_qualified_columns")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q42.sql"])
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": []})
    mv_state_path = mv_rewrite_state(store)
    bad_record = rewrite_record(
        "q42",
        "SELECT `date_dim.d_year`, SUM(sum_ss_ext_sales_price) FROM mv_ok GROUP BY `date_dim.d_year`",
    )

    rewrite_dir = RewriteAgent(store, StaticLLM([{"rewrites": [bad_record]}, {"rewrites": [bad_record]}])).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    meta = store.read_json(rewrite_dir / "q42_rewrite_meta.json")
    assert meta["status"] == "fallback"
    assert meta["used_mv_ids"] == []
    assert meta["fallback_reason"] == "mv_uses_source_qualified_columns"
    assert (rewrite_dir / "q42_rewritten.sql").read_text(encoding="utf-8") == (PROJECT_ROOT / "tpcds-spark" / "q42.sql").read_text(
        encoding="utf-8"
    )


def test_rewrite_agent_scopes_output_to_current_batch_and_fills_missing_query() -> None:
    store = make_store("test_rewrite_scope_normalization")
    sql_manifest_path = SQLLoaderAgent(store).run(
        [
            PROJECT_ROOT / "tpcds-spark" / "q42.sql",
            PROJECT_ROOT / "tpcds-spark" / "q52.sql",
        ]
    )
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    query_blocks_path = store.write_json(
        "01_query_blocks/query_blocks.json",
        {
            "query_blocks": [
                {"qb_id": "q42.outer", "query_id": "q42", "unsupported_reasons": []},
                {"qb_id": "q52.outer", "query_id": "q52", "unsupported_reasons": []},
            ]
        },
    )
    mv_state_path = mv_rewrite_state(store)
    extra_record = rewrite_record(
        "q52",
        "SELECT d_year, i_brand_id, i_brand, SUM(sum_ss_ext_sales_price) AS ext_price FROM mv_ok GROUP BY d_year, i_brand_id, i_brand",
    )

    rewrite_dir = RewriteAgent(store, StaticLLM([{"rewrites": [extra_record]}, {"rewrites": [extra_record]}])).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    assert not (rewrite_dir / "q52_rewrite_meta.json").exists()
    meta = store.read_json(rewrite_dir / "q42_rewrite_meta.json")
    assert meta["status"] == "fallback"
    assert meta["used_mv_ids"] == []
    assert meta["fallback_reason"] == "rewrite_output_missing_query"
    assert (rewrite_dir / "q42_rewritten.sql").read_text(encoding="utf-8") == (PROJECT_ROOT / "tpcds-spark" / "q42.sql").read_text(
        encoding="utf-8"
    )


def test_rewrite_agent_falls_back_when_original_output_aliases_are_missing() -> None:
    store = make_store("test_rewrite_missing_aliases")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q52.sql"])
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q52"])
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": []})
    mv_state_path = mv_rewrite_state(store)
    bad_record = rewrite_record(
        "q52",
        "SELECT d_year, SUM(sum_ss_ext_sales_price) AS ext_price FROM mv_ok GROUP BY d_year",
    )

    rewrite_dir = RewriteAgent(store, StaticLLM([{"rewrites": [bad_record]}, {"rewrites": [bad_record]}])).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    meta = store.read_json(rewrite_dir / "q52_rewrite_meta.json")
    assert meta["status"] == "fallback"
    assert meta["used_mv_ids"] == []
    assert meta["fallback_reason"] == "output_alias_missing"


def test_rewrite_agent_falls_back_when_implicit_expression_output_name_is_missing() -> None:
    store = make_store("test_rewrite_missing_implicit_expression_name")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q42.sql"])
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": []})
    mv_state_path = mv_rewrite_state(store)
    bad_record = rewrite_record(
        "q42",
        "SELECT d_year, i_category_id, i_category, "
        "SUM(sum_ss_ext_sales_price) AS sum_ss_ext_sales_price FROM mv_ok GROUP BY d_year, i_category_id, i_category",
    )

    rewrite_dir = RewriteAgent(store, StaticLLM([{"rewrites": [bad_record]}, {"rewrites": [bad_record]}])).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    meta = store.read_json(rewrite_dir / "q42_rewrite_meta.json")
    assert meta["status"] == "fallback"
    assert meta["used_mv_ids"] == []
    assert meta["fallback_reason"] == "output_name_missing"


def test_rewrite_agent_falls_back_when_mv_rewrite_uses_unknown_mv_column() -> None:
    store = make_store("test_rewrite_unknown_mv_column")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q42.sql"])
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": []})
    mv_state_path = mv_rewrite_state(store)
    bad_record = rewrite_record(
        "q42",
        "SELECT d_year, i_category_id, i_category, "
        "SUM(sum_ext_sales_price) AS `sum(ss_ext_sales_price)` "
        "FROM mv_ok GROUP BY d_year, i_category_id, i_category "
        "ORDER BY `sum(ss_ext_sales_price)` DESC, d_year, i_category_id, i_category LIMIT 100",
    )

    rewrite_dir = RewriteAgent(store, StaticLLM([{"rewrites": [bad_record]}, {"rewrites": [bad_record]}])).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    meta = store.read_json(rewrite_dir / "q42_rewrite_meta.json")
    assert meta["status"] == "fallback"
    assert meta["used_mv_ids"] == []
    assert meta["fallback_reason"] == "mv_unknown_column"


def test_rewrite_agent_retries_twice_then_falls_back_when_final_order_limit_is_missing() -> None:
    store = make_store("test_rewrite_missing_order_limit")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q42.sql"])
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": []})
    mv_state_path = mv_rewrite_state(store)
    bad_record = rewrite_record(
        "q42",
        "SELECT d_year, i_category_id, i_category, "
        "SUM(sum_ss_ext_sales_price) AS `sum(ss_ext_sales_price)` "
        "FROM mv_ok GROUP BY d_year, i_category_id, i_category",
    )
    llm = StaticLLM(
        [
            {"rewrites": [bad_record]},
            {"rewrites": [bad_record]},
            {"rewrites": [bad_record]},
            {"rewrites": [bad_record]},
        ]
    )

    rewrite_dir = RewriteAgent(store, llm).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    meta = store.read_json(rewrite_dir / "q42_rewrite_meta.json")
    assert llm.calls == 4
    assert meta["status"] == "fallback"
    assert meta["used_mv_ids"] == []
    assert meta["fallback_reason"] == "order_by_missing"


def test_rewrite_agent_accepts_preserved_implicit_expression_output_name() -> None:
    store = make_store("test_rewrite_preserved_implicit_expression_name")
    sql_manifest_path = SQLLoaderAgent(store).run([PROJECT_ROOT / "tpcds-spark" / "q42.sql"])
    batches_path = write_minimal_complexity_batches(store, 3, "join_filter_groupby", ["q42"])
    query_blocks_path = store.write_json("01_query_blocks/query_blocks.json", {"query_blocks": []})
    mv_state_path = mv_rewrite_state(store)
    good_record = rewrite_record(
        "q42",
        "SELECT d_year, i_category_id, i_category, "
        "SUM(sum_ss_ext_sales_price) AS `sum(ss_ext_sales_price)` "
        "FROM mv_ok GROUP BY d_year, i_category_id, i_category "
        "ORDER BY `sum(ss_ext_sales_price)` DESC, d_year, i_category_id, i_category LIMIT 100",
    )

    rewrite_dir = RewriteAgent(store, StaticLLM([{"rewrites": [good_record]}, {"rewrites": [good_record]}])).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        mv_state_path,
    )

    meta = store.read_json(rewrite_dir / "q42_rewrite_meta.json")
    assert meta["status"] == "rewritten"
    assert meta["used_mv_ids"] == ["mv_ok"]
    assert meta["fallback_reason"] is None


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

    target_batch = by_type["Batch-1"]
    assert target_batch["batch_id"] == 1
    assert {"q42", "q52"}.issubset(set(target_batch["query_ids"]))
    assert any(
        {"q42", "q52"}.issubset(set(group["query_ids"]))
        and {"q42.outer", "q52.outer"}.issubset(set(group["qb_ids"]))
        for group in target_batch["family_groups"]
    )

    historical_rewrite_dir = write_original_equivalent_historical_rewrite(store, 1, ["q42", "q52"])
    mv_candidates_path = BatchMVAgent(store, llm_client).run(
        1,
        batches_path,
        query_blocks_path,
        families_path,
        historical_rewrite_dir,
    )
    mv_output = store.read_json(mv_candidates_path)
    assert mv_output["batch_id"] == 1
    assert mv_output["mv_candidates"]
    mv_build_sql = store.read_text(store.path("04_batch_mvs/batch_1_mv_build.sql"))
    assert "CREATE OR REPLACE TABLE" not in mv_build_sql.upper()
    assert "CREATE TABLE" in mv_build_sql.upper()
    assert store.path("04_batch_mvs/materialized_mvs.json").exists()

    for candidate in mv_output["mv_candidates"]:
        assert candidate["source_batch_id"] == 1
        assert candidate["family_id"]
        assert set(candidate["source_query_ids"]).issubset(set(target_batch["query_ids"]))
        assert set(candidate["target_queries"]).issubset(set(target_batch["query_ids"]))
        assert not any(query_id.startswith("downstream") for query_id in candidate["target_queries"])
        if candidate["decision"] == "materialize":
            assert candidate["source_qb_ids"]
            assert candidate["target_qb_ids"]
            assert candidate["build_sql"].upper().startswith("CREATE TABLE")
            assert "CREATE OR REPLACE TABLE" not in candidate["build_sql"].upper()
            assert candidate["column_mappings"]
            assert all("." not in column for column in candidate["output_columns"])
            mapped_columns = {mapping["mv_column"] for mapping in candidate["column_mappings"]}
            assert set(candidate["output_columns"]).issubset(mapped_columns)
            for mapping in candidate["column_mappings"]:
                if mapping["role"] == "measure":
                    assert mapping["mv_column"] == f"sum_{mapping['source_column']}"
                else:
                    assert mapping["mv_column"] == mapping["source_column"]

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


def test_live_rewrite_executor_and_self_iteration_agents_complete_batch_flow() -> None:
    store = make_store("test_live_full_batch_flow")
    llm_client = LLMClient(project_root=PROJECT_ROOT)

    sql_manifest_path = SQLLoaderAgent(store).run(SQL_PATHS)
    query_blocks_path = FeatureAgent(store, llm_client).run(sql_manifest_path)
    families_path = FamilyAgent(store, llm_client).run(query_blocks_path)
    batches_path = BatchClusterAgent(store, llm_client).run(query_blocks_path, families_path)

    historical_rewrite_dir = RewriteAgent(store, llm_client).run(
        3,
        "historical",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
    )
    for query_id in ("q42", "q52"):
        meta = store.read_json(historical_rewrite_dir / f"{query_id}_rewrite_meta.json")
        assert meta["rewrite_stage"] == "historical"
        assert meta["status"] in {"fallback", "rewritten"}
        if meta["status"] == "fallback":
            assert meta["used_mv_ids"] == []
            assert meta["fallback_reason"]

    mv_candidates_path = BatchMVAgent(store, llm_client).run(
        3,
        batches_path,
        query_blocks_path,
        families_path,
        historical_rewrite_dir,
    )
    materialized_mvs_path = ExecutorAgent(store).materialize_mvs(
        3,
        mv_candidates_path,
        store.path("04_batch_mvs/batch_3_mv_build.sql"),
    )
    assert materialized_mvs_path == store.path("04_batch_mvs/materialized_mvs.json")

    final_rewrite_dir = RewriteAgent(store, llm_client).run(
        3,
        "final",
        batches_path,
        sql_manifest_path,
        query_blocks_path,
        materialized_mvs_path,
    )
    for query_id in ("q42", "q52"):
        assert (final_rewrite_dir / f"{query_id}_rewritten.sql").exists()
        meta = store.read_json(final_rewrite_dir / f"{query_id}_rewrite_meta.json")
        assert meta["rewrite_stage"] == "final"
        if meta["status"] == "rewritten":
            assert meta["used_mv_ids"]
            assert meta["fallback_reason"] is None
            rewritten_sql = (final_rewrite_dir / f"{query_id}_rewritten.sql").read_text(encoding="utf-8")
            materialized_by_id = {
                mv["mv_id"]: mv for mv in store.read_json(materialized_mvs_path).get("materialized_mvs", [])
            }
            for mv_id in meta["used_mv_ids"]:
                for mapping in materialized_by_id[mv_id]["column_mappings"]:
                    source_ref = f"{mapping['source_table']}.{mapping['source_column']}"
                    assert source_ref.lower() not in rewritten_sql.lower()
            if query_id == "q52":
                assert "brand_id" in rewritten_sql
                assert "brand" in rewritten_sql
                assert "ext_price" in rewritten_sql
            if query_id == "q42":
                assert "`sum(ss_ext_sales_price)`" in rewritten_sql
            assert "ORDER BY" in rewritten_sql.upper()
            assert "LIMIT 100" in rewritten_sql.upper()
        else:
            assert meta["status"] == "fallback"
            assert meta["used_mv_ids"] == []
            assert meta["fallback_reason"]

    execution_order_path = ExecutorAgent(store).run_queries(3, final_rewrite_dir, batches_path)
    execution_order = store.read_json(execution_order_path)
    query_steps = [step for step in execution_order["steps"] if step["step_type"] == "run_query"]
    assert [step["query_id"] for step in query_steps] == ["q42", "q52"]
    assert {
        step["query_id"]: step["depends_on_mv_ids"] for step in query_steps
    } == {
        query_id: store.read_json(final_rewrite_dir / f"{query_id}_rewrite_meta.json")["used_mv_ids"]
        for query_id in ("q42", "q52")
    }

    feedback_path = SelfIterationAgent(store, llm_client).run(store.run_log_path)
    feedback = store.read_json(feedback_path)
    assert feedback["run_id"] == store.run_id
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
    assert "QueryAnalysisRunner" in source
    assert "FamilyCandidateBuilder" in source
    assert "FamilyAgent" in source
    assert "BatchClusterAgent" in source
    assert "BatchWorkflowRunner" in source
    assert "CoverageSummaryBuilder" in source
    assert "SelfIterationAgent" in source
