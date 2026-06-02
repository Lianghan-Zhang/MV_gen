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
from llm_demo.src.core.llm_client import LLMClient


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
                            "family_id": "family_ss_dd_item",
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
                    "family_id": "family_ss_dd_item",
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
                    "family_id": "family_ss_dd_item",
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
                    "family_id": "family_ss_dd_item",
                    "mv_type": "fine_grain_aggregate",
                    "target_table_name": "mv_ok",
                    "target_queries": ["q42"],
                    "depends_on_mv_ids": [],
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
                    "family_id": "family_ss_dd_item",
                    "target_table_name": "mv_missing_dep",
                    "target_queries": ["q52"],
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
                "family_id": "family_ss_dd_item",
                "target_queries": ["q42"],
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
                "family_id": "family_ss_dd_item",
                "target_queries": ["q42"],
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


def test_batch_mv_agent_normalizes_create_or_replace_to_create_table() -> None:
    store = make_store("test_batch_mv_create_table")
    batches_path, query_blocks_path, families_path, historical_rewrite_dir = write_minimal_batch_mv_inputs(store, ["q42"])
    output = {
        "batch_id": 3,
        "mv_candidates": [
            {
                "candidate_id": "cand_create_table",
                "source_batch_id": 3,
                "source_query_ids": ["q42"],
                "family_id": "family_ss_dd_item",
                "target_queries": ["q42"],
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

    mv_candidates_path = BatchMVAgent(store, StaticLLM([output, output])).run(
        3,
        batches_path,
        query_blocks_path,
        families_path,
        historical_rewrite_dir,
    )

    candidate = store.read_json(mv_candidates_path)["mv_candidates"][0]
    mv_build_sql = store.read_text(store.path("04_batch_mvs/batch_3_mv_build.sql"))
    assert candidate["build_sql"].startswith("CREATE TABLE mv_create_table")
    assert "CREATE OR REPLACE TABLE" not in candidate["build_sql"].upper()
    assert "CREATE TABLE mv_create_table" in mv_build_sql
    assert "CREATE OR REPLACE TABLE" not in mv_build_sql.upper()


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
                "family_id": "family_ss_dd_item",
                "target_queries": ["q42"],
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
    mv_build_sql = store.read_text(store.path("04_batch_mvs/batch_3_mv_build.sql"))
    assert "CREATE OR REPLACE TABLE" not in mv_build_sql.upper()
    assert "CREATE TABLE" in mv_build_sql.upper()
    assert store.path("04_batch_mvs/materialized_mvs.json").exists()

    for candidate in mv_output["mv_candidates"]:
        assert candidate["source_batch_id"] == 3
        assert candidate["family_id"]
        assert set(candidate["source_query_ids"]).issubset(set(target_batch["query_ids"]))
        assert set(candidate["target_queries"]).issubset(set(target_batch["query_ids"]))
        assert not any(query_id.startswith("downstream") for query_id in candidate["target_queries"])
        if candidate["decision"] == "materialize":
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
    assert "FamilyAgent" in source
    assert "BatchClusterAgent" in source
    assert "BatchMVAgent" in source
    assert "RewriteAgent" in source
    assert "ExecutorAgent" in source
    assert "SelfIterationAgent" in source
