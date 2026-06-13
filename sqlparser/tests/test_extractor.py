from __future__ import annotations

from pathlib import Path

import pytest

from sqlparser import extract_sql_features
from sqlparser.extractor import validate_sqlglot_version


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TPCDS_SQL_DIR = PROJECT_ROOT / "tpcds-spark"


def extract_query(query_id: str) -> dict:
    sql_path = TPCDS_SQL_DIR / f"{query_id}.sql"
    return extract_sql_features(sql_path.read_text(encoding="utf-8"), query_id=query_id)


def block_by_id(output: dict, qb_id: str) -> dict:
    return {block["qb_id"]: block for block in output["query_blocks"]}[qb_id]


def relation_by_id(block: dict) -> dict:
    return {relation["relation_id"]: relation for relation in block["relation_instances"]}


def all_relation_dicts(output: dict) -> list[dict]:
    return [
        relation
        for block in output["query_blocks"]
        for relation in block["relation_instances"]
    ]


def unsupported_reasons(block: dict) -> list[str]:
    return block["unsupported_reasons"]


def test_sqlglot_version_is_supported() -> None:
    validate_sqlglot_version()


def test_extracts_all_tpcds_sql_files() -> None:
    sql_files = sorted(TPCDS_SQL_DIR.glob("*.sql"))

    assert len(sql_files) == 105
    for sql_path in sql_files:
        output = extract_sql_features(sql_path.read_text(encoding="utf-8"), query_id=sql_path.stem)
        assert output["query_blocks"]
        assert f"{sql_path.stem}.outer" in output["query_to_qbs"][sql_path.stem]


def test_q42_extracts_join_aggregate_order_and_limit() -> None:
    output = extract_query("q42")
    block = block_by_id(output, "q42.outer")

    assert {"date_dim", "store_sales", "item"} == set(block["tables"])
    assert not {"complexity_type", "block_shape", "family_key"} & set(block)
    assert any(relation["physical_table"] == "date_dim" for relation in block["relation_instances"])
    assert any(
        edge["left_table"] == "date_dim"
        and edge["left_column"] == "d_date_sk"
        and edge["right_table"] == "store_sales"
        and edge["right_column"] == "ss_sold_date_sk"
        for edge in block["join_edges"]
    )
    assert any(
        aggregate["agg_func"] == "SUM"
        and aggregate["source_table"] == "store_sales"
        and aggregate["source_column"] == "ss_ext_sales_price"
        for aggregate in block["aggregate_exprs"]
    )
    assert block["limit"] == "100"
    assert "has_order_by" in block["structural_flags"]
    assert "has_limit" in block["structural_flags"]


@pytest.mark.parametrize(
    ("query_id", "table_name", "expected_min_instances"),
    [
        ("q72", "date_dim", 3),
        ("q85", "customer_demographics", 2),
    ],
)
def test_self_join_uses_physical_relation_features(
    query_id: str,
    table_name: str,
    expected_min_instances: int,
) -> None:
    output = extract_query(query_id)
    block = block_by_id(output, f"{query_id}.outer")
    matching_relations = [
        relation
        for relation in block["relation_instances"]
        if relation["physical_table"] == table_name
    ]

    assert len(matching_relations) >= expected_min_instances
    assert all("sql_alias" not in relation for relation in all_relation_dicts(output))
    assert {relation["relation_id"] for relation in matching_relations} == {table_name}
    assert "has_self_join" in block["structural_flags"]


def test_q11_extracts_cte_union_branches_and_outer_dependency() -> None:
    output = extract_query("q11")
    qb_ids = output["query_to_qbs"]["q11"]

    assert "q11.outer" in qb_ids
    assert "q11.year_total" in qb_ids
    assert "q11.year_total.branch1" in qb_ids
    assert "q11.year_total.branch2" in qb_ids
    assert block_by_id(output, "q11.year_total")["depends_on_qb_ids"] == [
        "q11.year_total.branch1",
        "q11.year_total.branch2",
    ]
    assert block_by_id(output, "q11.outer")["depends_on_qb_ids"] == ["q11.year_total"]


def test_q14a_extracts_multiple_ctes_and_set_branches() -> None:
    output = extract_query("q14a")
    qb_ids = output["query_to_qbs"]["q14a"]

    assert "q14a.cross_items" in qb_ids
    assert "q14a.avg_sales" in qb_ids
    assert "q14a.avg_sales.derived1" in qb_ids
    assert any(qb_id.startswith("q14a.avg_sales.derived1.branch") for qb_id in qb_ids)
    assert "q14a.outer" in qb_ids


def test_q39a_outer_depends_on_cte() -> None:
    output = extract_query("q39a")

    assert "q39a.inv" in output["query_to_qbs"]["q39a"]
    assert block_by_id(output, "q39a.outer")["depends_on_qb_ids"] == ["q39a.inv"]


def test_indexes_are_consistent() -> None:
    output = extract_query("q72")
    qb_ids = [block["qb_id"] for block in output["query_blocks"]]

    assert output["query_to_qbs"]["q72"] == qb_ids
    assert output["qb_to_query"] == {qb_id: "q72" for qb_id in qb_ids}


def test_q96_count_star_is_not_wildcard_projection() -> None:
    block = block_by_id(extract_query("q96"), "q96.outer")

    assert "has_wildcard" not in block["structural_flags"]
    assert not any("wildcard_projection_not_expanded:COUNT(*)" in reason for reason in unsupported_reasons(block))
    assert any(aggregate["expr"] == "COUNT(*)" for aggregate in block["aggregate_exprs"])


@pytest.mark.parametrize(
    ("query_id", "left_table", "left_column", "right_table", "right_column"),
    [
        ("q40", "catalog_sales", "cs_order_number", "catalog_returns", "cr_order_number"),
        ("q72", "catalog_sales", "cs_order_number", "catalog_returns", "cr_order_number"),
        ("q75", "catalog_sales", "cs_order_number", "catalog_returns", "cr_order_number"),
        ("q80", "store_sales", "ss_item_sk", "store_returns", "sr_item_sk"),
        ("q97", "store_sales", "ss_customer_sk", "catalog_sales", "cs_bill_customer_sk"),
    ],
)
def test_explicit_join_on_edges_are_extracted(
    query_id: str,
    left_table: str,
    left_column: str,
    right_table: str,
    right_column: str,
) -> None:
    output = extract_query(query_id)
    edges = [
        edge
        for block in output["query_blocks"]
        for edge in block["join_edges"]
    ]

    assert any(
        {
            (edge["left_table"], edge["left_column"]),
            (edge["right_table"], edge["right_column"]),
        }
        == {
            (left_table, left_column),
            (right_table, right_column),
        }
        for edge in edges
    )


@pytest.mark.parametrize("query_id", ["q88", "q89", "q90", "q93"])
def test_from_derived_tables_get_query_blocks(query_id: str) -> None:
    output = extract_query(query_id)

    assert any(".derived" in block["qb_id"] for block in output["query_blocks"])
    assert any(
        ".derived" in dependency
        for block in output["query_blocks"]
        for dependency in block["depends_on_qb_ids"]
    )


@pytest.mark.parametrize("query_id", ["q1", "q9", "q92", "q94", "q95"])
def test_predicate_subqueries_get_query_blocks(query_id: str) -> None:
    output = extract_query(query_id)

    assert any(".predicate_subquery" in block["qb_id"] for block in output["query_blocks"])


@pytest.mark.parametrize(("query_id", "alias"), [("q52", "ext_price"), ("q75", "sales_cnt_diff"), ("q78", "ratio"), ("q98", "revenueratio")])
def test_order_by_projection_alias_resolves_lineage(query_id: str, alias: str) -> None:
    block = block_by_id(extract_query(query_id), f"{query_id}.outer")
    alias_order = [order for order in block["order_by_exprs"] if order["expr"].lower() == alias]

    assert alias_order
    assert alias_order[0]["columns"]
    assert not any(f"unresolved_column:{alias}" in reason for reason in unsupported_reasons(block))


@pytest.mark.parametrize("query_id", ["q36", "q70", "q86", "q98"])
def test_window_grouping_and_rollup_are_separate_from_aggregates(query_id: str) -> None:
    block = block_by_id(extract_query(query_id), f"{query_id}.outer")

    assert "window_exprs" in block
    assert not any("GROUPING(" in aggregate["expr"] or " OVER " in aggregate["expr"] for aggregate in block["aggregate_exprs"])
    if query_id in {"q36", "q70", "q86"}:
        assert block["grouping_exprs"]
        assert block["rollup_exprs"]


def test_correlated_subquery_resolves_outer_scope_columns() -> None:
    output = extract_query("q94")
    reasons = [
        reason
        for block in output["query_blocks"]
        for reason in block["unsupported_reasons"]
    ]

    assert not any(reason.startswith("unknown_column_relation:ws1.") for reason in reasons)


def test_wildcard_over_derived_source_propagates_output_schema() -> None:
    output = extract_query("q44")
    reasons = [
        reason
        for block in output["query_blocks"]
        for reason in block["unsupported_reasons"]
    ]

    assert not any(reason.startswith("unresolved_source_column:q44.outer.derived") for reason in reasons)


def test_order_by_expression_resolves_projection_alias_references() -> None:
    block = block_by_id(extract_query("q36"), "q36.outer")

    assert not any("unresolved_column:lochierarchy" in reason for reason in block["unsupported_reasons"])
