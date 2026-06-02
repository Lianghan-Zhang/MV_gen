from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp


SQL_DIALECT = "spark"


@dataclass(frozen=True)
class SelectOutput:
    name: str
    aliased: bool


def normalize_create_table_as_select(sql: str) -> str:
    create = _parse_create(sql)
    create.set("replace", False)
    return create.sql(dialect=SQL_DIALECT)


def create_table_select_output_names(sql: str) -> set[str]:
    create = _parse_create(sql)
    select = create.args.get("expression")
    if not isinstance(select, exp.Select):
        raise ValueError("CREATE TABLE build_sql must use AS SELECT")
    return {output.name.lower() for output in _select_outputs(select)}


def select_output_names(sql: str) -> set[str]:
    return {output.name.lower() for output in _select_outputs(_parse_select(sql))}


def aliased_select_output_names(sql: str) -> set[str]:
    return {output.name.lower() for output in _select_outputs(_parse_select(sql)) if output.aliased}


def unaliased_select_output_names(sql: str) -> set[str]:
    return {output.name.lower() for output in _select_outputs(_parse_select(sql)) if not output.aliased}


def uses_source_qualified_columns(sql: str, mappings: list[dict[str, Any]]) -> bool:
    expression = _parse_sql(sql)
    source_refs = {
        (mapping["source_table"].lower(), mapping["source_column"].lower())
        for mapping in mappings
    }
    quoted_source_refs = {f"{table}.{column}" for table, column in source_refs}
    for column in expression.find_all(exp.Column):
        table_name = (column.table or "").lower()
        column_name = column.name.lower()
        if (table_name, column_name) in source_refs:
            return True
        if not table_name and column_name in quoted_source_refs:
            return True
    return False


def aggregate_measure_column_name(source_expr: str, source_column: str) -> str:
    expression = _parse_sql(source_expr)
    if not isinstance(expression, exp.AggFunc):
        raise ValueError(f"Measure source_expr must be an aggregate expression: {source_expr}")
    return f"{expression.key.lower()}_{source_column.lower()}"


def unknown_column_names(sql: str, allowed_columns: set[str]) -> set[str]:
    expression = _parse_sql(sql)
    allowed = {column.lower() for column in allowed_columns}
    allowed.update(select_output_names(sql))
    unknown = set()
    for column in expression.find_all(exp.Column):
        column_name = column.name.lower()
        if column_name not in allowed:
            unknown.add(column_name)
    return unknown


def order_limit_mismatch_reason(original_sql: str, rewritten_sql: str) -> str | None:
    original = _parse_select(original_sql)
    rewritten = _parse_select(rewritten_sql)

    original_order = original.args.get("order")
    rewritten_order = rewritten.args.get("order")
    if original_order:
        if not rewritten_order:
            return "order_by_missing"
        if len(original_order.expressions) != len(rewritten_order.expressions):
            return "order_by_mismatch"
        for original_item, rewritten_item in zip(original_order.expressions, rewritten_order.expressions):
            if bool(original_item.args.get("desc")) != bool(rewritten_item.args.get("desc")):
                return "order_by_mismatch"

    original_limit = original.args.get("limit")
    rewritten_limit = rewritten.args.get("limit")
    if original_limit:
        if not rewritten_limit:
            return "limit_missing"
        if _limit_value(original_limit) != _limit_value(rewritten_limit):
            return "limit_mismatch"

    return None


def _parse_create(sql: str) -> exp.Create:
    expression = _parse_sql(sql)
    if not isinstance(expression, exp.Create) or expression.args.get("kind") != "TABLE":
        raise ValueError("build_sql must start with CREATE TABLE")
    if expression.args.get("expression") is None:
        raise ValueError("CREATE TABLE build_sql must use AS SELECT")
    return expression


def _parse_select(sql: str) -> exp.Select:
    expression = _parse_sql(sql)
    if isinstance(expression, exp.Select):
        return expression
    select = expression.find(exp.Select)
    if not isinstance(select, exp.Select):
        raise ValueError("SQL must contain SELECT")
    return select


def _parse_sql(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect=SQL_DIALECT)


def _select_outputs(select: exp.Select) -> list[SelectOutput]:
    outputs: list[SelectOutput] = []
    for expression in select.expressions:
        if isinstance(expression, exp.Alias):
            outputs.append(SelectOutput(name=expression.output_name, aliased=True))
        elif expression.output_name:
            outputs.append(SelectOutput(name=expression.output_name, aliased=False))
        else:
            outputs.append(SelectOutput(name=expression.sql(dialect=SQL_DIALECT).lower(), aliased=False))
    return outputs


def _limit_value(limit: exp.Limit) -> str:
    expression = limit.args.get("expression")
    if expression is None:
        return ""
    return expression.sql(dialect=SQL_DIALECT).lower()
