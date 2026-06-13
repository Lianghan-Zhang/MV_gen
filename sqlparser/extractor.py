from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import sqlglot
from sqlglot import exp

from sqlparser.models import (
    AggregateFeature,
    ExpressionFeature,
    FeatureOutput,
    JoinEdge,
    PredicateFeature,
    ProjectionFeature,
    QueryBlock,
    RelationInstance,
    to_dict,
)
from sqlparser.schema import PhysicalSchema


DEFAULT_DIALECT = "spark"
MIN_SQLGLOT_VERSION = (30, 7, 0)


@dataclass(frozen=True)
class _OutputColumn:
    output_name: str
    expr: str
    columns: tuple[str, ...] = ()
    relation_ids: tuple[str, ...] = ()
    direct_table: str | None = None
    direct_column: str | None = None


@dataclass(frozen=True)
class _ResolvedColumn:
    relation_id: str | None
    table: str | None
    column: str
    columns: tuple[str, ...] = ()
    relation_ids: tuple[str, ...] = ()
    source_key: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class _SourceRef:
    relation: RelationInstance
    lookup_names: frozenset[str]
    internal_id: str
    output_schema: dict[str, _OutputColumn] = field(default_factory=dict)


@dataclass(frozen=True)
class _RelationContext:
    instances: list[RelationInstance]
    by_name: dict[str, _SourceRef]
    cte_names: set[str]
    outer_context: "_RelationContext | None" = None


def extract_sql_features(
    sql_text: str,
    query_id: str,
    dialect: str = DEFAULT_DIALECT,
    project_root: str | Path | None = None,
) -> dict:
    """Extract deterministic QueryBlock facts from one SQL statement."""
    extractor = SqlglotFeatureExtractor(dialect=dialect, project_root=project_root)
    return extractor.extract(sql_text=sql_text, query_id=query_id)


def validate_sqlglot_version() -> None:
    version = _version_tuple(sqlglot.__version__)
    if version < MIN_SQLGLOT_VERSION:
        expected = ".".join(str(part) for part in MIN_SQLGLOT_VERSION)
        raise RuntimeError(
            f"sqlparser requires sqlglot>={expected}; current sqlglot is {sqlglot.__version__}. "
            "Run with the conda mv_gen environment."
        )


def _version_tuple(version: str) -> tuple[int, int, int]:
    parts: list[int] = []
    for part in version.split(".")[:3]:
        digits = ""
        for char in part:
            if not char.isdigit():
                break
            digits += char
        parts.append(int(digits or 0))
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


class SqlglotFeatureExtractor:
    def __init__(self, dialect: str = DEFAULT_DIALECT, project_root: str | Path | None = None) -> None:
        validate_sqlglot_version()
        self.dialect = dialect
        self.schema = PhysicalSchema.load(project_root)
        self._output_schemas: dict[str, dict[str, _OutputColumn]] = {}

    def extract(self, sql_text: str, query_id: str) -> dict:
        self._output_schemas = {}
        root = sqlglot.parse_one(sql_text, dialect=self.dialect)
        cte_names = self._cte_names(root)
        cte_qb_ids = {cte_name: f"{query_id}.{cte_name}" for cte_name in cte_names}

        blocks: list[QueryBlock] = []
        for cte in self._top_level_ctes(root):
            cte_name = cte.alias.lower()
            blocks.extend(
                self._blocks_for_expression(
                    expression=cte.this,
                    query_id=query_id,
                    qb_id=f"{query_id}.{cte_name}",
                    block_name=cte_name,
                    scope_type="cte",
                    parent_qb_id=None,
                    cte_qb_ids=cte_qb_ids,
                )
            )

        if isinstance(root, exp.SetOperation):
            blocks.extend(
                self._blocks_for_expression(
                    expression=root,
                    query_id=query_id,
                    qb_id=f"{query_id}.outer",
                    block_name="outer",
                    scope_type="outer",
                    parent_qb_id=None,
                    cte_qb_ids=cte_qb_ids,
                )
            )
        else:
            outer_select = self._first_select(root)
            if outer_select is None:
                blocks.append(
                    QueryBlock(
                        qb_id=f"{query_id}.outer",
                        query_id=query_id,
                        block_name="outer",
                        scope_type="outer",
                        unsupported_reasons=[f"unsupported_root_expression:{type(root).__name__}"],
                    )
                )
            else:
                blocks.extend(
                    self._blocks_for_select(
                        select=outer_select,
                        query_id=query_id,
                        qb_id=f"{query_id}.outer",
                        block_name="outer",
                        scope_type="outer",
                        parent_qb_id=None,
                        cte_qb_ids=cte_qb_ids,
                    )
                )

        output = FeatureOutput(
            query_blocks=blocks,
            query_to_qbs={query_id: [block.qb_id for block in blocks]},
            qb_to_query={block.qb_id: query_id for block in blocks},
        )
        return to_dict(output)

    def _blocks_for_expression(
        self,
        *,
        expression: exp.Expression,
        query_id: str,
        qb_id: str,
        block_name: str | None,
        scope_type: str,
        parent_qb_id: str | None,
        cte_qb_ids: dict[str, str],
        outer_context: _RelationContext | None = None,
    ) -> list[QueryBlock]:
        if isinstance(expression, exp.Subquery):
            return self._blocks_for_expression(
                expression=expression.this,
                query_id=query_id,
                qb_id=qb_id,
                block_name=block_name,
                scope_type=scope_type,
                parent_qb_id=parent_qb_id,
                cte_qb_ids=cte_qb_ids,
                outer_context=outer_context,
            )

        if isinstance(expression, exp.Select):
            return self._blocks_for_select(
                select=expression,
                query_id=query_id,
                qb_id=qb_id,
                block_name=block_name,
                scope_type=scope_type,
                parent_qb_id=parent_qb_id,
                cte_qb_ids=cte_qb_ids,
                outer_context=outer_context,
            )

        if isinstance(expression, exp.SetOperation):
            return self._blocks_for_set_operation(
                expression=expression,
                query_id=query_id,
                qb_id=qb_id,
                block_name=block_name,
                scope_type=scope_type,
                parent_qb_id=parent_qb_id,
                cte_qb_ids=cte_qb_ids,
                outer_context=outer_context,
            )

        select = self._first_select(expression)
        if select is not None:
            return self._blocks_for_select(
                select=select,
                query_id=query_id,
                qb_id=qb_id,
                block_name=block_name,
                scope_type=scope_type,
                parent_qb_id=parent_qb_id,
                cte_qb_ids=cte_qb_ids,
                outer_context=outer_context,
            )

        return [
            QueryBlock(
                qb_id=qb_id,
                query_id=query_id,
                block_name=block_name,
                scope_type=scope_type,
                parent_qb_id=parent_qb_id,
                unsupported_reasons=[f"unsupported_query_block_expression:{type(expression).__name__}"],
            )
        ]

    def _blocks_for_select(
        self,
        *,
        select: exp.Select,
        query_id: str,
        qb_id: str,
        block_name: str | None,
        scope_type: str,
        parent_qb_id: str | None,
        cte_qb_ids: dict[str, str],
        outer_context: _RelationContext | None = None,
    ) -> list[QueryBlock]:
        source_blocks, derived_refs, source_dependency_ids = self._derived_source_blocks(
            select=select,
            query_id=query_id,
            parent_qb_id=qb_id,
            cte_qb_ids=cte_qb_ids,
        )
        context, relation_reasons = self._relation_context(
            select=select,
            cte_qb_ids=cte_qb_ids,
            derived_refs=derived_refs,
            outer_context=outer_context,
        )
        predicate_blocks, predicate_dependency_ids = self._predicate_subquery_blocks(
            select=select,
            query_id=query_id,
            parent_qb_id=qb_id,
            cte_qb_ids=cte_qb_ids,
            outer_context=context,
        )
        block = self._build_select_block(
            select=select,
            query_id=query_id,
            qb_id=qb_id,
            block_name=block_name,
            scope_type=scope_type,
            parent_qb_id=parent_qb_id,
            context=context,
            relation_reasons=relation_reasons,
            cte_qb_ids=cte_qb_ids,
            extra_dependency_ids=[*source_dependency_ids, *predicate_dependency_ids],
        )
        return [block, *source_blocks, *predicate_blocks]

    def _blocks_for_set_operation(
        self,
        *,
        expression: exp.SetOperation,
        query_id: str,
        qb_id: str,
        block_name: str | None,
        scope_type: str,
        parent_qb_id: str | None,
        cte_qb_ids: dict[str, str],
        outer_context: _RelationContext | None = None,
    ) -> list[QueryBlock]:
        branch_selects = list(self._set_operation_selects(expression))
        branch_ids = [f"{qb_id}.branch{index}" for index in range(1, len(branch_selects) + 1)]
        branch_blocks: list[QueryBlock] = []
        for index, (branch_id, branch_select) in enumerate(zip(branch_ids, branch_selects), start=1):
            branch_blocks.extend(
                self._blocks_for_select(
                    select=branch_select,
                    query_id=query_id,
                    qb_id=branch_id,
                    block_name=f"branch{index}",
                    scope_type="set_branch",
                    parent_qb_id=qb_id,
                    cte_qb_ids=cte_qb_ids,
                    outer_context=outer_context,
                )
            )

        parent = QueryBlock(
            qb_id=qb_id,
            query_id=query_id,
            block_name=block_name,
            scope_type=scope_type,
            parent_qb_id=parent_qb_id,
            depends_on_qb_ids=branch_ids,
            structural_flags=self._unique([self._set_operation_flag(expression)]),
        )
        self._output_schemas[qb_id] = self._merge_set_output_schema(branch_ids)
        return [parent, *branch_blocks]

    def _derived_source_blocks(
        self,
        *,
        select: exp.Select,
        query_id: str,
        parent_qb_id: str,
        cte_qb_ids: dict[str, str],
        outer_context: _RelationContext | None = None,
    ) -> tuple[list[QueryBlock], dict[int, _SourceRef], list[str]]:
        blocks: list[QueryBlock] = []
        refs: dict[int, _SourceRef] = {}
        dependency_ids: list[str] = []
        derived_index = 0
        for source in self._top_level_sources(select):
            if not isinstance(source, exp.Subquery):
                continue
            derived_index += 1
            block_name = f"derived{derived_index}"
            qb_id = f"{parent_qb_id}.{block_name}"
            source_blocks = self._blocks_for_expression(
                expression=source.this,
                query_id=query_id,
                qb_id=qb_id,
                block_name=block_name,
                scope_type="derived",
                parent_qb_id=parent_qb_id,
                cte_qb_ids=cte_qb_ids,
                outer_context=outer_context,
            )
            lookup_names = self._unique([source.alias.lower() if source.alias else None, qb_id])
            relation = RelationInstance(
                relation_id=qb_id,
                source_name=qb_id,
                physical_table=None,
                relation_type="derived",
            )
            refs[id(source)] = _SourceRef(
                relation=relation,
                lookup_names=frozenset(lookup_names),
                internal_id=qb_id,
                output_schema=self._output_schemas.get(qb_id, {}),
            )
            dependency_ids.append(qb_id)
            blocks.extend(source_blocks)
        return blocks, refs, dependency_ids

    def _predicate_subquery_blocks(
        self,
        *,
        select: exp.Select,
        query_id: str,
        parent_qb_id: str,
        cte_qb_ids: dict[str, str],
        outer_context: _RelationContext,
    ) -> tuple[list[QueryBlock], list[str]]:
        blocks: list[QueryBlock] = []
        dependency_ids: list[str] = []
        for index, expression in enumerate(self._predicate_subquery_expressions(select), start=1):
            qb_id = f"{parent_qb_id}.predicate_subquery{index}"
            source_blocks = self._blocks_for_expression(
                expression=expression,
                query_id=query_id,
                qb_id=qb_id,
                block_name=f"predicate_subquery{index}",
                scope_type="predicate_subquery",
                parent_qb_id=parent_qb_id,
                cte_qb_ids=cte_qb_ids,
                outer_context=outer_context,
            )
            dependency_ids.append(qb_id)
            blocks.extend(source_blocks)
        return blocks, dependency_ids

    def _build_select_block(
        self,
        *,
        select: exp.Select,
        query_id: str,
        qb_id: str,
        block_name: str | None,
        scope_type: str,
        parent_qb_id: str | None,
        context: _RelationContext,
        relation_reasons: list[str],
        cte_qb_ids: dict[str, str],
        extra_dependency_ids: list[str],
    ) -> QueryBlock:
        unsupported_reasons: list[str] = list(relation_reasons)
        depends_on_qb_ids = self._unique([*self._depends_on_qb_ids(context, cte_qb_ids), *extra_dependency_ids])

        join_edges, join_condition_sqls, join_reasons = self._join_edges(select, context)
        unsupported_reasons.extend(join_reasons)

        predicates, predicate_reasons = self._predicates(select, context, join_condition_sqls)
        unsupported_reasons.extend(predicate_reasons)

        projections, output_schema, projection_alias_map, projection_reasons = self._projections(select, context)
        unsupported_reasons.extend(projection_reasons)
        self._output_schemas[qb_id] = output_schema

        group_by_exprs, group_reasons = self._group_by_exprs(select, context)
        unsupported_reasons.extend(group_reasons)

        aggregate_exprs, aggregate_reasons = self._aggregate_exprs(select, context)
        unsupported_reasons.extend(aggregate_reasons)

        window_exprs, window_reasons = self._window_exprs(select, context)
        unsupported_reasons.extend(window_reasons)

        grouping_exprs, grouping_reasons = self._grouping_exprs(select, context)
        unsupported_reasons.extend(grouping_reasons)

        rollup_exprs, rollup_reasons = self._rollup_exprs(select, context)
        unsupported_reasons.extend(rollup_reasons)

        order_by_exprs, order_reasons = self._order_by_exprs(select, context, projection_alias_map)
        unsupported_reasons.extend(order_reasons)

        return QueryBlock(
            qb_id=qb_id,
            query_id=query_id,
            block_name=block_name,
            scope_type=scope_type,
            parent_qb_id=parent_qb_id,
            depends_on_qb_ids=depends_on_qb_ids,
            structural_flags=self._structural_flags(
                select=select,
                context=context,
                has_subquery=bool(extra_dependency_ids),
                has_wildcard=any(self._is_projection_wildcard(projection) for projection in select.expressions),
                has_window=bool(window_exprs),
            ),
            relation_instances=context.instances,
            tables=self._physical_tables(context.instances),
            join_edges=join_edges,
            predicates=predicates,
            projections=projections,
            group_by_exprs=group_by_exprs,
            aggregate_exprs=aggregate_exprs,
            window_exprs=window_exprs,
            grouping_exprs=grouping_exprs,
            rollup_exprs=rollup_exprs,
            order_by_exprs=order_by_exprs,
            limit=self._limit(select),
            unsupported_reasons=self._unique(unsupported_reasons),
        )

    def _top_level_ctes(self, expression: exp.Expression) -> list[exp.CTE]:
        with_expression = expression.args.get("with_")
        if not isinstance(with_expression, exp.With):
            return []
        return [cte for cte in with_expression.expressions if isinstance(cte, exp.CTE)]

    def _cte_names(self, expression: exp.Expression) -> set[str]:
        return {cte.alias.lower() for cte in self._top_level_ctes(expression) if cte.alias}

    def _first_select(self, expression: exp.Expression) -> exp.Select | None:
        if isinstance(expression, exp.Select):
            return expression
        if isinstance(expression, exp.Subquery):
            return self._first_select(expression.this)
        if isinstance(expression, exp.SetOperation):
            return self._first_select(expression.left)
        return expression.find(exp.Select)

    def _set_operation_selects(self, expression: exp.SetOperation) -> Iterable[exp.Select]:
        for side in (expression.left, expression.right):
            if isinstance(side, exp.SetOperation):
                yield from self._set_operation_selects(side)
            else:
                select = self._first_select(side)
                if select is not None:
                    yield select

    def _set_operation_flag(self, expression: exp.SetOperation) -> str:
        if isinstance(expression, exp.Union):
            return "has_union"
        if isinstance(expression, exp.Intersect):
            return "has_intersect"
        if isinstance(expression, exp.Except):
            return "has_except"
        return "has_set_operation"

    def _relation_context(
        self,
        *,
        select: exp.Select,
        cte_qb_ids: dict[str, str],
        derived_refs: dict[int, _SourceRef],
        outer_context: _RelationContext | None = None,
    ) -> tuple[_RelationContext, list[str]]:
        refs: list[_SourceRef] = []
        reasons: list[str] = []
        used_internal_ids: dict[str, int] = {}

        for source in self._top_level_sources(select):
            if id(source) in derived_refs:
                refs.append(derived_refs[id(source)])
                continue
            source_ref, reason = self._source_ref(source, cte_qb_ids, used_internal_ids)
            if source_ref is not None:
                refs.append(source_ref)
            if reason:
                reasons.append(reason)

        instances = [ref.relation for ref in refs]
        by_name: dict[str, _SourceRef] = {}
        for ref in refs:
            for name in ref.lookup_names:
                by_name.setdefault(name.lower(), ref)

        return _RelationContext(
            instances=instances,
            by_name=by_name,
            cte_names=set(cte_qb_ids),
            outer_context=outer_context,
        ), reasons

    def _source_ref(
        self,
        source: exp.Expression,
        cte_qb_ids: dict[str, str],
        used_internal_ids: dict[str, int],
    ) -> tuple[_SourceRef | None, str | None]:
        if isinstance(source, exp.Table):
            source_name = source.name.lower()
            alias = source.alias.lower() if source.alias else None
            internal_id = self._dedupe_internal_id(alias or source_name, used_internal_ids)
            lookup_names = self._unique([source_name, alias])
            if self.schema.is_physical_table(source_name):
                relation = RelationInstance(
                    relation_id=source_name,
                    source_name=source_name,
                    physical_table=source_name,
                    relation_type="physical",
                )
                return (
                    _SourceRef(
                        relation=relation,
                        lookup_names=frozenset(lookup_names),
                        internal_id=internal_id,
                    ),
                    None,
                )
            if source_name in cte_qb_ids:
                cte_qb_id = cte_qb_ids[source_name]
                relation = RelationInstance(
                    relation_id=cte_qb_id,
                    source_name=source_name,
                    physical_table=None,
                    relation_type="cte",
                )
                return (
                    _SourceRef(
                        relation=relation,
                        lookup_names=frozenset(lookup_names),
                        internal_id=internal_id,
                        output_schema=self._output_schemas.get(cte_qb_id, {}),
                    ),
                    None,
                )
            relation = RelationInstance(
                relation_id=source_name,
                source_name=source_name,
                physical_table=None,
                relation_type="unknown",
            )
            return (
                _SourceRef(
                    relation=relation,
                    lookup_names=frozenset(lookup_names),
                    internal_id=internal_id,
                ),
                f"unknown_relation_source:{source_name}",
            )

        if isinstance(source, exp.Subquery):
            relation_id = self._dedupe_internal_id("derived_source", used_internal_ids)
            relation = RelationInstance(
                relation_id=relation_id,
                source_name=relation_id,
                physical_table=None,
                relation_type="derived",
            )
            return (
                _SourceRef(
                    relation=relation,
                    lookup_names=frozenset([relation_id]),
                    internal_id=relation_id,
                ),
                f"unregistered_derived_source:{relation_id}",
            )

        relation_id = self._dedupe_internal_id(type(source).__name__.lower(), used_internal_ids)
        relation = RelationInstance(
            relation_id=relation_id,
            source_name=relation_id,
            physical_table=None,
            relation_type="unknown",
        )
        return (
            _SourceRef(
                relation=relation,
                lookup_names=frozenset([relation_id]),
                internal_id=relation_id,
            ),
            f"unsupported_relation_source:{type(source).__name__}",
        )

    def _dedupe_internal_id(self, value: str, used_internal_ids: dict[str, int]) -> str:
        count = used_internal_ids.get(value, 0) + 1
        used_internal_ids[value] = count
        if count == 1:
            return value
        return f"{value}__{count}"

    def _top_level_sources(self, select: exp.Select) -> list[exp.Expression]:
        sources: list[exp.Expression] = []
        from_expression = select.args.get("from_")
        if isinstance(from_expression, exp.From) and from_expression.this is not None:
            sources.append(from_expression.this)
        for join in select.args.get("joins") or []:
            if isinstance(join, exp.Join) and join.this is not None:
                sources.append(join.this)
        return sources

    def _depends_on_qb_ids(self, context: _RelationContext, cte_qb_ids: dict[str, str]) -> list[str]:
        dependencies = [
            cte_qb_ids[instance.source_name]
            for instance in context.instances
            if instance.relation_type == "cte" and instance.source_name in cte_qb_ids
        ]
        return self._unique(dependencies)

    def _join_edges(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[JoinEdge], set[str], list[str]]:
        conditions: list[tuple[exp.Expression, str]] = []
        for join in select.args.get("joins") or []:
            if isinstance(join, exp.Join) and join.args.get("on") is not None:
                conditions.extend((condition, "join_on") for condition in self._split_conjunction(join.args["on"]))

        where = select.args.get("where")
        if isinstance(where, exp.Where):
            conditions.extend((condition, "where") for condition in self._split_conjunction(where.this))

        edges: list[JoinEdge] = []
        edge_condition_sqls: set[str] = set()
        reasons: list[str] = []
        seen_condition_sqls: set[str] = set()
        for condition, source in conditions:
            raw_condition_sql = condition.sql(dialect=self.dialect)
            if raw_condition_sql in seen_condition_sqls:
                edge_condition_sqls.add(raw_condition_sql)
                continue
            edge, reason = self._join_edge_from_condition(condition, context)
            if reason:
                reasons.append(reason)
            if edge is None:
                if source == "join_on" and self._contains_join_like_or(condition, context):
                    reasons.append(f"complex_or_join_condition:{raw_condition_sql}")
                continue
            seen_condition_sqls.add(raw_condition_sql)
            edges.append(edge)
            edge_condition_sqls.add(raw_condition_sql)
        return edges, edge_condition_sqls, reasons

    def _join_edge_from_condition(
        self,
        condition: exp.Expression,
        context: _RelationContext,
    ) -> tuple[JoinEdge | None, str | None]:
        condition = self._unwrap(condition)
        if not isinstance(condition, exp.EQ):
            return None, None
        left = condition.left
        right = condition.right
        if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
            return None, None

        left_column = self._resolve_column(left, context)
        right_column = self._resolve_column(right, context)
        if left_column.reason:
            return None, left_column.reason
        if right_column.reason:
            return None, right_column.reason
        if left_column.source_key == right_column.source_key and left_column.table == right_column.table:
            return None, None
        if not left_column.table or not right_column.table:
            return None, f"join_edge_requires_physical_lineage:{condition.sql(dialect=self.dialect)}"

        return (
            JoinEdge(
                left_relation_id=left_column.relation_id,
                left_table=left_column.table,
                left_column=left_column.column,
                right_relation_id=right_column.relation_id,
                right_table=right_column.table,
                right_column=right_column.column,
                operator="=",
                expr=self._expression_sql(condition, context),
            ),
            None,
        )

    def _predicates(
        self,
        select: exp.Select,
        context: _RelationContext,
        join_condition_sqls: set[str],
    ) -> tuple[list[PredicateFeature], list[str]]:
        predicates: list[PredicateFeature] = []
        reasons: list[str] = []

        where = select.args.get("where")
        if isinstance(where, exp.Where):
            for condition in self._split_conjunction(where.this):
                if condition.sql(dialect=self.dialect) in join_condition_sqls:
                    continue
                feature, feature_reasons = self._predicate_feature(condition, context)
                predicates.append(feature)
                reasons.extend(feature_reasons)
                if self._contains_join_like_or(condition, context):
                    reasons.append(f"complex_or_join_condition:{condition.sql(dialect=self.dialect)}")

        having = select.args.get("having")
        if isinstance(having, exp.Having):
            for condition in self._split_conjunction(having.this):
                feature, feature_reasons = self._predicate_feature(condition, context)
                predicates.append(feature)
                reasons.extend(feature_reasons)

        return predicates, reasons

    def _predicate_feature(
        self,
        expression: exp.Expression,
        context: _RelationContext,
    ) -> tuple[PredicateFeature, list[str]]:
        columns, relation_ids, reasons = self._column_lineage(expression, context)
        normalized = self._normalize_expression(expression, context)
        shape = normalized.copy().transform(
            lambda node: exp.Placeholder() if isinstance(node, exp.Literal) else node
        ).sql(dialect=self.dialect)
        return (
            PredicateFeature(
                expr=normalized.sql(dialect=self.dialect),
                predicate_shape=shape,
                columns=columns,
                relation_ids=relation_ids,
            ),
            reasons,
        )

    def _projections(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[ProjectionFeature], dict[str, _OutputColumn], dict[str, exp.Expression], list[str]]:
        projections: list[ProjectionFeature] = []
        output_schema: dict[str, _OutputColumn] = {}
        projection_alias_map: dict[str, exp.Expression] = {}
        reasons: list[str] = []

        for projection in select.expressions:
            inner = self._projection_inner(projection)
            columns, relation_ids, projection_reasons = self._column_lineage(projection, context)
            reasons.extend(projection_reasons)
            if self._is_projection_wildcard(projection):
                reasons.append(f"wildcard_projection_not_expanded:{projection.sql(dialect=self.dialect)}")
                for name, output in self._wildcard_output_schema(inner, context).items():
                    output_schema.setdefault(name, output)
            output_name = projection.output_name or projection.alias_or_name or None
            projections.append(
                ProjectionFeature(
                    expr=self._expression_sql(projection, context),
                    output_name=output_name,
                    columns=columns,
                    relation_ids=relation_ids,
                )
            )
            if output_name and not self._is_projection_wildcard(projection):
                direct_table, direct_column = self._direct_projection_source(inner, context)
                output_schema[output_name.lower()] = _OutputColumn(
                    output_name=output_name,
                    expr=self._expression_sql(inner, context),
                    columns=tuple(columns),
                    relation_ids=tuple(relation_ids),
                    direct_table=direct_table,
                    direct_column=direct_column,
                )
                projection_alias_map[output_name.lower()] = inner
        return projections, output_schema, projection_alias_map, reasons

    def _wildcard_output_schema(
        self,
        expression: exp.Expression,
        context: _RelationContext,
    ) -> dict[str, _OutputColumn]:
        table_name = expression.table.lower() if isinstance(expression, exp.Column) and expression.table else None
        if table_name:
            source = context.by_name.get(table_name)
            sources = [source] if source is not None else []
        else:
            sources = self._unique_sources(context.by_name.values())

        output_schema: dict[str, _OutputColumn] = {}
        for source in sources:
            if source is None:
                continue
            for name, output in source.output_schema.items():
                output_schema.setdefault(name, output)
        return output_schema

    def _direct_projection_source(
        self,
        expression: exp.Expression,
        context: _RelationContext,
    ) -> tuple[str | None, str | None]:
        if isinstance(expression, exp.Column):
            resolved = self._resolve_column(expression, context)
            if resolved.reason is None and resolved.table:
                return resolved.table, resolved.column
        return None, None

    def _projection_inner(self, projection: exp.Expression) -> exp.Expression:
        if isinstance(projection, exp.Alias):
            return projection.this
        return projection

    def _is_projection_wildcard(self, projection: exp.Expression) -> bool:
        inner = self._projection_inner(projection)
        if isinstance(inner, exp.Star):
            return True
        if isinstance(inner, exp.Column):
            return isinstance(inner.this, exp.Star) or inner.name == "*"
        return False

    def _group_by_exprs(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[ExpressionFeature], list[str]]:
        group = select.args.get("group")
        if not isinstance(group, exp.Group):
            return [], []
        return self._expression_features(group.expressions, context)

    def _rollup_exprs(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[ExpressionFeature], list[str]]:
        group = select.args.get("group")
        if not isinstance(group, exp.Group):
            return [], []
        rollup = group.args.get("rollup")
        if not rollup:
            return [], []
        if isinstance(rollup, exp.Expression):
            return self._expression_features(rollup.expressions, context)
        if isinstance(rollup, list):
            return self._expression_features(rollup, context)
        return [], [f"unsupported_rollup_expression:{type(rollup).__name__}"]

    def _order_by_exprs(
        self,
        select: exp.Select,
        context: _RelationContext,
        projection_alias_map: dict[str, exp.Expression],
    ) -> tuple[list[ExpressionFeature], list[str]]:
        order = select.args.get("order")
        if not isinstance(order, exp.Order):
            return [], []
        features: list[ExpressionFeature] = []
        reasons: list[str] = []
        for ordered in order.expressions:
            expression = ordered.this if isinstance(ordered, exp.Ordered) else ordered
            alias_expression = self._order_alias_expression(expression, projection_alias_map)
            lineage_expression = alias_expression or self._replace_projection_aliases(expression, projection_alias_map)
            columns, relation_ids, expression_reasons = self._column_lineage(lineage_expression, context)
            features.append(
                ExpressionFeature(
                    expr=expression.sql(dialect=self.dialect),
                    columns=columns,
                    relation_ids=relation_ids,
                )
            )
            reasons.extend(expression_reasons)
        return features, reasons

    def _order_alias_expression(
        self,
        expression: exp.Expression,
        projection_alias_map: dict[str, exp.Expression],
    ) -> exp.Expression | None:
        if isinstance(expression, exp.Column) and not expression.table:
            return projection_alias_map.get(expression.name.lower())
        return None

    def _replace_projection_aliases(
        self,
        expression: exp.Expression,
        projection_alias_map: dict[str, exp.Expression],
    ) -> exp.Expression:
        expression_copy = expression.copy()

        def transform(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and not node.table:
                alias_expression = projection_alias_map.get(node.name.lower())
                if alias_expression is not None:
                    return alias_expression.copy()
            return node

        return expression_copy.transform(transform)

    def _expression_features(
        self,
        expressions: Iterable[exp.Expression],
        context: _RelationContext,
    ) -> tuple[list[ExpressionFeature], list[str]]:
        features: list[ExpressionFeature] = []
        reasons: list[str] = []
        for expression in expressions:
            columns, relation_ids, expression_reasons = self._column_lineage(expression, context)
            features.append(
                ExpressionFeature(
                    expr=self._expression_sql(expression, context),
                    columns=columns,
                    relation_ids=relation_ids,
                )
            )
            reasons.extend(expression_reasons)
        return features, reasons

    def _aggregate_exprs(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[AggregateFeature], list[str]]:
        aggregate_features: list[AggregateFeature] = []
        reasons: list[str] = []
        seen: set[str] = set()
        for projection in select.expressions:
            output_name = projection.output_name or projection.alias_or_name or None
            for aggregate in self._current_scope_nodes(projection, exp.AggFunc, skip_window=True):
                if isinstance(aggregate, exp.Grouping):
                    continue
                normalized_expr = self._expression_sql(aggregate, context)
                if normalized_expr in seen:
                    continue
                seen.add(normalized_expr)
                columns, relation_ids, aggregate_reasons = self._column_lineage(aggregate, context)
                reasons.extend(aggregate_reasons)
                source_table = None
                source_column = None
                source_relation_id = None
                resolved_columns = [
                    self._resolve_column(column, context)
                    for column in self._current_scope_nodes(aggregate, exp.Column)
                ]
                resolved_without_reasons = [column for column in resolved_columns if column.reason is None]
                direct_resolved = [
                    column
                    for column in resolved_without_reasons
                    if column.table and len(column.columns) <= 1
                ]
                if len(direct_resolved) == 1:
                    source = direct_resolved[0]
                    source_table = source.table
                    source_column = source.column
                    source_relation_id = source.relation_id
                elif len(resolved_without_reasons) > 1:
                    reasons.append(f"aggregate_multi_column_source:{normalized_expr}")

                aggregate_features.append(
                    AggregateFeature(
                        expr=normalized_expr,
                        agg_func=aggregate.key.upper(),
                        source_table=source_table,
                        source_column=source_column,
                        source_relation_id=source_relation_id,
                        output_name=output_name,
                        columns=columns,
                        relation_ids=relation_ids,
                    )
                )
        return aggregate_features, reasons

    def _window_exprs(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[ExpressionFeature], list[str]]:
        return self._expression_features(self._unique_expressions(self._current_scope_nodes(select, exp.Window)), context)

    def _grouping_exprs(
        self,
        select: exp.Select,
        context: _RelationContext,
    ) -> tuple[list[ExpressionFeature], list[str]]:
        return self._expression_features(self._unique_expressions(self._current_scope_nodes(select, exp.Grouping)), context)

    def _limit(self, select: exp.Select) -> str | None:
        limit = select.args.get("limit")
        if not isinstance(limit, exp.Limit):
            return None
        expression = limit.args.get("expression")
        if expression is None:
            return None
        return expression.sql(dialect=self.dialect)

    def _structural_flags(
        self,
        *,
        select: exp.Select,
        context: _RelationContext,
        has_subquery: bool,
        has_wildcard: bool,
        has_window: bool,
    ) -> list[str]:
        flags: list[str] = []
        if select.args.get("with_") is not None:
            flags.append("has_cte")
        if has_subquery:
            flags.append("has_subquery")
        if has_window:
            flags.append("has_window")
        if select.args.get("having") is not None:
            flags.append("has_having")
        if select.args.get("group") is not None:
            flags.append("has_group_by")
        if select.args.get("distinct") is not None:
            flags.append("has_distinct")
        if select.args.get("order") is not None:
            flags.append("has_order_by")
        if select.args.get("limit") is not None:
            flags.append("has_limit")
        if has_wildcard:
            flags.append("has_wildcard")

        physical_tables = [instance.physical_table for instance in context.instances if instance.physical_table]
        if len(physical_tables) != len(set(physical_tables)):
            flags.append("has_self_join")
        return self._unique(flags)

    def _column_lineage(
        self,
        expression: exp.Expression,
        context: _RelationContext,
    ) -> tuple[list[str], list[str], list[str]]:
        columns: list[str] = []
        relation_ids: list[str] = []
        reasons: list[str] = []
        for column in self._current_scope_nodes(expression, exp.Column):
            resolved = self._resolve_column(column, context)
            if resolved.reason:
                reasons.append(resolved.reason)
            if resolved.columns:
                columns.extend(resolved.columns)
            elif resolved.table:
                columns.append(f"{resolved.table}.{resolved.column}")
            else:
                columns.append(resolved.column)
            if resolved.relation_ids:
                relation_ids.extend(resolved.relation_ids)
            elif resolved.relation_id:
                relation_ids.append(resolved.relation_id)
        return self._unique(columns), self._unique(relation_ids), self._unique(reasons)

    def _resolve_column(self, column: exp.Column, context: _RelationContext) -> _ResolvedColumn:
        column_name = column.name.lower()
        table_or_alias = column.table.lower() if column.table else ""
        if table_or_alias:
            source = context.by_name.get(table_or_alias)
            if source is None:
                if context.outer_context is not None:
                    outer_resolved = self._resolve_column(column, context.outer_context)
                    if outer_resolved.reason is None:
                        return outer_resolved
                if self.schema.is_physical_table(table_or_alias):
                    return self._resolved_physical(table_or_alias, column_name, table_or_alias)
                return _ResolvedColumn(
                    relation_id=table_or_alias,
                    table=None,
                    column=column_name,
                    columns=(column_name,),
                    relation_ids=(table_or_alias,),
                    source_key=table_or_alias,
                    reason=f"unknown_column_relation:{table_or_alias}.{column_name}",
                )
            return self._resolve_from_source(source, column_name)

        physical_candidates = [
            source
            for source in context.by_name.values()
            if source.relation.physical_table
            and self.schema.table_has_column(source.relation.physical_table, column_name)
        ]
        physical_candidates = self._unique_sources(physical_candidates)
        if len(physical_candidates) == 1:
            source = physical_candidates[0]
            return self._resolved_physical(source.relation.physical_table or "", column_name, source.internal_id)
        if len(physical_candidates) > 1:
            sources = ",".join(source.internal_id for source in physical_candidates)
            return _ResolvedColumn(
                relation_id=None,
                table=None,
                column=column_name,
                columns=(column_name,),
                relation_ids=(),
                reason=f"ambiguous_unqualified_column:{column_name}:{sources}",
            )

        output_candidates = [
            source
            for source in context.by_name.values()
            if column_name in source.output_schema
        ]
        output_candidates = self._unique_sources(output_candidates)
        if len(output_candidates) == 1:
            return self._resolve_from_source(output_candidates[0], column_name)
        if len(output_candidates) > 1:
            sources = ",".join(source.relation.relation_id for source in output_candidates)
            return _ResolvedColumn(
                relation_id=None,
                table=None,
                column=column_name,
                columns=(column_name,),
                relation_ids=(),
                reason=f"ambiguous_output_column:{column_name}:{sources}",
            )

        if context.outer_context is not None:
            outer_resolved = self._resolve_column(column, context.outer_context)
            if outer_resolved.reason is None:
                return outer_resolved

        schema_tables = self.schema.tables_for_column(column_name)
        if len(schema_tables) == 1:
            table = schema_tables[0]
            return self._resolved_physical(table, column_name, table)
        if len(schema_tables) > 1:
            return _ResolvedColumn(
                relation_id=None,
                table=None,
                column=column_name,
                columns=(column_name,),
                relation_ids=(),
                reason=f"ambiguous_schema_column:{column_name}:{','.join(schema_tables)}",
            )
        return _ResolvedColumn(
            relation_id=None,
            table=None,
            column=column_name,
            columns=(column_name,),
            relation_ids=(),
            reason=f"unresolved_column:{column_name}",
        )

    def _resolve_from_source(self, source: _SourceRef, column_name: str) -> _ResolvedColumn:
        if source.relation.physical_table:
            return self._resolved_physical(source.relation.physical_table, column_name, source.internal_id)
        output = source.output_schema.get(column_name)
        if output:
            relation_ids = output.relation_ids or ((output.direct_table,) if output.direct_table else (source.relation.relation_id,))
            columns = output.columns or ((f"{output.direct_table}.{output.direct_column}",) if output.direct_table and output.direct_column else (column_name,))
            return _ResolvedColumn(
                relation_id=relation_ids[0] if len(relation_ids) == 1 else source.relation.relation_id,
                table=output.direct_table,
                column=output.direct_column or column_name,
                columns=columns,
                relation_ids=relation_ids,
                source_key=source.internal_id,
            )
        return _ResolvedColumn(
            relation_id=source.relation.relation_id,
            table=None,
            column=column_name,
            columns=(column_name,),
            relation_ids=(source.relation.relation_id,),
            source_key=source.internal_id,
            reason=f"unresolved_source_column:{source.relation.relation_id}.{column_name}",
        )

    def _resolved_physical(self, table: str, column: str, source_key: str) -> _ResolvedColumn:
        return _ResolvedColumn(
            relation_id=table,
            table=table,
            column=column,
            columns=(f"{table}.{column}",),
            relation_ids=(table,),
            source_key=source_key,
        )

    def _expression_sql(self, expression: exp.Expression, context: _RelationContext) -> str:
        return self._normalize_expression(expression, context).sql(dialect=self.dialect)

    def _normalize_expression(self, expression: exp.Expression, context: _RelationContext) -> exp.Expression:
        expression_copy = expression.copy()

        def transform(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column):
                resolved = self._resolve_column(node, context)
                if resolved.table:
                    return exp.column(resolved.column, table=resolved.table)
            return node

        return expression_copy.transform(transform)

    def _predicate_subquery_expressions(self, select: exp.Select) -> list[exp.Expression]:
        containers: list[exp.Expression] = list(select.expressions)
        for key in ("where", "having"):
            clause = select.args.get(key)
            if isinstance(clause, (exp.Where, exp.Having)):
                containers.append(clause.this)
        order = select.args.get("order")
        if isinstance(order, exp.Order):
            containers.extend(ordered.this if isinstance(ordered, exp.Ordered) else ordered for ordered in order.expressions)

        expressions: list[exp.Expression] = []
        seen: set[int] = set()
        for container in containers:
            for subquery in self._iter_subquery_expressions(container):
                if id(subquery) in seen:
                    continue
                seen.add(id(subquery))
                expressions.append(subquery)
        return expressions

    def _iter_subquery_expressions(self, expression: exp.Expression) -> Iterable[exp.Expression]:
        def visit(node: exp.Expression, is_root: bool = False) -> Iterable[exp.Expression]:
            if not is_root:
                if isinstance(node, exp.Subquery):
                    yield node.this
                    return
                if isinstance(node, exp.Exists) and isinstance(node.this, exp.Select):
                    yield node.this
                    return
                if isinstance(node, exp.Select):
                    yield node
                    return
            for child in node.iter_expressions():
                yield from visit(child)

        yield from visit(expression, is_root=True)

    def _current_scope_nodes(
        self,
        expression: exp.Expression,
        node_type: type[exp.Expression],
        *,
        skip_window: bool = False,
    ) -> list[exp.Expression]:
        nodes: list[exp.Expression] = []

        def visit(node: exp.Expression, *, is_root: bool = False, inside_window: bool = False) -> None:
            if not is_root and isinstance(node, (exp.Subquery, exp.Select)):
                return
            next_inside_window = inside_window or isinstance(node, exp.Window)
            if isinstance(node, node_type) and not (skip_window and next_inside_window):
                nodes.append(node)
            for child in node.iter_expressions():
                visit(child, inside_window=next_inside_window)

        visit(expression, is_root=True)
        return nodes

    def _unique_expressions(self, expressions: Iterable[exp.Expression]) -> list[exp.Expression]:
        unique_expressions: list[exp.Expression] = []
        seen: set[str] = set()
        for expression in expressions:
            sql = expression.sql(dialect=self.dialect)
            if sql in seen:
                continue
            seen.add(sql)
            unique_expressions.append(expression)
        return unique_expressions

    def _split_conjunction(self, expression: exp.Expression) -> list[exp.Expression]:
        expression = self._unwrap(expression)
        if isinstance(expression, exp.And):
            return [
                *self._split_conjunction(expression.left),
                *self._split_conjunction(expression.right),
            ]
        return [expression]

    def _unwrap(self, expression: exp.Expression) -> exp.Expression:
        while isinstance(expression, exp.Paren):
            expression = expression.this
        return expression

    def _contains_join_like_or(self, expression: exp.Expression, context: _RelationContext) -> bool:
        if not expression.find(exp.Or):
            return False
        for eq in self._current_scope_nodes(expression, exp.EQ):
            if not isinstance(eq.left, exp.Column) or not isinstance(eq.right, exp.Column):
                continue
            left = self._resolve_column(eq.left, context)
            right = self._resolve_column(eq.right, context)
            if left.reason is None and right.reason is None and left.source_key != right.source_key:
                return True
        return False

    def _merge_set_output_schema(self, branch_ids: list[str]) -> dict[str, _OutputColumn]:
        for branch_id in branch_ids:
            schema = self._output_schemas.get(branch_id)
            if schema:
                return dict(schema)
        return {}

    def _physical_tables(self, instances: list[RelationInstance]) -> list[str]:
        return self._unique(
            instance.physical_table
            for instance in instances
            if instance.physical_table is not None
        )

    def _unique_sources(self, sources: Iterable[_SourceRef]) -> list[_SourceRef]:
        unique_sources: list[_SourceRef] = []
        seen: set[str] = set()
        for source in sources:
            if source.internal_id in seen:
                continue
            seen.add(source.internal_id)
            unique_sources.append(source)
        return unique_sources

    def _unique(self, values: Iterable[str | None]) -> list[str]:
        unique_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value is None:
                continue
            if value in seen:
                continue
            seen.add(value)
            unique_values.append(value)
        return unique_values
