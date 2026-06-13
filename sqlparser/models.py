from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RelationInstance:
    relation_id: str
    source_name: str
    physical_table: str | None
    relation_type: str = "physical"
    role: str | None = None


@dataclass
class JoinEdge:
    left_relation_id: str | None
    left_table: str | None
    left_column: str
    right_relation_id: str | None
    right_table: str | None
    right_column: str
    operator: str
    expr: str


@dataclass
class ExpressionFeature:
    expr: str
    columns: list[str] = field(default_factory=list)
    relation_ids: list[str] = field(default_factory=list)


@dataclass
class PredicateFeature(ExpressionFeature):
    predicate_shape: str = ""


@dataclass
class ProjectionFeature(ExpressionFeature):
    output_name: str | None = None


@dataclass
class AggregateFeature(ExpressionFeature):
    agg_func: str = ""
    source_table: str | None = None
    source_column: str | None = None
    source_relation_id: str | None = None
    output_name: str | None = None


@dataclass
class QueryBlock:
    qb_id: str
    query_id: str
    block_name: str | None
    scope_type: str
    parent_qb_id: str | None = None
    depends_on_qb_ids: list[str] = field(default_factory=list)
    structural_flags: list[str] = field(default_factory=list)
    relation_instances: list[RelationInstance] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    join_edges: list[JoinEdge] = field(default_factory=list)
    predicates: list[PredicateFeature] = field(default_factory=list)
    projections: list[ProjectionFeature] = field(default_factory=list)
    group_by_exprs: list[ExpressionFeature] = field(default_factory=list)
    aggregate_exprs: list[AggregateFeature] = field(default_factory=list)
    window_exprs: list[ExpressionFeature] = field(default_factory=list)
    grouping_exprs: list[ExpressionFeature] = field(default_factory=list)
    rollup_exprs: list[ExpressionFeature] = field(default_factory=list)
    order_by_exprs: list[ExpressionFeature] = field(default_factory=list)
    limit: str | None = None
    unsupported_reasons: list[str] = field(default_factory=list)


@dataclass
class FeatureOutput:
    query_blocks: list[QueryBlock]
    query_to_qbs: dict[str, list[str]]
    qb_to_query: dict[str, str]


def to_dict(value: Any) -> Any:
    return asdict(value)
