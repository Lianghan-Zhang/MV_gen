from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ComplexityType = Literal["join", "join_filter", "join_filter_groupby", "other"]


class QueryBlock(BaseModel):
    qb_id: str
    query_id: str
    scope_type: str = "outer"
    tables: list[str] = Field(default_factory=list)
    join_edges: list[Any] = Field(default_factory=list)
    predicates: list[str] = Field(default_factory=list)
    group_by_exprs: list[str] = Field(default_factory=list)
    aggregate_exprs: list[str] = Field(default_factory=list)
    complexity_type: ComplexityType
    family_key: str
    unsupported_reasons: list[str] = Field(default_factory=list)


class FeatureOutput(BaseModel):
    query_blocks: list[QueryBlock]
    query_to_qbs: dict[str, list[str]]
    qb_to_query: dict[str, str]


class QueryFamily(BaseModel):
    family_id: str
    family_key: str
    members: list[str]
    common_tables: list[str] = Field(default_factory=list)
    common_join_skeleton: list[Any] = Field(default_factory=list)
    common_predicates: list[str] = Field(default_factory=list)
    predicate_shapes: list[str] = Field(default_factory=list)
    union_group_by_exprs: list[str] = Field(default_factory=list)
    union_measure_exprs: list[str] = Field(default_factory=list)


class FamilyOutput(BaseModel):
    query_families: list[QueryFamily]


class EvidenceRef(BaseModel):
    artifact: str | None = None
    event_id: str | None = None
    batch_id: int | None = None
    candidate_id: str | None = None
    mv_id: str | None = None
    query_ids: list[str] = Field(default_factory=list)
    event: str | None = None


class RuleSuggestion(BaseModel):
    target_rule: str
    suggestion: str
    suggested_rule_text: str
    reason: str
    evidence_refs: list[EvidenceRef]


class AgentRuleSuggestions(BaseModel):
    suggestions: list[RuleSuggestion] = Field(default_factory=list)


class SelfIterationFeedback(BaseModel):
    run_id: str
    agent_rule_suggestions: dict[str, AgentRuleSuggestions] = Field(default_factory=dict)


def validate_model(model_cls: type[BaseModel], data: dict[str, Any]) -> BaseModel:
    if hasattr(model_cls, "model_validate"):
        return model_cls.model_validate(data)
    return model_cls.parse_obj(data)


def model_to_dict(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def model_schema(model_cls: type[BaseModel]) -> dict[str, Any]:
    if hasattr(model_cls, "model_json_schema"):
        return model_cls.model_json_schema()
    return model_cls.schema()
