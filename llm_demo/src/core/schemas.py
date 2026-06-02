from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ComplexityType = Literal["join", "join_filter", "join_filter_groupby", "other"]
MVType = Literal["fine_grain_aggregate", "detail_superset", "dimension_only"]
MVDecision = Literal["materialize", "skip"]
RewriteStage = Literal["historical", "final"]
RewriteStatus = Literal["rewritten", "fallback"]
ExecutionStepType = Literal["materialize_mv", "run_query"]
ExecutionStatus = Literal["success", "failed", "skipped", "planned"]
FeatureStatus = Literal["success", "partial_success", "feature_failed", "unsupported_sql_pattern"]


class QueryBlock(BaseModel):
    qb_id: str
    query_id: str
    block_name: str | None = None
    scope_type: str = "outer"
    parent_qb_id: str | None = None
    depends_on_qb_ids: list[str] = Field(default_factory=list)
    structural_flags: list[str] = Field(default_factory=list)
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


class FeatureQueryStatus(BaseModel):
    query_id: str
    status: FeatureStatus
    attempts: int = 1
    qb_ids: list[str] = Field(default_factory=list)
    unsupported_reasons: list[str] = Field(default_factory=list)
    error_type: str | None = None
    error_message: str | None = None


class FeatureExtractStatus(BaseModel):
    run_id: str
    queries: list[FeatureQueryStatus] = Field(default_factory=list)


class FamilyCandidateMember(BaseModel):
    qb_id: str
    query_id: str
    tables: list[str] = Field(default_factory=list)
    predicates: list[str] = Field(default_factory=list)
    group_by_exprs: list[str] = Field(default_factory=list)
    aggregate_exprs: list[str] = Field(default_factory=list)


class FamilyPairEvidence(BaseModel):
    left_qb_id: str
    right_qb_id: str
    table_jaccard: float
    table_containment: float
    shared_tables: list[str] = Field(default_factory=list)
    shared_predicates: list[str] = Field(default_factory=list)
    shared_measures: list[str] = Field(default_factory=list)
    reason: str


class FamilyCandidate(BaseModel):
    candidate_family_id: str
    family_type: str
    candidate_key: str
    core_fact_table_set: list[str] = Field(default_factory=list)
    table_set: list[str] = Field(default_factory=list)
    join_signature: list[str] = Field(default_factory=list)
    predicate_set: list[str] = Field(default_factory=list)
    group_by_set: list[str] = Field(default_factory=list)
    measure_set: list[str] = Field(default_factory=list)
    members: list[FamilyCandidateMember] = Field(default_factory=list)
    pair_evidence: list[FamilyPairEvidence] = Field(default_factory=list)
    unsupported_qb_ids: list[str] = Field(default_factory=list)


class FamilyCandidateOutput(BaseModel):
    family_candidates: list[FamilyCandidate] = Field(default_factory=list)


class QueryFamily(BaseModel):
    family_id: str
    family_key: str
    family_type: str | None = None
    core_fact_table_set: list[str] = Field(default_factory=list)
    members: list[str]
    common_tables: list[str] = Field(default_factory=list)
    common_join_skeleton: list[Any] = Field(default_factory=list)
    common_predicates: list[str] = Field(default_factory=list)
    predicate_shapes: list[str] = Field(default_factory=list)
    union_group_by_exprs: list[str] = Field(default_factory=list)
    union_measure_exprs: list[str] = Field(default_factory=list)


class FamilyOutput(BaseModel):
    query_families: list[QueryFamily]


class FamilyGroup(BaseModel):
    family_id: str
    query_ids: list[str] = Field(default_factory=list)
    qb_ids: list[str] = Field(default_factory=list)


class ComplexityBatch(BaseModel):
    batch_id: int
    batch_type: ComplexityType
    query_ids: list[str] = Field(default_factory=list)
    family_groups: list[FamilyGroup] = Field(default_factory=list)


class BatchClusterOutput(BaseModel):
    complexity_batches: list[ComplexityBatch]


class GeneralizedPredicate(BaseModel):
    predicate_shape: str
    covered_values: list[Any] = Field(default_factory=list)
    source_query_ids: list[str] = Field(default_factory=list)


class ResidualFilter(BaseModel):
    query_id: str
    predicates: list[str] = Field(default_factory=list)


class MVColumnMapping(BaseModel):
    source_expr: str
    source_table: str
    source_column: str
    mv_column: str
    role: str


class MVCandidate(BaseModel):
    candidate_id: str
    source_batch_id: int
    source_query_ids: list[str]
    source_qb_ids: list[str] = Field(default_factory=list)
    family_id: str
    target_queries: list[str]
    target_qb_ids: list[str] = Field(default_factory=list)
    decision: MVDecision
    reason: str
    mv_id: str | None = None
    mv_type: MVType | None = None
    target_table_name: str | None = None
    depends_on_mv_ids: list[str] = Field(default_factory=list)
    mv_predicates: list[str] = Field(default_factory=list)
    generalized_predicates: list[GeneralizedPredicate] = Field(default_factory=list)
    residual_filters: list[ResidualFilter] = Field(default_factory=list)
    output_columns: list[str] = Field(default_factory=list)
    column_mappings: list[MVColumnMapping] = Field(default_factory=list)
    group_by_exprs: list[str] = Field(default_factory=list)
    measure_exprs: list[str] = Field(default_factory=list)
    build_sql: str | None = None


class BatchMVOutput(BaseModel):
    batch_id: int
    mv_candidates: list[MVCandidate] = Field(default_factory=list)


class SemanticCheck(BaseModel):
    status: str
    reason: str


class RewriteRecord(BaseModel):
    query_id: str
    rewrite_stage: RewriteStage
    status: RewriteStatus
    used_mv_ids: list[str] = Field(default_factory=list)
    original_sql_path: str
    rewritten_sql_path: str
    rewrite_meta_path: str
    rewrite_mode: str
    target_qb_ids: list[str] = Field(default_factory=list)
    rewritten_sql: str
    residual_filters: list[Any] = Field(default_factory=list)
    rollup_exprs: list[Any] = Field(default_factory=list)
    semantic_check: SemanticCheck
    fallback_reason: str | None = None


class RewriteOutput(BaseModel):
    rewrites: list[RewriteRecord]


class ExecutionStep(BaseModel):
    step_order: int
    step_type: ExecutionStepType
    status: ExecutionStatus
    query_id: str | None = None
    candidate_id: str | None = None
    mv_id: str | None = None
    sql_path: str | None = None
    meta_path: str | None = None
    reason: str | None = None
    depends_on_mv_ids: list[str] = Field(default_factory=list)


class ExecutionOrder(BaseModel):
    run_id: str
    batch_id: int
    mode: str = "dry_run"
    steps: list[ExecutionStep] = Field(default_factory=list)


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


class CoverageSummary(BaseModel):
    run_id: str
    total_queries: int = 0
    feature_status_counts: dict[str, int] = Field(default_factory=dict)
    family_candidate_count: int = 0
    family_count: int = 0
    batch_query_counts: dict[str, int] = Field(default_factory=dict)
    mv_candidate_counts: dict[str, int] = Field(default_factory=dict)
    rewrite_status_counts: dict[str, int] = Field(default_factory=dict)
    execution_step_counts: dict[str, int] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


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
