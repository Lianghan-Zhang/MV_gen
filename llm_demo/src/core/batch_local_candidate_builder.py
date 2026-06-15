from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Any

from llm_demo.src.core.family_candidate_builder import TPCDS_FACT_TABLES


class BatchLocalCandidateBuilder:
    """Build deterministic batch-local MV candidate groups from QueryBlock features."""

    def __init__(
        self,
        *,
        table_jaccard_threshold: float = 0.6,
        table_containment_threshold: float = 0.8,
        max_qbs_per_group: int = 12,
        max_query_ids_per_group: int = 8,
        max_pair_evidence_sent_to_llm: int = 20,
    ) -> None:
        self.table_jaccard_threshold = table_jaccard_threshold
        self.table_containment_threshold = table_containment_threshold
        self.max_qbs_per_group = max_qbs_per_group
        self.max_query_ids_per_group = max_query_ids_per_group
        self.max_pair_evidence_sent_to_llm = max_pair_evidence_sent_to_llm

    def build(self, *, batch_id: int, query_blocks: list[dict[str, Any]]) -> dict[str, Any]:
        ordered_blocks = sorted(query_blocks, key=self._block_sort_key)
        supported_blocks = [block for block in ordered_blocks if not block.get("unsupported_reasons")]
        rejected_blocks = [
            {
                "qb_id": block.get("qb_id"),
                "query_id": block.get("query_id"),
                "unsupported_reasons": list(block.get("unsupported_reasons", [])),
            }
            for block in ordered_blocks
            if block.get("unsupported_reasons")
        ]
        profiles = {block["qb_id"]: self._profile(block) for block in supported_blocks}
        pairwise_similarity, eligible_pair_records = self._pairwise_similarity(supported_blocks, profiles)
        eligible_pair_lookup = self._pair_lookup(eligible_pair_records)
        candidate_groups = self._candidate_groups(
            supported_blocks=supported_blocks,
            profiles=profiles,
            eligible_pair_lookup=eligible_pair_lookup,
        )

        return {
            "batch_id": batch_id,
            "thresholds": {
                "table_jaccard": self.table_jaccard_threshold,
                "table_containment": self.table_containment_threshold,
                "max_qbs_per_group": self.max_qbs_per_group,
                "max_query_ids_per_group": self.max_query_ids_per_group,
                "max_pair_evidence_sent_to_llm": self.max_pair_evidence_sent_to_llm,
            },
            "query_block_inventory": [self._inventory_record(block, profiles.get(block.get("qb_id"))) for block in ordered_blocks],
            "pairwise_similarity": pairwise_similarity,
            "candidate_groups": candidate_groups,
            "rejected_query_blocks": rejected_blocks,
        }

    def _pairwise_similarity(
        self,
        blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        pairwise_similarity: list[dict[str, Any]] = []
        eligible_pair_records: list[dict[str, Any]] = []
        for left, right in combinations(blocks, 2):
            evidence = self._pair_evidence(profiles[left["qb_id"]], profiles[right["qb_id"]])
            recall_rule, recall_reasons, rejected_reason = self._recall_decision(evidence)
            pair_record = {
                **evidence,
                "eligible": rejected_reason is None,
                "recall_rule": recall_rule,
                "recall_reasons": recall_reasons,
                "rejected_reason": rejected_reason,
            }
            pairwise_similarity.append(pair_record)
            if rejected_reason is None:
                eligible_pair_records.append(pair_record)
        return pairwise_similarity, eligible_pair_records

    def _candidate_groups(
        self,
        *,
        supported_blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        buckets: dict[tuple[str, str, tuple[str, ...], str], list[dict[str, Any]]] = defaultdict(list)
        for block in supported_blocks:
            buckets[self._compatibility_key(profiles[block["qb_id"]])].append(block)

        groups = []
        grouped_qb_ids: set[str] = set()
        for bucket_blocks in buckets.values():
            bucket_groups = self._bucket_groups(
                blocks=bucket_blocks,
                profiles=profiles,
                eligible_pair_lookup=eligible_pair_lookup,
            )
            for group in bucket_groups:
                grouped_qb_ids.update(group["qb_ids"])
            groups.extend(bucket_groups)

        for block in supported_blocks:
            if block["qb_id"] in grouped_qb_ids:
                continue
            groups.append(
                self._candidate_group(
                    group_type="single_query_candidate",
                    recall_rule="unpaired_supported_query_block",
                    recall_reasons=["no_pair_candidate_passed_recall"],
                    blocks=[block],
                    profiles=profiles,
                    eligible_pair_lookup=eligible_pair_lookup,
                    split_reason=None,
                )
            )
        return self._assign_group_ids(groups)

    def _bucket_groups(
        self,
        *,
        blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if len(blocks) < 2:
            return []

        groups: list[dict[str, Any]] = []
        exact_groups = self._exact_shape_groups(blocks, profiles, eligible_pair_lookup)
        groups.extend(exact_groups)

        groups.extend(self._superset_anchor_groups(blocks, profiles, eligible_pair_lookup))
        return self._split_oversized_groups(groups, profiles, eligible_pair_lookup)

    def _exact_shape_groups(
        self,
        blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        partitions: dict[tuple[tuple[str, ...], tuple[str, ...], str], list[dict[str, Any]]] = defaultdict(list)
        for block in blocks:
            profile = profiles[block["qb_id"]]
            partitions[
                (
                    tuple(profile["table_set"]),
                    tuple(profile["join_signature"]),
                    profile["measure_key"],
                )
            ].append(block)

        groups = []
        for partition_blocks in partitions.values():
            if len(partition_blocks) < 2:
                continue
            candidate_groups = self._complete_linkage_groups(
                blocks=partition_blocks,
                profiles=profiles,
                compatible=lambda left, right: self._exact_compatible(left, right, eligible_pair_lookup),
            )
            for candidate_blocks in candidate_groups:
                groups.append(
                    self._candidate_group(
                        group_type=self._exact_group_type([profiles[block["qb_id"]] for block in candidate_blocks]),
                        recall_rule="exact_mv_compatibility_key",
                        recall_reasons=["same_scope_family_fact_measure_table_join"],
                        blocks=candidate_blocks,
                        profiles=profiles,
                        eligible_pair_lookup=eligible_pair_lookup,
                        split_reason=None,
                    )
                )
        return groups

    def _superset_anchor_groups(
        self,
        blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        groups = []
        anchors = sorted(
            blocks,
            key=lambda block: (-len(profiles[block["qb_id"]]["table_set"]), self._block_sort_key(block)),
        )
        seen_group_keys: set[tuple[str, ...]] = set()
        for anchor in anchors:
            anchor_profile = profiles[anchor["qb_id"]]
            direct_members = []
            for member in sorted(blocks, key=self._block_sort_key):
                if member["qb_id"] == anchor["qb_id"]:
                    continue
                member_profile = profiles[member["qb_id"]]
                if not self._anchor_direct_compatible(anchor_profile, member_profile, eligible_pair_lookup):
                    continue
                direct_members.append(member)

            member_groups: list[list[dict[str, Any]]] = []
            for member in direct_members:
                member_profile = profiles[member["qb_id"]]
                placed = False
                for member_group in member_groups:
                    if all(
                        self._anchor_member_compatible(profiles[existing["qb_id"]], member_profile)
                        for existing in member_group
                    ):
                        member_group.append(member)
                        placed = True
                        break
                if not placed:
                    member_groups.append([member])

            for member_group in member_groups:
                group_blocks = [anchor, *member_group]
                group_key = tuple(sorted(block["qb_id"] for block in group_blocks))
                if len(group_blocks) < 2 or group_key in seen_group_keys:
                    continue
                seen_group_keys.add(group_key)
                groups.append(
                    self._candidate_group(
                        group_type="superset_anchor_candidate",
                        recall_rule="direct_superset_anchor",
                        recall_reasons=["anchor_table_set_directly_covers_members"],
                        blocks=group_blocks,
                        profiles=profiles,
                        eligible_pair_lookup=eligible_pair_lookup,
                        split_reason=None,
                    )
                )
        return groups

    def _split_oversized_groups(
        self,
        groups: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        output = []
        for group in groups:
            output.extend(self._split_group(group, profiles, eligible_pair_lookup, split_fields=[
                "table_set",
                "join_signature",
                "measure_key",
                "group_by_set",
                "predicate_set",
            ]))
        return output

    def _split_group(
        self,
        group: dict[str, Any],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
        split_fields: list[str],
    ) -> list[dict[str, Any]]:
        if self._within_context_cap(group):
            return [group]

        blocks = [
            {
                "qb_id": member["qb_id"],
                "query_id": member["query_id"],
                "scope_type": member.get("scope_type"),
            }
            for member in group["members"]
        ]
        for index, field in enumerate(split_fields):
            partitions: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
            for block in blocks:
                profile = profiles[block["qb_id"]]
                value = profile[field]
                key = tuple(value) if isinstance(value, list) else (value,)
                partitions[key].append(block)
            if len(partitions) <= 1:
                continue
            split_groups = []
            for partition_blocks in partitions.values():
                split_group = self._candidate_group(
                    group_type=group["group_type"],
                    recall_rule=group["recall_rule"],
                    recall_reasons=group["recall_reasons"],
                    blocks=partition_blocks,
                    profiles=profiles,
                    eligible_pair_lookup=eligible_pair_lookup,
                    split_reason=f"oversize_{field}",
                )
                split_groups.extend(
                    self._split_group(split_group, profiles, eligible_pair_lookup, split_fields[index + 1 :])
                )
            return split_groups

        return [
            self._candidate_group(
                group_type=group["group_type"],
                recall_rule=group["recall_rule"],
                recall_reasons=group["recall_reasons"],
                blocks=chunk,
                profiles=profiles,
                eligible_pair_lookup=eligible_pair_lookup,
                split_reason="context_cap",
            )
            for chunk in self._stable_chunks(blocks)
        ]

    def _stable_chunks(self, blocks: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        chunks: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_query_ids: set[str] = set()
        for block in sorted(blocks, key=self._block_sort_key):
            would_exceed_qbs = len(current) >= self.max_qbs_per_group
            would_exceed_queries = block["query_id"] not in current_query_ids and len(current_query_ids) >= self.max_query_ids_per_group
            if current and (would_exceed_qbs or would_exceed_queries):
                chunks.append(current)
                current = []
                current_query_ids = set()
            current.append(block)
            current_query_ids.add(block["query_id"])
        if current:
            chunks.append(current)
        return chunks

    def _complete_linkage_groups(
        self,
        *,
        blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        compatible: Any,
    ) -> list[list[dict[str, Any]]]:
        groups: list[list[dict[str, Any]]] = []
        for block in sorted(blocks, key=self._block_sort_key):
            profile = profiles[block["qb_id"]]
            placed = False
            for group in groups:
                if all(compatible(profile, profiles[member["qb_id"]]) for member in group):
                    group.append(block)
                    placed = True
                    break
            if not placed:
                groups.append([block])
        return [group for group in groups if len(group) > 1]

    def _candidate_group(
        self,
        *,
        group_type: str,
        recall_rule: str,
        recall_reasons: list[str],
        blocks: list[dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
        split_reason: str | None,
    ) -> dict[str, Any]:
        ordered_blocks = sorted(blocks, key=self._block_sort_key)
        group_profiles = [profiles[block["qb_id"]] for block in ordered_blocks]
        pair_evidence = self._group_pair_evidence(ordered_blocks, eligible_pair_lookup)
        top_pair_evidence = self._top_pair_evidence(pair_evidence)
        common_tables = self._intersection_sorted(group_profiles, "table_set")
        common_join_signature = self._intersection_sorted(group_profiles, "join_signature")
        common_predicates = self._intersection_sorted(group_profiles, "predicate_set")
        union_predicates = self._union_sorted(group_profiles, "predicate_set")
        pipeline_reuse_hints = self._pipeline_reuse_hints(group_profiles)
        group = {
            "group_type": group_type,
            "recall_rule": recall_rule,
            "recall_reasons": sorted(set(recall_reasons)),
            "query_ids": self._dedupe([block["query_id"] for block in ordered_blocks]),
            "qb_ids": [block["qb_id"] for block in ordered_blocks],
            "scope_class": self._common_value(group_profiles, "scope_class"),
            "family_type": self._common_value(group_profiles, "family_type"),
            "core_fact_table_set": self._union_sorted(group_profiles, "core_fact_table_set"),
            "measure_key": self._common_value(group_profiles, "measure_key"),
            "table_set": self._union_sorted(group_profiles, "table_set"),
            "join_signature": self._union_sorted(group_profiles, "join_signature"),
            "predicate_set": union_predicates,
            "group_by_set": self._union_sorted(group_profiles, "group_by_set"),
            "measure_set": self._union_sorted(group_profiles, "measure_set"),
            "common_tables": common_tables,
            "common_join_signature": common_join_signature,
            "union_group_by": self._union_sorted(group_profiles, "group_by_set"),
            "predicate_summary": {
                "common_predicate_shapes": common_predicates,
                "residual_predicate_shapes": sorted(set(union_predicates) - set(common_predicates)),
            },
            "pipeline_reuse_hints": pipeline_reuse_hints,
            "top_pair_evidence": top_pair_evidence,
            "pair_evidence": top_pair_evidence,
            "pair_evidence_total_count": len(pair_evidence),
            "pair_evidence_truncated": len(top_pair_evidence) < len(pair_evidence),
            "members": [self._member_record(block, profiles[block["qb_id"]]) for block in ordered_blocks],
        }
        if split_reason:
            group["split_reason"] = split_reason
        return group

    def _assign_group_ids(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked_groups = sorted(
            groups,
            key=lambda group: (
                self._group_type_rank(group["group_type"]),
                group.get("split_reason") or "",
                ",".join(group["query_ids"]),
                ",".join(group["qb_ids"]),
            ),
        )
        return [
            {
                "candidate_group_id": f"batch_local_group_{index:04d}",
                **group,
            }
            for index, group in enumerate(ranked_groups, start=1)
        ]

    def _profile(self, block: dict[str, Any]) -> dict[str, Any]:
        tables = self._normalized_set(block.get("tables", []))
        core_facts = sorted(table for table in tables if table in TPCDS_FACT_TABLES)
        aggregates = self._normalized_aggregates(block.get("aggregate_exprs", []))
        measure_key = self._measure_key(block, aggregates)
        return {
            "qb_id": block["qb_id"],
            "query_id": block["query_id"],
            "scope_class": self._scope_class(block),
            "family_type": "fact_based" if core_facts else "dimension_only",
            "core_fact_table_set": core_facts,
            "measure_key": measure_key,
            "table_set": tables,
            "join_signature": self._normalized_join_edges(block.get("join_edges", [])),
            "predicate_set": self._normalized_expressions(block.get("predicates", []), ("predicate_shape", "expr")),
            "predicate_columns": self._columns_from_expressions(block.get("predicates", [])),
            "group_by_set": self._normalized_expressions(block.get("group_by_exprs", []), ("expr",)),
            "group_by_columns": self._columns_from_expressions(block.get("group_by_exprs", [])),
            "projection_columns": self._columns_from_expressions(block.get("projections", [])),
            "measure_set": aggregates,
        }

    def _pair_evidence(self, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
        left_tables = set(left["table_set"])
        right_tables = set(right["table_set"])
        left_joins = set(left["join_signature"])
        right_joins = set(right["join_signature"])
        left_predicates = set(left["predicate_set"])
        right_predicates = set(right["predicate_set"])
        left_group_by = set(left["group_by_set"])
        right_group_by = set(right["group_by_set"])
        left_measures = set(left["measure_set"])
        right_measures = set(right["measure_set"])
        left_facts = set(left["core_fact_table_set"])
        right_facts = set(right["core_fact_table_set"])
        return {
            "left_qb_id": left["qb_id"],
            "right_qb_id": right["qb_id"],
            "left_query_id": left["query_id"],
            "right_query_id": right["query_id"],
            "left_scope_class": left["scope_class"],
            "right_scope_class": right["scope_class"],
            "left_family_type": left["family_type"],
            "right_family_type": right["family_type"],
            "left_core_fact_table_set": sorted(left_facts),
            "right_core_fact_table_set": sorted(right_facts),
            "left_measure_key": left["measure_key"],
            "right_measure_key": right["measure_key"],
            "table_jaccard": self._jaccard(left_tables, right_tables),
            "table_containment": self._containment(left_tables, right_tables),
            "join_jaccard": self._jaccard(left_joins, right_joins),
            "shared_tables": sorted(left_tables & right_tables),
            "left_only_tables": sorted(left_tables - right_tables),
            "right_only_tables": sorted(right_tables - left_tables),
            "shared_fact_tables": sorted(left_facts & right_facts),
            "shared_join_edges": sorted(left_joins & right_joins),
            "shared_predicate_shapes": sorted(left_predicates & right_predicates),
            "shared_group_by_exprs": sorted(left_group_by & right_group_by),
            "shared_measure_exprs": sorted(left_measures & right_measures),
        }

    def _recall_decision(self, evidence: dict[str, Any]) -> tuple[str, list[str], str | None]:
        if evidence["left_family_type"] != evidence["right_family_type"]:
            return "rejected", [], "family_type_mismatch"

        left_facts = set(evidence["left_core_fact_table_set"])
        right_facts = set(evidence["right_core_fact_table_set"])
        if evidence["left_family_type"] == "fact_based":
            if not evidence["shared_fact_tables"]:
                return "rejected", [], "no_shared_fact_table"
            if left_facts != right_facts:
                return "rejected", [], "core_fact_table_set_mismatch"

        reasons = []
        supporting_reasons = []
        if evidence["table_jaccard"] == 1.0:
            reasons.append("exact_table_set")
        if evidence["table_jaccard"] >= self.table_jaccard_threshold:
            reasons.append("table_jaccard_threshold")
        if evidence["table_containment"] >= self.table_containment_threshold:
            reasons.append("table_containment_threshold")
        if evidence["left_family_type"] == "fact_based" and left_facts == right_facts:
            supporting_reasons.append("same_core_fact_table_set")

        if not reasons:
            return "rejected", [], "below_similarity_threshold"
        for preferred_rule in (
            "exact_table_set",
            "table_containment_threshold",
            "table_jaccard_threshold",
        ):
            if preferred_rule in reasons:
                return preferred_rule, sorted(set(reasons + supporting_reasons)), None
        return reasons[0], sorted(set(reasons + supporting_reasons)), None

    def _inventory_record(self, block: dict[str, Any], profile: dict[str, Any] | None) -> dict[str, Any]:
        record = {
            "qb_id": block.get("qb_id"),
            "query_id": block.get("query_id"),
            "scope_type": block.get("scope_type"),
            "unsupported_reasons": list(block.get("unsupported_reasons", [])),
        }
        if profile is not None:
            record.update(
                {
                    "scope_class": profile["scope_class"],
                    "family_type": profile["family_type"],
                    "core_fact_table_set": profile["core_fact_table_set"],
                    "measure_key": profile["measure_key"],
                    "table_set": profile["table_set"],
                    "join_signature": profile["join_signature"],
                    "predicate_set": profile["predicate_set"],
                    "predicate_columns": profile["predicate_columns"],
                    "group_by_set": profile["group_by_set"],
                    "group_by_columns": profile["group_by_columns"],
                    "projection_columns": profile["projection_columns"],
                    "measure_set": profile["measure_set"],
                }
            )
        return record

    def _member_record(self, block: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
        return {
            "qb_id": block["qb_id"],
            "query_id": block["query_id"],
            "scope_type": block.get("scope_type"),
            "scope_class": profile["scope_class"],
            "family_type": profile["family_type"],
            "core_fact_table_set": profile["core_fact_table_set"],
            "measure_key": profile["measure_key"],
            "tables": profile["table_set"],
            "join_signature": profile["join_signature"],
            "predicates": profile["predicate_set"],
            "predicate_columns": profile["predicate_columns"],
            "group_by_exprs": profile["group_by_set"],
            "group_by_columns": profile["group_by_columns"],
            "projection_columns": profile["projection_columns"],
            "aggregate_exprs": profile["measure_set"],
        }

    def _pipeline_reuse_hints(self, profiles: list[dict[str, Any]]) -> dict[str, Any]:
        residual_filter_columns = self._union_sorted(profiles, "predicate_columns")
        group_by_columns = self._union_sorted(profiles, "group_by_columns")
        return {
            "online_scope": "current_batch_only",
            "residual_filter_columns": residual_filter_columns,
            "residual_filter_predicate_shapes": self._union_sorted(profiles, "predicate_set"),
            "projection_columns": self._union_sorted(profiles, "projection_columns"),
            "group_by_columns": group_by_columns,
            "recommended_fine_grain_group_by_columns": sorted(set(group_by_columns) | set(residual_filter_columns)),
            "predicate_pushdown_policy": "prefer_current_batch_constant_predicates_as_residual_filters",
        }

    def _compatibility_key(self, profile: dict[str, Any]) -> tuple[str, str, tuple[str, ...], str]:
        return (
            profile["scope_class"],
            profile["family_type"],
            tuple(profile["core_fact_table_set"]),
            profile["measure_key"],
        )

    def _exact_compatible(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> bool:
        if frozenset({left["qb_id"], right["qb_id"]}) not in eligible_pair_lookup:
            return False
        return (
            left["table_set"] == right["table_set"]
            and left["join_signature"] == right["join_signature"]
            and left["measure_key"] == right["measure_key"]
        )

    def _anchor_direct_compatible(
        self,
        anchor: dict[str, Any],
        member: dict[str, Any],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> bool:
        if frozenset({anchor["qb_id"], member["qb_id"]}) not in eligible_pair_lookup:
            return False
        anchor_tables = set(anchor["table_set"])
        member_tables = set(member["table_set"])
        if not member_tables < anchor_tables:
            return False
        return set(member["join_signature"]).issubset(set(anchor["join_signature"]))

    def _anchor_member_compatible(self, left: dict[str, Any], right: dict[str, Any]) -> bool:
        left_tables = set(left["table_set"])
        right_tables = set(right["table_set"])
        return left_tables.issubset(right_tables) or right_tables.issubset(left_tables)

    def _exact_group_type(self, profiles: list[dict[str, Any]]) -> str:
        first = profiles[0]
        if first["family_type"] == "dimension_only":
            return "dimension_filter_candidate"
        if self._is_additive_measure_key(first["measure_key"]):
            return "measure_rollup_candidate"
        return "exact_shape_candidate"

    def _within_context_cap(self, group: dict[str, Any]) -> bool:
        return (
            len(group["qb_ids"]) <= self.max_qbs_per_group
            and len(group["query_ids"]) <= self.max_query_ids_per_group
        )

    def _group_pair_evidence(
        self,
        blocks: list[dict[str, Any]],
        eligible_pair_lookup: dict[frozenset[str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        evidence = []
        for left, right in combinations(blocks, 2):
            record = eligible_pair_lookup.get(frozenset({left["qb_id"], right["qb_id"]}))
            if record:
                evidence.append(record)
        return evidence

    def _top_pair_evidence(self, pair_evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(
            pair_evidence,
            key=lambda record: (
                -float(record.get("table_containment", 0.0)),
                -float(record.get("table_jaccard", 0.0)),
                -len(record.get("shared_join_edges", [])),
                str(record.get("left_qb_id", "")),
                str(record.get("right_qb_id", "")),
            ),
        )
        return ranked[: self.max_pair_evidence_sent_to_llm]

    def _pair_lookup(self, pair_evidence: list[dict[str, Any]]) -> dict[frozenset[str], dict[str, Any]]:
        return {
            frozenset({record["left_qb_id"], record["right_qb_id"]}): record
            for record in pair_evidence
        }

    def _scope_class(self, block: dict[str, Any]) -> str:
        scope_type = str(block.get("scope_type") or "").strip().lower()
        if scope_type.startswith("cte"):
            return "cte"
        if scope_type.startswith("subquery"):
            return "subquery"
        if scope_type == "outer":
            return "outer"
        return scope_type or "unknown"

    def _measure_key(self, block: dict[str, Any], aggregates: list[str]) -> str:
        if block.get("window_exprs"):
            return "window_or_complex"
        if not aggregates:
            return "detail_no_measure"
        return "|".join(aggregates)

    def _is_additive_measure_key(self, measure_key: str) -> bool:
        parts = measure_key.split("|")
        return bool(parts) and all(part.startswith("sum(") for part in parts)

    def _common_value(self, profiles: list[dict[str, Any]], key: str) -> str:
        values = {profile[key] for profile in profiles}
        if len(values) == 1:
            return next(iter(values))
        return "mixed"

    def _union_sorted(self, profiles: list[dict[str, Any]], key: str) -> list[str]:
        return sorted({value for profile in profiles for value in profile[key]})

    def _intersection_sorted(self, profiles: list[dict[str, Any]], key: str) -> list[str]:
        if not profiles:
            return []
        values = [set(profile[key]) for profile in profiles]
        return sorted(set.intersection(*values)) if values else []

    def _normalized_set(self, values: list[Any]) -> list[str]:
        return sorted({str(value).strip().lower() for value in values if value is not None and str(value).strip()})

    def _normalized_join_edges(self, values: list[Any]) -> list[str]:
        return sorted({signature for value in values if (signature := self._join_edge_signature(value))})

    def _normalized_expressions(self, values: list[Any], preferred_keys: tuple[str, ...]) -> list[str]:
        return sorted({signature for value in values if (signature := self._expression_signature(value, preferred_keys))})

    def _normalized_aggregates(self, values: list[Any]) -> list[str]:
        return sorted({signature for value in values if (signature := self._aggregate_signature(value))})

    def _columns_from_expressions(self, values: list[Any]) -> list[str]:
        columns: set[str] = set()
        for value in values:
            columns.update(self._expression_columns(value))
        return sorted(columns)

    def _join_edge_signature(self, value: Any) -> str:
        if not isinstance(value, dict):
            return str(value).strip().lower()

        left = self._column_ref(value.get("left_table"), value.get("left_column"))
        right = self._column_ref(value.get("right_table"), value.get("right_column"))
        operator = str(value.get("operator") or "=").strip().lower()
        if left and right:
            if operator == "=" and right < left:
                left, right = right, left
            return f"{left}{operator}{right}"
        return self._expression_signature(value, ("expr",))

    def _expression_signature(self, value: Any, preferred_keys: tuple[str, ...]) -> str:
        if not isinstance(value, dict):
            return str(value).strip().lower()

        for key in preferred_keys:
            raw_value = value.get(key)
            if raw_value is not None and str(raw_value).strip():
                return str(raw_value).strip().lower()

        columns = value.get("columns") or []
        if columns:
            return ",".join(self._normalized_set(columns))
        return ""

    def _expression_columns(self, value: Any) -> list[str]:
        if isinstance(value, dict):
            return self._normalized_set(value.get("columns") or [])

        raw_value = str(value or "").strip().lower()
        if self._is_simple_column_ref(raw_value):
            return [raw_value]
        return []

    def _aggregate_signature(self, value: Any) -> str:
        if not isinstance(value, dict):
            return str(value).strip().lower()

        agg_func = str(value.get("agg_func") or "").strip().lower()
        source_ref = self._column_ref(value.get("source_table"), value.get("source_column"))
        if agg_func and source_ref:
            return f"{agg_func}({source_ref})"
        expr = self._expression_signature(value, ("expr",))
        if agg_func == "count" and expr:
            return expr
        return expr

    def _column_ref(self, table: Any, column: Any) -> str:
        table_name = str(table or "").strip().lower()
        column_name = str(column or "").strip().lower()
        if not table_name or not column_name:
            return ""
        return f"{table_name}.{column_name}"

    def _is_simple_column_ref(self, value: str) -> bool:
        if value.count(".") != 1:
            return False
        table_name, column_name = value.split(".", 1)
        return table_name.replace("_", "").isalnum() and column_name.replace("_", "").isalnum()

    def _jaccard(self, left: set[str], right: set[str]) -> float:
        if not left and not right:
            return 1.0
        return round(len(left & right) / len(left | right), 4)

    def _containment(self, left: set[str], right: set[str]) -> float:
        smaller = min(len(left), len(right))
        if smaller == 0:
            return 0.0
        return round(len(left & right) / smaller, 4)

    def _dedupe(self, values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))

    def _block_sort_key(self, block: dict[str, Any]) -> tuple[str, str]:
        return (str(block.get("query_id", "")), str(block.get("qb_id", "")))

    def _group_type_rank(self, group_type: str) -> int:
        ranks = {
            "measure_rollup_candidate": 0,
            "exact_shape_candidate": 1,
            "superset_anchor_candidate": 2,
            "dimension_filter_candidate": 3,
            "single_query_candidate": 4,
        }
        return ranks.get(group_type, 99)
