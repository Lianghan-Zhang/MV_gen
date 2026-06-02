from __future__ import annotations

from itertools import combinations
import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.artifact_store import ArtifactStore


TPCDS_FACT_TABLES = {
    "store_sales",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
    "inventory",
}


class FamilyCandidateBuilder:
    def __init__(self, store: ArtifactStore) -> None:
        self.store = store

    def run(self, query_blocks_path: str | Path) -> Path:
        started_at = time.monotonic()
        qb_path = Path(query_blocks_path)
        query_blocks = self.store.read_json(qb_path).get("query_blocks", [])
        grouped = self._group_query_blocks(query_blocks)
        candidates = [
            self._candidate(candidate_id=index + 1, blocks=blocks, unsupported_qb_ids=unsupported_qb_ids)
            for index, (blocks, unsupported_qb_ids) in enumerate(grouped.values())
            if blocks
        ]
        candidates_path = self.store.write_json(
            "02_families/family_candidates.json",
            {"family_candidates": candidates},
        )
        self.store.append_run_log(
            agent_name="FamilyCandidateBuilder",
            event="success",
            input_artifact_paths=[qb_path],
            output_artifact_paths=[candidates_path],
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            details={"candidate_count": len(candidates)},
        )
        return candidates_path

    def _group_query_blocks(self, query_blocks: list[dict[str, Any]]) -> dict[str, tuple[list[dict[str, Any]], list[str]]]:
        grouped: dict[str, tuple[list[dict[str, Any]], list[str]]] = {}
        for block in query_blocks:
            key = self._group_key(block)
            blocks, unsupported_qb_ids = grouped.setdefault(key, ([], []))
            if block.get("unsupported_reasons"):
                unsupported_qb_ids.append(block["qb_id"])
            else:
                blocks.append(block)
        return grouped

    def _group_key(self, block: dict[str, Any]) -> str:
        tables = self._normalized_set(block.get("tables", []))
        facts = sorted(table for table in tables if table in TPCDS_FACT_TABLES)
        family_type = "fact_based" if facts else "dimension_only"
        join_signature = self._normalized_set(block.get("join_edges", []))
        return "|".join(
            [
                family_type,
                ",".join(facts),
                ",".join(tables),
                ",".join(join_signature),
            ]
        )

    def _candidate(self, candidate_id: int, blocks: list[dict[str, Any]], unsupported_qb_ids: list[str]) -> dict[str, Any]:
        table_sets = [set(self._normalized_set(block.get("tables", []))) for block in blocks]
        predicate_sets = [set(self._normalized_set(block.get("predicates", []))) for block in blocks]
        group_by_sets = [set(self._normalized_set(block.get("group_by_exprs", []))) for block in blocks]
        measure_sets = [set(self._normalized_set(block.get("aggregate_exprs", []))) for block in blocks]
        join_sets = [set(self._normalized_set(block.get("join_edges", []))) for block in blocks]
        all_tables = sorted(set().union(*table_sets)) if table_sets else []
        core_facts = sorted(table for table in all_tables if table in TPCDS_FACT_TABLES)
        family_type = "fact_based" if core_facts else "dimension_only"
        return {
            "candidate_family_id": f"candidate_family_{candidate_id:04d}",
            "family_type": family_type,
            "candidate_key": self._candidate_key(family_type, core_facts, all_tables, join_sets),
            "core_fact_table_set": core_facts,
            "table_set": all_tables,
            "join_signature": sorted(set().union(*join_sets)) if join_sets else [],
            "predicate_set": sorted(set().union(*predicate_sets)) if predicate_sets else [],
            "group_by_set": sorted(set().union(*group_by_sets)) if group_by_sets else [],
            "measure_set": sorted(set().union(*measure_sets)) if measure_sets else [],
            "members": [
                {
                    "qb_id": block["qb_id"],
                    "query_id": block["query_id"],
                    "tables": self._normalized_set(block.get("tables", [])),
                    "predicates": self._normalized_set(block.get("predicates", [])),
                    "group_by_exprs": self._normalized_set(block.get("group_by_exprs", [])),
                    "aggregate_exprs": self._normalized_set(block.get("aggregate_exprs", [])),
                }
                for block in blocks
            ],
            "pair_evidence": self._pair_evidence(blocks),
            "unsupported_qb_ids": unsupported_qb_ids,
        }

    def _pair_evidence(self, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        evidence = []
        for left, right in list(combinations(blocks, 2))[:20]:
            left_tables = set(self._normalized_set(left.get("tables", [])))
            right_tables = set(self._normalized_set(right.get("tables", [])))
            left_predicates = set(self._normalized_set(left.get("predicates", [])))
            right_predicates = set(self._normalized_set(right.get("predicates", [])))
            left_measures = set(self._normalized_set(left.get("aggregate_exprs", [])))
            right_measures = set(self._normalized_set(right.get("aggregate_exprs", [])))
            evidence.append(
                {
                    "left_qb_id": left["qb_id"],
                    "right_qb_id": right["qb_id"],
                    "table_jaccard": self._jaccard(left_tables, right_tables),
                    "table_containment": self._containment(left_tables, right_tables),
                    "shared_tables": sorted(left_tables & right_tables),
                    "shared_predicates": sorted(left_predicates & right_predicates),
                    "shared_measures": sorted(left_measures & right_measures),
                    "reason": "same grouped table/join signature; LLM must evaluate predicate and measure compatibility",
                }
            )
        return evidence

    def _candidate_key(self, family_type: str, core_facts: list[str], all_tables: list[str], join_sets: list[set[str]]) -> str:
        join_signature = sorted(set().union(*join_sets)) if join_sets else []
        return "|".join([family_type, ",".join(core_facts), ",".join(all_tables), ",".join(join_signature)])

    def _normalized_set(self, values: list[Any]) -> list[str]:
        return sorted({str(value).strip().lower() for value in values if str(value).strip()})

    def _jaccard(self, left: set[str], right: set[str]) -> float:
        if not left and not right:
            return 1.0
        return round(len(left & right) / len(left | right), 4)

    def _containment(self, left: set[str], right: set[str]) -> float:
        smaller = min(len(left), len(right))
        if smaller == 0:
            return 0.0
        return round(len(left & right) / smaller, 4)
