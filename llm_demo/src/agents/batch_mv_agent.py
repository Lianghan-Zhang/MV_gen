from __future__ import annotations

from collections import Counter
import time
from pathlib import Path
from typing import Any

from llm_demo.src.core.agent_base import LLMRulesAgent
from llm_demo.src.core.artifact_store import ArtifactStore
from llm_demo.src.core.family_candidate_builder import TPCDS_FACT_TABLES
from llm_demo.src.core.family_utils import normalize_query_families
from llm_demo.src.core.llm_client import LLMClient
from llm_demo.src.core.physical_schema import load_physical_schema, validate_physical_column
from llm_demo.src.core.schemas import BatchMVOutput
from llm_demo.src.core.sql_utils import (
    assert_create_table_as_select,
    aggregate_measure_column_name,
    create_table_select_output_names,
)


class BatchMVAgent(LLMRulesAgent):
    def __init__(self, store: ArtifactStore, llm_client: LLMClient) -> None:
        super().__init__(store=store, llm_client=llm_client, agent_name="BatchMVAgent")

    def _rules_name(self) -> str:
        return "batch_mv_agent"

    def run(
        self,
        batch_id: int,
        complexity_batches_path: str | Path,
        query_blocks_path: str | Path,
        families_path: str | Path | None,
        historical_rewrite_dir: str | Path,
        materialized_mvs_path: str | Path | None = None,
    ) -> Path:
        started_at = time.monotonic()
        batches_path = Path(complexity_batches_path)
        qb_path = Path(query_blocks_path)
        family_path = Path(families_path) if families_path else None
        rewrite_dir = Path(historical_rewrite_dir)
        mv_state_path = self._ensure_materialized_mvs(materialized_mvs_path)

        complexity_batches = self.store.read_json(batches_path)
        current_batch = self._find_batch(complexity_batches, batch_id)
        query_blocks = self.store.read_json(qb_path)
        query_to_qbs = self.store.read_json(qb_path.parent / "query_to_qbs.json")
        qb_to_query = self.store.read_json(qb_path.parent / "qb_to_query.json")
        families, family_normalization_events = self._load_query_family_hints(family_path)
        materialized_mvs = self.store.read_json(mv_state_path)
        historical_rewrites = self._load_historical_rewrites(rewrite_dir)
        current_query_blocks = self._current_query_blocks(query_blocks["query_blocks"], current_batch)
        current_query_to_qbs = self._current_query_to_qbs(query_to_qbs, current_batch)
        current_qb_to_query = self._current_qb_to_query(qb_to_query, current_query_to_qbs)
        current_query_families = self._current_query_families(
            families["query_families"],
            {block["qb_id"] for block in current_query_blocks},
        )
        batch_local_candidate_groups = self._batch_local_candidate_groups(current_query_blocks)

        input_artifacts = {
            "current_batch": current_batch,
            "historical_rewrites": historical_rewrites,
            "query_blocks": current_query_blocks,
            "query_to_qbs": current_query_to_qbs,
            "qb_to_query": current_qb_to_query,
            "query_family_hints": current_query_families,
            "batch_local_candidate_groups": batch_local_candidate_groups,
            "materialized_mvs": materialized_mvs,
            "complexity_batches": complexity_batches["complexity_batches"],
        }
        candidate_output = self._infer_structured(
            task="基于当前 batch 的 historical rewrite SQL、QueryBlock、batch-local candidate groups 和 materialized_mvs 生成 candidate_mv_output。",
            context={"run_id": self.store.run_id, "batch_id": batch_id},
            input_artifacts=input_artifacts,
            output_model=BatchMVOutput,
        )
        output = self._infer_structured(
            task=(
                "evaluate candidate_mv_output：检查当前 batch 边界、QueryBlock 边界、shared upstream superset MV、"
                "depends_on_mv_ids、build_sql 和 decision；返回修正后的完整 BatchMVOutput。"
            ),
            context={"run_id": self.store.run_id, "batch_id": batch_id},
            input_artifacts={**input_artifacts, "candidate_mv_output": candidate_output},
            output_model=BatchMVOutput,
        )
        output, scope_normalization_events = self._normalize_output_scope(
            output=output,
            current_batch=current_batch,
            batch_id=batch_id,
        )
        self._validate_output(
            output,
            current_batch,
            materialized_mvs,
            batch_id,
            query_blocks["query_blocks"],
            query_to_qbs,
        )

        mv_candidates_path = self.store.write_json(f"04_batch_mvs/batch_{batch_id}_mv_candidates.json", output)
        mv_build_path = self.store.write_text(
            f"04_batch_mvs/batch_{batch_id}_mv_build.sql",
            self._render_build_sql(output),
        )
        candidate_ids = [candidate["candidate_id"] for candidate in output["mv_candidates"]]
        self.store.append_run_log(
            agent_name=self.agent_name,
            event="success",
            input_artifact_paths=[
                path
                for path in [batches_path, qb_path, family_path, rewrite_dir, mv_state_path]
                if path is not None
            ],
            output_artifact_paths=[mv_candidates_path, mv_build_path],
            elapsed_ms=self._elapsed_ms(started_at),
            batch_id=batch_id,
            details={
                "llm_stages": ["generate_candidate_mv_output", "evaluate_mv_output"],
                "candidate_count": len(candidate_ids),
                "candidate_ids": candidate_ids,
                "family_normalization_events": family_normalization_events,
                "scope_normalization_events": scope_normalization_events,
            },
        )
        self._append_candidate_logs(output, mv_candidates_path, mv_build_path, batch_id)
        return mv_candidates_path

    def _load_query_family_hints(self, family_path: Path | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if family_path is None or not family_path.exists():
            return {"query_families": []}, []
        families = self.store.read_json(family_path)
        families["query_families"], events = normalize_query_families(families.get("query_families", []))
        return families, events

    def _ensure_materialized_mvs(self, materialized_mvs_path: str | Path | None) -> Path:
        path = Path(materialized_mvs_path) if materialized_mvs_path else self.store.path("04_batch_mvs/materialized_mvs.json")
        if not path.is_absolute():
            path = self.store.path(path)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{\n  "materialized_mvs": []\n}', encoding="utf-8")
        return path

    def _find_batch(self, complexity_batches: dict[str, Any], batch_id: int) -> dict[str, Any]:
        for batch in complexity_batches.get("complexity_batches", []):
            if batch.get("batch_id") == batch_id:
                return batch
        raise ValueError(f"Batch {batch_id} not found in complexity_batches")

    def _load_historical_rewrites(self, rewrite_dir: Path) -> list[dict[str, Any]]:
        if not rewrite_dir.is_dir():
            raise FileNotFoundError(f"Historical rewrite dir not found: {rewrite_dir}")
        rewrites = []
        for sql_path in sorted(rewrite_dir.glob("*_rewritten.sql")):
            query_id = sql_path.name.removesuffix("_rewritten.sql")
            meta_path = rewrite_dir / f"{query_id}_rewrite_meta.json"
            meta = self.store.read_json(meta_path) if meta_path.exists() else {}
            rewrites.append(
                {
                    "query_id": query_id,
                    "rewritten_sql_path": str(sql_path),
                    "rewritten_sql": sql_path.read_text(encoding="utf-8"),
                    "rewrite_meta": meta,
                }
            )
        if not rewrites:
            raise ValueError(f"No historical rewritten SQL files found in {rewrite_dir}")
        return rewrites

    def _current_query_blocks(self, query_blocks: list[dict[str, Any]], current_batch: dict[str, Any]) -> list[dict[str, Any]]:
        batch_queries = set(current_batch.get("query_ids", []))
        return [block for block in query_blocks if block.get("query_id") in batch_queries]

    def _current_query_to_qbs(self, query_to_qbs: dict[str, list[str]], current_batch: dict[str, Any]) -> dict[str, list[str]]:
        batch_queries = set(current_batch.get("query_ids", []))
        return {query_id: qb_ids for query_id, qb_ids in query_to_qbs.items() if query_id in batch_queries}

    def _current_qb_to_query(self, qb_to_query: dict[str, str], current_query_to_qbs: dict[str, list[str]]) -> dict[str, str]:
        current_qb_ids = {qb_id for qb_ids in current_query_to_qbs.values() for qb_id in qb_ids}
        return {qb_id: query_id for qb_id, query_id in qb_to_query.items() if qb_id in current_qb_ids}

    def _current_query_families(
        self,
        query_families: list[dict[str, Any]],
        current_qb_ids: set[str],
    ) -> list[dict[str, Any]]:
        current_families = []
        for family in query_families:
            members = [
                qb_id
                for qb_id in family.get("members", [])
                if qb_id in current_qb_ids
            ]
            if not members:
                continue
            current_families.append(
                {
                    **family,
                    "members": members,
                }
            )
        return current_families

    def _batch_local_candidate_groups(self, query_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for block in query_blocks:
            if block.get("unsupported_reasons"):
                continue
            tables = self._normalized_set(block.get("tables", []))
            facts = sorted(table for table in tables if table in TPCDS_FACT_TABLES)
            join_signature = self._normalized_join_edges(block.get("join_edges", []))
            group_key = "|".join(
                [
                    "fact_based" if facts else "dimension_only",
                    ",".join(facts),
                    ",".join(tables),
                    ",".join(join_signature),
                ]
            )
            group = groups.setdefault(
                group_key,
                {
                    "group_key": group_key,
                    "family_type": "fact_based" if facts else "dimension_only",
                    "core_fact_table_set": facts,
                    "table_set": tables,
                    "join_signature": join_signature,
                    "query_ids": [],
                    "qb_ids": [],
                    "predicate_set": set(),
                    "group_by_set": set(),
                    "measure_set": set(),
                },
            )
            group["query_ids"].append(block["query_id"])
            group["qb_ids"].append(block["qb_id"])
            group["predicate_set"].update(self._normalized_expressions(block.get("predicates", []), ("predicate_shape", "expr")))
            group["group_by_set"].update(self._normalized_expressions(block.get("group_by_exprs", []), ("expr",)))
            group["measure_set"].update(self._normalized_aggregates(block.get("aggregate_exprs", [])))

        output = []
        for index, group in enumerate(groups.values(), start=1):
            output.append(
                {
                    **group,
                    "candidate_group_id": f"batch_local_group_{index:04d}",
                    "query_ids": self._dedupe(group["query_ids"]),
                    "qb_ids": self._dedupe(group["qb_ids"]),
                    "predicate_set": sorted(group["predicate_set"]),
                    "group_by_set": sorted(group["group_by_set"]),
                    "measure_set": sorted(group["measure_set"]),
                }
            )
        return output

    def _normalized_set(self, values: list[Any]) -> list[str]:
        return sorted({str(value).strip().lower() for value in values if value is not None and str(value).strip()})

    def _normalized_join_edges(self, values: list[Any]) -> list[str]:
        return sorted({signature for value in values if (signature := self._join_edge_signature(value))})

    def _normalized_expressions(self, values: list[Any], preferred_keys: tuple[str, ...]) -> list[str]:
        return sorted({signature for value in values if (signature := self._expression_signature(value, preferred_keys))})

    def _normalized_aggregates(self, values: list[Any]) -> list[str]:
        return sorted({signature for value in values if (signature := self._aggregate_signature(value))})

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

    def _aggregate_signature(self, value: Any) -> str:
        if not isinstance(value, dict):
            return str(value).strip().lower()

        agg_func = str(value.get("agg_func") or "").strip().lower()
        source_ref = self._column_ref(value.get("source_table"), value.get("source_column"))
        if agg_func and source_ref:
            return f"{agg_func}({source_ref})"
        return self._expression_signature(value, ("expr",))

    def _column_ref(self, table: Any, column: Any) -> str:
        table_name = str(table or "").strip().lower()
        column_name = str(column or "").strip().lower()
        if not table_name or not column_name:
            return ""
        return f"{table_name}.{column_name}"

    def _dedupe(self, values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))

    def _normalize_output_scope(
        self,
        *,
        output: dict[str, Any],
        current_batch: dict[str, Any],
        batch_id: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        batch_queries = set(current_batch.get("query_ids", []))
        kept_candidates = []
        events = []

        for candidate in output.get("mv_candidates", []):
            reason = self._candidate_scope_rejection_reason(
                candidate=candidate,
                batch_id=batch_id,
                batch_queries=batch_queries,
            )
            if reason:
                events.append(
                    {
                        "action": "drop_candidate",
                        "candidate_id": candidate.get("candidate_id"),
                        "reason": reason,
                    }
                )
                continue
            kept_candidates.append(candidate)

        return {**output, "mv_candidates": kept_candidates}, events

    def _candidate_scope_rejection_reason(
        self,
        *,
        candidate: dict[str, Any],
        batch_id: int,
        batch_queries: set[str],
    ) -> str | None:
        if candidate.get("source_batch_id") != batch_id:
            return "source_batch_id_out_of_scope"
        if not set(candidate.get("source_query_ids", [])).issubset(batch_queries):
            return "source_query_ids_out_of_scope"
        if not set(candidate.get("target_queries", [])).issubset(batch_queries):
            return "target_queries_out_of_scope"
        return None

    def _validate_output(
        self,
        output: dict[str, Any],
        current_batch: dict[str, Any],
        materialized_mvs: dict[str, Any],
        batch_id: int,
        query_blocks: list[dict[str, Any]],
        query_to_qbs: dict[str, list[str]],
    ) -> None:
        if output["batch_id"] != batch_id:
            raise ValueError(f"BatchMVOutput batch_id {output['batch_id']} does not match {batch_id}")

        batch_queries = set(current_batch.get("query_ids", []))
        available_mv_ids = {
            mv["mv_id"]
            for mv in materialized_mvs.get("materialized_mvs", [])
            if mv.get("available_from_batch", batch_id) <= batch_id
        }
        existing_mv_ids = {
            mv["mv_id"]
            for mv in materialized_mvs.get("materialized_mvs", [])
            if mv.get("mv_id")
        }
        physical_schema = load_physical_schema(self.store.project_root)
        qb_by_id = {block["qb_id"]: block for block in query_blocks}
        unsupported_qb_ids = {
            block["qb_id"]
            for block in query_blocks
            if block.get("unsupported_reasons")
        }
        seen_candidate_ids: set[str] = set()
        for candidate in output["mv_candidates"]:
            candidate_id = candidate["candidate_id"]
            if candidate_id in seen_candidate_ids:
                raise ValueError(f"Duplicate candidate_id: {candidate_id}")
            seen_candidate_ids.add(candidate_id)

            if candidate["source_batch_id"] != batch_id:
                raise ValueError(f"Candidate {candidate_id} source_batch_id must be {batch_id}")
            if not candidate["source_query_ids"]:
                raise ValueError(f"Candidate {candidate_id} source_query_ids cannot be empty")
            if not candidate["target_queries"]:
                raise ValueError(f"Candidate {candidate_id} target_queries cannot be empty")
            if not set(candidate["source_query_ids"]).issubset(batch_queries):
                raise ValueError(f"Candidate {candidate_id} source_query_ids must belong to current batch")
            if not set(candidate["target_queries"]).issubset(batch_queries):
                raise ValueError(f"Candidate {candidate_id} target_queries must belong to current batch")
            self._validate_candidate_qbs(
                candidate=candidate,
                qb_by_id=qb_by_id,
                unsupported_qb_ids=unsupported_qb_ids,
                query_to_qbs=query_to_qbs,
                require_qb_ids=candidate["decision"] == "materialize",
            )

            mv_id = candidate.get("mv_id")
            if mv_id and mv_id in existing_mv_ids:
                raise ValueError(f"Candidate {candidate_id} mv_id already exists: {mv_id}")
            for dependency in candidate.get("depends_on_mv_ids", []):
                if dependency == mv_id:
                    raise ValueError(f"Candidate {candidate_id} cannot depend on itself")
                if dependency not in available_mv_ids:
                    raise ValueError(f"Candidate {candidate_id} depends on unavailable MV {dependency}")
            self._validate_mv_dependency_graph(candidate, materialized_mvs)

            if candidate["decision"] == "materialize":
                missing = [
                    field
                    for field in ("mv_id", "target_table_name", "build_sql")
                    if not candidate.get(field)
                ]
                if missing:
                    raise ValueError(f"Candidate {candidate_id} decision=materialize missing {missing}")
                self._validate_materialized_columns(candidate, physical_schema)

    def _validate_candidate_qbs(
        self,
        *,
        candidate: dict[str, Any],
        qb_by_id: dict[str, dict[str, Any]],
        unsupported_qb_ids: set[str],
        query_to_qbs: dict[str, list[str]],
        require_qb_ids: bool,
    ) -> None:
        candidate_id = candidate["candidate_id"]
        source_qb_ids = candidate.get("source_qb_ids", [])
        target_qb_ids = candidate.get("target_qb_ids", [])
        if require_qb_ids and (not source_qb_ids or not target_qb_ids):
            raise ValueError(f"Candidate {candidate_id} decision=materialize requires source_qb_ids and target_qb_ids")

        for field_name, qb_ids, query_ids in (
            ("source_qb_ids", source_qb_ids, candidate["source_query_ids"]),
            ("target_qb_ids", target_qb_ids, candidate["target_queries"]),
        ):
            unknown_qbs = [qb_id for qb_id in qb_ids if qb_id not in qb_by_id]
            if unknown_qbs:
                raise ValueError(f"Candidate {candidate_id} {field_name} contains unknown QueryBlock {unknown_qbs}")
            unsupported = [qb_id for qb_id in qb_ids if qb_id in unsupported_qb_ids]
            if unsupported:
                raise ValueError(f"Candidate {candidate_id} {field_name} contains unsupported QueryBlock {unsupported}")
            allowed_qbs = {
                qb_id
                for query_id in query_ids
                for qb_id in query_to_qbs.get(query_id, [])
            }
            outside_queries = [qb_id for qb_id in qb_ids if qb_id not in allowed_qbs]
            if outside_queries:
                raise ValueError(f"Candidate {candidate_id} {field_name} do not match query ids: {outside_queries}")

    def _validate_mv_dependency_graph(self, candidate: dict[str, Any], materialized_mvs: dict[str, Any]) -> None:
        mv_id = candidate.get("mv_id")
        if not mv_id:
            return
        graph = {
            mv["mv_id"]: list(mv.get("depends_on_mv_ids", []))
            for mv in materialized_mvs.get("materialized_mvs", [])
            if mv.get("mv_id")
        }
        graph[mv_id] = list(candidate.get("depends_on_mv_ids", []))
        visited: set[str] = set()
        visiting: set[str] = set()

        def visit(current_mv_id: str) -> None:
            if current_mv_id in visiting:
                raise ValueError(f"Candidate {candidate['candidate_id']} creates an MV dependency cycle")
            if current_mv_id in visited:
                return
            visiting.add(current_mv_id)
            for dependency in graph.get(current_mv_id, []):
                if dependency in graph:
                    visit(dependency)
            visiting.remove(current_mv_id)
            visited.add(current_mv_id)

        visit(mv_id)

    def _validate_materialized_columns(self, candidate: dict[str, Any], physical_schema: dict[str, set[str]]) -> None:
        candidate_id = candidate["candidate_id"]
        mappings = candidate.get("column_mappings", [])
        if not mappings:
            raise ValueError(f"Candidate {candidate_id} decision=materialize missing column_mappings")

        output_columns = candidate.get("output_columns", [])
        dotted_columns = [column for column in output_columns if "." in column]
        if dotted_columns:
            raise ValueError(f"Candidate {candidate_id} output_columns must be MV physical columns, got {dotted_columns}")

        mapped_columns = {mapping["mv_column"] for mapping in mappings}
        missing_mappings = [column for column in output_columns if column not in mapped_columns]
        if missing_mappings:
            raise ValueError(f"Candidate {candidate_id} output_columns missing column_mappings for {missing_mappings}")

        build_sql = candidate.get("build_sql") or ""
        assert_create_table_as_select(build_sql)
        build_output_columns = create_table_select_output_names(build_sql)
        non_measure_counts = Counter(
            mapping["source_column"]
            for mapping in mappings
            if mapping["role"] != "measure"
        )
        for mapping in mappings:
            validate_physical_column(physical_schema, mapping["source_table"], mapping["source_column"])
            mv_column = mapping["mv_column"]
            if mv_column.lower() not in build_output_columns:
                raise ValueError(f"Candidate {candidate_id} build_sql must output MV column {mv_column}")
            if mapping["role"] == "measure":
                expected_mv_column = aggregate_measure_column_name(mapping["source_expr"], mapping["source_column"])
                if mapping["mv_column"] != expected_mv_column:
                    raise ValueError(
                        f"Candidate {candidate_id} measure mv_column {mv_column} must be {expected_mv_column}"
                    )
            else:
                expected_mv_column = self._expected_physical_column_name(mapping, non_measure_counts)
                if mapping["mv_column"] != expected_mv_column:
                    raise ValueError(f"Candidate {candidate_id} mv_column {mv_column} must be {expected_mv_column}")

    def _expected_physical_column_name(self, mapping: dict[str, Any], column_counts: Counter[str]) -> str:
        source_column = mapping["source_column"]
        if column_counts[source_column] > 1:
            return f"{mapping['source_table']}_{source_column}"
        return source_column

    def _render_build_sql(self, output: dict[str, Any]) -> str:
        statements = []
        for candidate in output["mv_candidates"]:
            if candidate["decision"] == "materialize" and candidate.get("build_sql"):
                statements.append(f"-- {candidate['candidate_id']}\n{candidate['build_sql'].rstrip()}")
        if not statements:
            return "-- No materialized MV candidates for this batch.\n"
        return "\n\n".join(statements) + "\n"

    def _append_candidate_logs(self, output: dict[str, Any], mv_candidates_path: Path, mv_build_path: Path, batch_id: int) -> None:
        for candidate in output["mv_candidates"]:
            event = "mv_candidate_skipped" if candidate["decision"] == "skip" else "mv_candidate_generated"
            self.store.append_run_log(
                agent_name=self.agent_name,
                event=event,
                input_artifact_paths=[mv_candidates_path],
                output_artifact_paths=[mv_build_path],
                elapsed_ms=0,
                batch_id=batch_id,
                candidate_id=candidate["candidate_id"],
                details={
                    "decision": candidate["decision"],
                    "mv_id": candidate.get("mv_id"),
                    "source_query_ids": candidate["source_query_ids"],
                    "source_qb_ids": candidate.get("source_qb_ids", []),
                    "target_queries": candidate["target_queries"],
                    "target_qb_ids": candidate.get("target_qb_ids", []),
                    "reason": candidate["reason"],
                },
            )
