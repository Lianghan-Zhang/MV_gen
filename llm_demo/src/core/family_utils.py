from __future__ import annotations

import json
from copy import deepcopy
from typing import Any


def normalize_query_families(query_families: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Ensure final QueryFamily ids are unique without silently merging different families."""
    normalized: list[dict[str, Any]] = []
    seen_signatures_by_id: dict[str, set[str]] = {}
    collision_count_by_id: dict[str, int] = {}
    used_family_ids: set[str] = set()
    events: list[dict[str, Any]] = []

    for family in query_families:
        current = deepcopy(family)
        original_id = current["family_id"]
        signature = _family_signature(current)
        seen_signatures = seen_signatures_by_id.setdefault(original_id, set())

        if original_id not in used_family_ids:
            normalized.append(current)
            used_family_ids.add(original_id)
            seen_signatures.add(signature)
            collision_count_by_id.setdefault(original_id, 1)
            continue

        if signature in seen_signatures:
            events.append(
                {
                    "action": "drop_exact_duplicate",
                    "family_id": original_id,
                    "members": current.get("members", []),
                }
            )
            continue

        suffix = collision_count_by_id.get(original_id, 1) + 1
        new_family_id = f"{original_id}__{suffix}"
        while new_family_id in used_family_ids:
            suffix += 1
            new_family_id = f"{original_id}__{suffix}"
        collision_count_by_id[original_id] = suffix

        current["family_id"] = new_family_id
        normalized.append(current)
        used_family_ids.add(new_family_id)
        seen_signatures.add(signature)
        events.append(
            {
                "action": "family_id_collision_fallback_rename",
                "original_family_id": original_id,
                "normalized_family_id": new_family_id,
                "members": current.get("members", []),
            }
        )

    return normalized, events


def _family_signature(family: dict[str, Any]) -> str:
    comparable = {key: value for key, value in family.items() if key != "family_id"}
    return json.dumps(_canonical(comparable), ensure_ascii=False, sort_keys=True)


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return sorted((_canonical(item) for item in value), key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    return value
