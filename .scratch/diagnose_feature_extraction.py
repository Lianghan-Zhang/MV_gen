from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import sqlglot
from sqlglot import exp


ROOT = Path("/Users/zlhmac/code/Python_project/MV_gen")
SQL_DIR = ROOT / "tpcds-spark"
RUN_DIR = ROOT / "llm_demo/artifacts/20260607_163234"
ART = RUN_DIR / "01_query_blocks"
OUT_CSV = ART / "feature_extraction_diagnosis.csv"
OUT_MD = ART / "feature_extraction_diagnosis.md"

PHYSICAL_TABLES = {
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
}

ISSUE_LABELS = {
    "missing_manifest": "SQL 未出现在 sql_manifest.json 中",
    "missing_status": "SQL 未出现在 feature_extract_status.json 中",
    "missing_query_blocks": "未生成 QueryBlock",
    "query_to_qbs_mismatch": "query_to_qbs 索引与实际 QueryBlock 不一致",
    "qb_to_query_mismatch": "qb_to_query 反向索引不一致",
    "missing_physical_table": "SQL 中的物理表没有出现在 artifact 的 tables 中",
    "cte_table_in_artifact": "artifact 的 tables 中出现 CTE 名称而非物理表名",
    "derived_or_unknown_table_in_artifact": "artifact 的 tables 中出现派生表或未知表名",
    "alias_prefix_leak": "表达式中仍保留 CTE/派生表/SQL alias 前缀",
    "missing_cte_query_block": "SQL 包含 CTE，但没有对应 cte QueryBlock",
    "missing_set_branch_query_block": "SQL 包含集合操作，但没有 set_branch QueryBlock",
    "missing_subquery_signal": "SQL 包含子查询，但没有 subquery QueryBlock 或 has_subquery 标记",
    "missing_aggregate_features": "SQL 包含聚合或 GROUP BY，但 artifact 未提取聚合特征",
    "missing_window_flag": "SQL 包含窗口函数，但没有 has_window 标记",
    "wrong_complexity_join_groupby": "QueryBlock 同时包含 join 与 aggregate/group by，但 complexity_type 不是 join_filter_groupby",
    "wrong_q42_q52_family_key": "q42/q52 的 family_key 不符合固定约定",
}

SEVERITY_LABELS = {
    "HIGH": "高风险",
    "MED": "中风险",
    "LOW": "低风险",
    "OK": "未发现问题",
}


def exprs_for(qb: dict) -> list[str]:
    values: list[str] = []
    for key in ("join_edges", "predicates", "group_by_exprs", "aggregate_exprs"):
        values.extend(str(item) for item in qb.get(key, []) or [])
    values.append(str(qb.get("family_key", "")))
    return values


def parse_info(sql_text: str) -> dict:
    info = {
        "parse_ok": True,
        "parse_error": "",
        "physical_tables": set(),
        "cte_names": set(),
        "derived_aliases": set(),
        "alias_to_table": {},
        "table_alias_counts": Counter(),
        "has_cte": False,
        "has_set": False,
        "has_window": False,
        "has_group": False,
        "has_agg": False,
        "has_subquery": False,
        "has_join_node": False,
        "has_where_or_having": False,
    }
    try:
        trees = [tree for tree in sqlglot.parse(sql_text, read="spark") if tree is not None]
    except Exception as exc:
        info["parse_ok"] = False
        info["parse_error"] = f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
        return info

    for tree in trees:
        ctes = list(tree.find_all(exp.CTE))
        for cte in ctes:
            if cte.alias:
                info["cte_names"].add(cte.alias.lower())
        info["has_cte"] = info["has_cte"] or bool(ctes)
        info["has_set"] = info["has_set"] or any(
            tree.find_all((exp.Union, exp.Intersect, exp.Except))
        )
        info["has_window"] = info["has_window"] or any(tree.find_all(exp.Window))
        info["has_group"] = info["has_group"] or any(
            node.args.get("group") for node in tree.find_all(exp.Select)
        )
        info["has_agg"] = info["has_agg"] or any(tree.find_all(exp.AggFunc))
        info["has_subquery"] = info["has_subquery"] or any(tree.find_all(exp.Subquery))
        info["has_join_node"] = info["has_join_node"] or any(tree.find_all(exp.Join))
        info["has_where_or_having"] = info["has_where_or_having"] or any(
            node.args.get("where") or node.args.get("having")
            for node in tree.find_all(exp.Select)
        )

        for subq in tree.find_all(exp.Subquery):
            if subq.alias:
                info["derived_aliases"].add(subq.alias.lower())
        for table in tree.find_all(exp.Table):
            name = (table.name or "").lower()
            if not name:
                continue
            alias = (table.alias or "").lower()
            if name in info["cte_names"]:
                if alias and alias != name:
                    info["derived_aliases"].add(alias)
                continue
            info["physical_tables"].add(name)
            if alias and alias != name:
                info["alias_to_table"][alias] = name
            info["table_alias_counts"][name] += 1
    return info


def query_sort_key(query_id: str) -> tuple[int, str]:
    digits = "".join(ch for ch in query_id if ch.isdigit())
    return (int(digits or 0), query_id)


def render_issue_cn(issue: str) -> str:
    severity, code, detail = issue.split(":", 2)
    label = ISSUE_LABELS.get(code, code)
    return f"{SEVERITY_LABELS.get(severity, severity)}：{label}（{detail}）"


def render_issue_codes_cn(issue_codes: str) -> str:
    if not issue_codes:
        return "-"
    return "；".join(ISSUE_LABELS.get(code, code) for code in issue_codes.split(","))


def main() -> None:
    query_blocks = json.loads((ART / "query_blocks.json").read_text())["query_blocks"]
    query_to_qbs = json.loads((ART / "query_to_qbs.json").read_text())
    qb_to_query = json.loads((ART / "qb_to_query.json").read_text())
    status_doc = json.loads((ART / "feature_extract_status.json").read_text())
    status_by_q = {query["query_id"]: query for query in status_doc["queries"]}
    manifest_doc = json.loads((RUN_DIR / "00_raw_sql/sql_manifest.json").read_text())
    manifest_by_q = {query["query_id"]: query for query in manifest_doc["queries"]}

    qbs_by_query: dict[str, list[dict]] = defaultdict(list)
    for qb in query_blocks:
        qbs_by_query[qb["query_id"]].append(qb)

    sql_paths = {path.stem: path for path in SQL_DIR.glob("*.sql")}
    rows = []
    issue_counter = Counter()
    high_examples = []

    for query_id in sorted(sql_paths, key=query_sort_key):
        sql_text = sql_paths[query_id].read_text()
        info = parse_info(sql_text)
        qbs = qbs_by_query.get(query_id, [])
        status = status_by_q.get(query_id, {}).get("status", "missing_status")
        query_unsupported = []
        for qb in qbs:
            query_unsupported.extend(qb.get("unsupported_reasons") or [])

        issues = []
        severities = []

        def add(severity: str, code: str, detail: str) -> None:
            issues.append(f"{severity}:{code}:{detail}")
            severities.append(severity)
            issue_counter[code] += 1

        if query_id not in manifest_by_q:
            add("HIGH", "missing_manifest", "SQL not present in sql_manifest.json")
        if query_id not in status_by_q:
            add("HIGH", "missing_status", "SQL not present in feature_extract_status.json")
        if not qbs:
            add("HIGH", "missing_query_blocks", "No QueryBlock emitted for SQL")

        expected_qbs = set(query_to_qbs.get(query_id, []))
        actual_qbs = {qb["qb_id"] for qb in qbs}
        if expected_qbs != actual_qbs:
            add(
                "HIGH",
                "query_to_qbs_mismatch",
                f"query_to_qbs={sorted(expected_qbs)} actual={sorted(actual_qbs)}",
            )
        for qb_id in actual_qbs:
            if qb_to_query.get(qb_id) != query_id:
                add("HIGH", "qb_to_query_mismatch", f"{qb_id} maps to {qb_to_query.get(qb_id)}")

        artifact_tables = {
            str(table).lower() for qb in qbs for table in (qb.get("tables") or [])
        }
        artifact_physical = artifact_tables & PHYSICAL_TABLES
        if info["parse_ok"]:
            missing_physical = sorted(info["physical_tables"] - artifact_physical)
            if missing_physical:
                add("HIGH", "missing_physical_table", ",".join(missing_physical))
            for table in sorted(t for t in artifact_tables if t and t not in PHYSICAL_TABLES):
                severity = "MED" if query_unsupported else "HIGH"
                code = (
                    "cte_table_in_artifact"
                    if table in info["cte_names"]
                    else "derived_or_unknown_table_in_artifact"
                )
                add(severity, code, table)

        alias_names = set(info["alias_to_table"]) | info["derived_aliases"] | info["cte_names"]
        for qb in qbs:
            qb_unsupported = bool(qb.get("unsupported_reasons"))
            for alias in sorted(alias_names):
                alias_pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(alias)}\.", re.I)
                hits = [value for value in exprs_for(qb) if alias_pattern.search(value)]
                if hits:
                    severity = "MED" if qb_unsupported or query_unsupported else "HIGH"
                    add(
                        severity,
                        "alias_prefix_leak",
                        f"{qb['qb_id']} uses {alias}. in {hits[0][:90]}",
                    )
                    break

        scopes = {qb.get("scope_type") for qb in qbs}
        flags = {flag for qb in qbs for flag in qb.get("structural_flags", [])}
        if info["has_cte"] and "cte" not in scopes:
            add("HIGH", "missing_cte_query_block", ",".join(sorted(info["cte_names"])))
        if info["has_set"] and "set_branch" not in scopes:
            add("MED", "missing_set_branch_query_block", "SQL contains set operation")
        if info["has_subquery"] and "subquery" not in scopes and "has_subquery" not in flags:
            add("MED", "missing_subquery_signal", "SQL contains subquery")
        if (info["has_group"] or info["has_agg"]) and not any(
            qb.get("group_by_exprs") or qb.get("aggregate_exprs") for qb in qbs
        ):
            add("HIGH", "missing_aggregate_features", "SQL has aggregate/group")
        if info["has_window"] and "has_window" not in flags:
            add("MED", "missing_window_flag", "SQL contains window")

        for qb in qbs:
            physical_table_count = len(
                [table for table in qb.get("tables", []) if str(table).lower() in PHYSICAL_TABLES]
            )
            has_join = bool(qb.get("join_edges")) or physical_table_count > 1
            has_groupish = bool(qb.get("group_by_exprs") or qb.get("aggregate_exprs"))
            if has_join and has_groupish and qb.get("complexity_type") != "join_filter_groupby":
                add(
                    "HIGH",
                    "wrong_complexity_join_groupby",
                    f"{qb['qb_id']} complexity_type={qb.get('complexity_type')}",
                )
        if query_id in {"q42", "q52"}:
            for qb in qbs:
                if (
                    qb["qb_id"] == f"{query_id}.outer"
                    and qb.get("family_key") != "store_sales-date_dim-item"
                ):
                    add(
                        "HIGH",
                        "wrong_q42_q52_family_key",
                        f"{qb['qb_id']} family_key={qb.get('family_key')}",
                    )

        severity_rank = {"HIGH": 3, "MED": 2, "LOW": 1}
        max_severity = max(severities, key=lambda sev: severity_rank[sev]) if severities else "OK"
        row = {
            "query_id": query_id,
            "status": status,
            "qb_count": len(qbs),
            "sql_parse_ok": info["parse_ok"],
            "physical_tables_in_sql": ",".join(sorted(info["physical_tables"])),
            "artifact_physical_tables": ",".join(sorted(artifact_physical)),
            "cte_names": ",".join(sorted(info["cte_names"])),
            "derived_aliases": ",".join(sorted(info["derived_aliases"])),
            "max_severity": max_severity,
            "issue_count": len(issues),
            "issue_codes": ",".join(sorted({issue.split(":", 2)[1] for issue in issues})),
            "issues": " | ".join(issues),
        }
        rows.append(row)
        if max_severity == "HIGH" and len(high_examples) < 30:
            high_examples.append(row)

    fieldnames = [
        "query_id",
        "status",
        "qb_count",
        "sql_parse_ok",
        "physical_tables_in_sql",
        "artifact_physical_tables",
        "cte_names",
        "derived_aliases",
        "max_severity",
        "issue_count",
        "issue_codes",
        "issues",
    ]
    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary = Counter(row["max_severity"] for row in rows)
    status_counts = Counter(row["status"] for row in rows)
    lines = [
        "# FeatureAgent 特征提取诊断报告",
        "",
        "被诊断 artifact：`llm_demo/artifacts/20260607_163234/01_query_blocks`",
        "SQL 来源：`tpcds-spark/*.sql`",
        "",
        "## 检查范围",
        "",
        f"- 已检查 SQL 文件数：{len(sql_paths)}",
        f"- 已检查 QueryBlock 条目数：{len(query_blocks)}",
        f"- QueryBlock 覆盖的 query 数：{len(qbs_by_query)}",
        f"- FeatureAgent 状态分布：{dict(status_counts)}",
        f"- 诊断严重度分布：{dict(summary)}",
        "",
        "## 诊断方法",
        "",
        "- 使用 `sqlglot` 的 Spark dialect 解析每条 SQL。",
        "- 将 SQL 中解析出的物理表、CTE 名称、派生表别名、集合操作、子查询、窗口函数、聚合信号与 FeatureAgent artifact 对比。",
        "- 按 `llm_demo/rules/feature_agent.md` 的本地规则检查：QueryBlock 覆盖、物理表名规范化、表达式中是否残留 alias 前缀、join+group/aggregate 的复杂度标记，以及 q42/q52 的 family_key 约定。",
        "- 已有 `unsupported_reasons` 的行仍列为诊断项，但 alias/CTE 归一化类问题按中风险处理，避免把已声明不支持的边界当作静默错误。",
        "",
        "## 问题类型统计",
        "",
    ]
    for code, count in issue_counter.most_common():
        lines.append(f"- `{code}`：{count} 处。{ISSUE_LABELS.get(code, code)}")
    lines.extend(
        [
            "",
            "## 代表性高风险记录",
            "",
            "| query_id | 状态 | QueryBlock 数 | 问题 |",
            "|---|---:|---:|---|",
        ]
    )
    for row in high_examples:
        issue_text = "；".join(render_issue_cn(issue) for issue in row["issues"].split(" | "))
        issue_text = issue_text.replace("|", "\\|")[:500]
        lines.append(
            f"| {row['query_id']} | {row['status']} | {row['qb_count']} | {issue_text} |"
        )
    lines.extend(
        [
            "",
            "## 逐 SQL 摘要",
            "",
            "| query_id | 状态 | QueryBlock 数 | 严重度 | 问题摘要 |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row['query_id']} | {row['status']} | {row['qb_count']} | "
            f"{SEVERITY_LABELS.get(row['max_severity'], row['max_severity'])} | "
            f"{render_issue_codes_cn(row['issue_codes'])} |"
        )
    lines.extend(["", "逐 SQL 的完整可筛选明细见 `feature_extraction_diagnosis.csv`。"])
    OUT_MD.write_text("\n".join(lines) + "\n")

    print(f"wrote {OUT_CSV}")
    print(f"wrote {OUT_MD}")
    print(f"severity {dict(summary)}")
    print(f"top issues {issue_counter.most_common(20)}")


if __name__ == "__main__":
    main()
