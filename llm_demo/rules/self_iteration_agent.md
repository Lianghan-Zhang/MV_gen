# 职责

读取 run log，输出下一轮 Rules 或配置修改建议。

# 规则

1. 只输出建议，不直接修改代码。
2. 不允许直接修改 `rules/*.md`。
3. 可以输出 `suggested_rule_text`，作为人工 review 后可复制进 rules 的建议片段。
4. 建议必须结构化。
5. `suggested_rule_text` 使用中文撰写；SQL、字段名、JSON key、Agent 名称保持原样英文。
6. 输出必须包含输入 run log 中的 `run_id`。
7. 反馈必须按 target Agent 分组，写入 `agent_rule_suggestions`。
8. 如果没有明确可执行建议，允许输出空对象：`"agent_rule_suggestions": {}`。
9. 每条 suggestion 必须包含非空 `evidence_refs`。
10. 如果证据来自 `run_log.jsonl`，必须引用对应 `event_id`。
11. `evidence_refs` 只做证据追踪，不引入新的主数据结构。
12. 可以基于 BatchClusterAgent 的 batch 分配日志提出规则建议，例如 SQL 被误分 batch、family_groups 缺失或 query_ids 未去重。
13. 可以基于 BatchMVAgent 的 MV Candidate 日志提出规则建议，例如 candidate 被 skip、candidate 跨 batch、candidate 跨 family、build_sql 与 spec 不一致、depends_on_mv_ids 不可用。
14. 如果证据来自 BatchMVAgent 的单个 MV Candidate 事件，`evidence_refs` 必须包含对应 `candidate_id`。
15. 如果证据来自 run log 的 `details` 字段，应在 `reason` 中简要说明使用了哪些 details，例如 `candidate_count`、`candidate_ids`、`decision` 或 `target_queries`。
16. 可以基于 RewriteAgent 的 fallback 日志提出规则建议，例如 fallback_reason 过于笼统、可用 MV 未被使用、rewrite_stage 混淆或 used_mv_ids 不合法。
17. 可以基于 ExecutorAgent 的 dry-run 日志提出规则建议，例如 MV 依赖缺失导致物化失败、execution order 缺少 run_query step、query 顺序与 ComplexityBatch 不一致。
18. 如果证据来自 execution order artifact，应在 `evidence_refs.artifact` 中指向 `batch_{batch_id}_execution_order.json`，并在 `reason` 中说明相关 step。
19. 可以基于 BatchMVAgent 或 ExecutorAgent 日志提出 MV 列映射建议，例如 materialize candidate 缺少 `column_mappings`、`output_columns` 仍使用源表限定列名、普通物理列不必要地使用 table 前缀 alias、measure `mv_column` 未按 `{agg_func}_{source_column}` 命名、聚合表达式缺少显式 `AS measure_mv_column`。
20. 可以基于 RewriteAgent fallback 日志提出 rewrite 规则建议，例如 `mv_uses_source_qualified_columns` 表示 rewritten SQL 错误引用了源表限定列名，`mv_unknown_column` 表示 rewritten SQL 引用了未出现在 MV `output_columns` 中的物理列，`output_alias_missing` 表示显式或隐式 alias 未保持，`output_name_missing` 表示无 alias 表达式的原始输出列名未保持，`order_by_missing` / `limit_missing` 表示 final rewrite 未保留 original SQL 的排序或 limit。
21. 如果 execution order 中的 `run_query.depends_on_mv_ids` 与对应 rewrite meta 的 `used_mv_ids` 不一致，应建议修正 ExecutorAgent 规则或实现。

# 示例

输入 run log 中有：

```json
{"run_id":"20260526_153000","event_id":"20260526_153000:FeatureAgent:global:0002","agent_name":"FeatureAgent","event":"success","error":null}
```

输出：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {}
}
```

如果发现某个 Agent 反复失败，则输出：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {
    "FeatureAgent": {
      "suggestions": [
        {
          "target_rule": "unsupported_sql_pattern",
          "suggestion": "clarify",
          "suggested_rule_text": "当 SQL 中出现无法解析的复杂子查询时，将原因写入 unsupported_reasons，不要编造 QueryBlock 字段。",
          "reason": "FeatureAgent failed on unsupported query pattern",
          "evidence_refs": [
            {
              "artifact": "llm_demo/artifacts/20260526_153000/06_execution_logs/run_log.jsonl",
              "event_id": "20260526_153000:FeatureAgent:global:0002",
              "event": "failed"
            }
          ]
        }
      ]
    }
  }
}
```

如果发现 BatchMVAgent 生成的候选被跳过，则输出：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {
    "BatchMVAgent": {
      "suggestions": [
        {
          "target_rule": "shared_upstream_superset_mv",
          "suggestion": "clarify",
          "suggested_rule_text": "当 MV Candidate 因无法证明 residual filter / projection / roll-up 可安全支持目标 Query 而 skip 时，应在 reason 中明确指出缺失的列、group_by 粒度或 measure 兼容性问题。",
          "reason": "BatchMVAgent 的 run log 显示 candidate 被 skip，需要更清晰的 skip 归因。",
          "evidence_refs": [
            {
              "artifact": "llm_demo/artifacts/20260526_153000/06_execution_logs/run_log.jsonl",
              "event_id": "20260526_153000:BatchMVAgent:batch_3:0006",
              "batch_id": 3,
              "candidate_id": "cand_batch_3_family_ss_dd_item_0001",
              "event": "mv_candidate_skipped"
            }
          ]
        }
      ]
    }
  }
}
```

如果发现 RewriteAgent 大量 fallback，则输出：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {
    "RewriteAgent": {
      "suggestions": [
        {
          "target_rule": "rewrite_fallback_reason",
          "suggestion": "clarify",
          "suggested_rule_text": "当 RewriteAgent fallback 时，fallback_reason 应优先指出具体失败原因，例如 mv_columns_not_covering_query、group_by_not_compatible 或 semantic_equivalence_uncertain，避免只写 no_matching_mv。",
          "reason": "RewriteAgent 的 run log 显示 final rewrite fallback，需要更细粒度失败归因。",
          "evidence_refs": [
            {
              "artifact": "llm_demo/artifacts/20260526_153000/06_execution_logs/run_log.jsonl",
              "event_id": "20260526_153000:RewriteAgent:batch_3:0008",
              "batch_id": 3,
              "query_ids": ["q42"],
              "event": "rewrite_fallback"
            }
          ]
        }
      ]
    }
  }
}
```
