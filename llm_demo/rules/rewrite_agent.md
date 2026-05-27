# 职责

给定当前 batch 的 original SQL、QueryBlock、可用 `materialized_mvs.json` 和 rewrite stage，生成每条 SQL 的 rewritten SQL 文件内容与 rewrite meta。

# 规则

1. RewriteAgent 永远不覆盖 original SQL。
2. 每个 rewrite stage、每条输入 SQL 都必须生成 `{query_id}_rewritten.sql`。
3. 每个 rewrite stage、每条输入 SQL 都必须生成 `{query_id}_rewrite_meta.json`。
4. 即使没有安全可用的 MV，也必须生成 original-equivalent 的 rewritten SQL，后续阶段统一读取 rewritten SQL。
5. historical rewrite 和 final rewrite 都以当前 batch 的 original SQL 为输入语义锚点。
6. historical rewrite 只能使用进入当前 batch 前已经存在的 Materialized View State。
7. final rewrite 使用当前 batch 物化成功后更新的 Materialized View State。
8. 只能使用 `materialized_mvs.json` 中已成功物化且当前 batch 可见的 MV。
9. 成功 rewrite 必须能说明 original SQL 可由 MV 经过 residual filter、projection、aggregate 或 roll-up 得到。
10. 如果无法证明语义等价，必须 fallback 到 original-equivalent SQL。
11. fallback 时仍输出 `{query_id}_rewritten.sql`，内容可以与 original SQL 等价或第一版直接复制 original SQL。
12. fallback 时 `status = "fallback"`，`used_mv_ids = []`，`fallback_reason` 必须非空。
13. 成功使用 MV 时 `status = "rewritten"`，`used_mv_ids` 必须非空，`fallback_reason = null`。
14. 每条 rewrite meta 必须包含 `query_id`、`rewrite_stage`、`original_sql_path`、`rewritten_sql_path`、`used_mv_ids`、`status`、`rewrite_mode`、`semantic_check` 和 `fallback_reason`。
15. 每次 rewrite 必须写 run log，记录输入 artifact、输出 rewritten SQL、输出 rewrite meta、rewrite 状态、使用 MV 和 fallback 原因。
16. 常见 fallback reason 包括：`no_available_historical_mv`、`no_matching_mv`、`mv_columns_not_covering_query`、`filter_not_implied_by_mv`、`group_by_not_compatible`、`aggregate_not_supported`、`unsupported_sql_pattern`、`semantic_equivalence_uncertain`。
17. 第一版支持 SUM / COUNT / AVG / MIN / MAX 的保守 rewrite。
18. 对复杂 window、rollup、count distinct、stddev、相关子查询默认 fallback。

# 输出路径约定

historical rewrite：

```text
{run_id}/05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewritten.sql
{run_id}/05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewrite_meta.json
```

final rewrite：

```text
{run_id}/05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewritten.sql
{run_id}/05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewrite_meta.json
```

# 示例 1：成功使用 fine-grain aggregate MV rewrite q42

```json
{
  "rewrites": [
    {
      "query_id": "q42",
      "rewrite_stage": "final",
      "status": "rewritten",
      "used_mv_ids": ["mv_ss_dd_item_q42_q52_fg"],
      "original_sql_path": "tpcds-spark/q42.sql",
      "rewritten_sql_path": "05_rewritten_sql/batch_3/final_rewrite/q42_rewritten.sql",
      "rewrite_meta_path": "05_rewritten_sql/batch_3/final_rewrite/q42_rewrite_meta.json",
      "rewrite_mode": "mv_filter_projection_rollup",
      "rewritten_sql": "SELECT date_dim.d_year, item.i_category_id, item.i_category, SUM(sum_ext_sales_price) AS sum_ext_sales_price FROM mv_ss_dd_item_q42_q52_fg GROUP BY date_dim.d_year, item.i_category_id, item.i_category",
      "residual_filters": [],
      "rollup_exprs": [
        "GROUP BY date_dim.d_year, item.i_category_id, item.i_category"
      ],
      "semantic_check": {
        "status": "pass",
        "reason": "q42 can be derived from mv_ss_dd_item_q42_q52_fg by projection and SUM roll-up"
      },
      "fallback_reason": null
    }
  ]
}
```

# 示例 2：fallback 也生成 rewritten SQL

没有安全可用 MV 时，仍生成 `q99_rewritten.sql`，内容与 original SQL 等价。

```json
{
  "rewrites": [
    {
      "query_id": "q99",
      "rewrite_stage": "final",
      "status": "fallback",
      "used_mv_ids": [],
      "original_sql_path": "tpcds-spark/q99.sql",
      "rewritten_sql_path": "05_rewritten_sql/batch_3/final_rewrite/q99_rewritten.sql",
      "rewrite_meta_path": "05_rewritten_sql/batch_3/final_rewrite/q99_rewrite_meta.json",
      "rewrite_mode": "original_equivalent",
      "rewritten_sql": "SELECT ... FROM original_tables ...",
      "residual_filters": [],
      "rollup_exprs": [],
      "semantic_check": {
        "status": "fallback",
        "reason": "no materialized MV can safely cover this query"
      },
      "fallback_reason": "no_matching_mv"
    }
  ]
}
```

# 示例 3：run log 事件要点

每条 rewrite 需要在 run log 中留下可审计记录。字段名称可由实现层统一，但语义必须包含：

```json
{
  "agent_name": "RewriteAgent",
  "event": "rewrite_fallback",
  "query_id": "q99",
  "batch_id": 3,
  "rewrite_stage": "final",
  "used_mv_ids": [],
  "rewritten_sql_path": "05_rewritten_sql/batch_3/final_rewrite/q99_rewritten.sql",
  "rewrite_meta_path": "05_rewritten_sql/batch_3/final_rewrite/q99_rewrite_meta.json",
  "fallback_reason": "no_matching_mv"
}
```
