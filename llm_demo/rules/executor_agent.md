# 职责

ExecutorAgent 第一版只做 dry-run，不连接 Spark，不真实执行 SQL。它负责把可物化的 MV Candidate 追加到 `materialized_mvs.json`，并输出当前 batch 的 MV 物化和 rewritten SQL 运行顺序。

# 规则

1. 当前固定 `mode = "dry_run"`。
2. 不执行 Spark SQL，不连接任何外部计算引擎。
3. `materialized_mvs.json` 只保存已经 dry-run 物化成功且可供 RewriteAgent 使用的 MV。
4. `decision = "materialize"` 且依赖满足的 MV Candidate 视为 dry-run 物化成功。
5. `decision = "skip"` 的 MV Candidate 不写入 `materialized_mvs.json`。
6. 如果 `depends_on_mv_ids` 中有任何 MV 不存在于当前 `materialized_mvs.json`，该 Candidate 物化失败，不写入 `materialized_mvs.json`。
7. 成功写入 `materialized_mvs.json` 时，必须保留 MV Candidate 中的 `mv_predicates`、`generalized_predicates` 和 `residual_filters`，这些字段用于 RewriteAgent 判断查询过滤是否已被 MV predicate implied。
8. 单个 MV Candidate 失败不阻断当前 batch 的后续步骤。
9. 每个 MV Candidate 都必须写 run log，事件为 `mv_materialize_success`、`mv_materialize_failed` 或 `mv_candidate_skipped`。
10. `run_queries(...)` 按 `ComplexityBatch.query_ids` 顺序读取 final rewritten SQL，不重新排序。
11. `run_queries(...)` 只输出运行顺序，不真实运行 SQL。
12. 运行顺序统一写入 `06_execution_logs/batch_{batch_id}_execution_order.json`。
13. execution order 中必须包含 `materialize_mv` 和 `run_query` 两类 step。
14. `materialize_mv` step 记录 `candidate_id`、`mv_id`、`status`、`sql_path`、`depends_on_mv_ids` 和 `reason`。
15. `run_query` step 记录 `query_id`、`status = "planned"`、`sql_path`、`meta_path` 和 `depends_on_mv_ids`。
16. `run_query.depends_on_mv_ids` 必须直接来自对应 `{query_id}_rewrite_meta.json` 的 `used_mv_ids`。
17. 如果 rewrite meta 缺少 `used_mv_ids`，不能推断依赖，必须直接失败。

# 示例

```json
{
  "run_id": "20260527_153000",
  "batch_id": 3,
  "mode": "dry_run",
  "steps": [
    {
      "step_order": 1,
      "step_type": "materialize_mv",
      "status": "success",
      "candidate_id": "cand_batch_3_family_ss_dd_item_0001",
      "mv_id": "mv_ss_dd_item_q42_q52_fg",
      "sql_path": "04_batch_mvs/batch_3_mv_build.sql",
      "depends_on_mv_ids": [],
      "reason": "dry-run materialized"
    },
    {
      "step_order": 2,
      "step_type": "run_query",
      "status": "planned",
      "query_id": "q42",
      "sql_path": "05_rewritten_sql/batch_3/final_rewrite/q42_rewritten.sql",
      "meta_path": "05_rewritten_sql/batch_3/final_rewrite/q42_rewrite_meta.json",
      "reason": "dry-run query execution order",
      "depends_on_mv_ids": ["mv_ss_dd_item_q42_q52_fg"]
    }
  ]
}
```
