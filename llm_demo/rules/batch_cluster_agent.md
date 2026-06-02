# 职责

给定 QueryBlock、query_to_qbs、qb_to_query 和 QueryFamily，以完整 SQL/query_id 为单位生成全局 ComplexityBatch，并在每个 batch 内保留 family_groups。

# 规则

1. batch 分配单位是 SQL/query_id，不是 qb_id。
2. QueryBlock/qb_id 只用于判断复杂度、family 归属和后续 MV 生成。
3. 一个 SQL 的最终 batch 由代码层 canonical rules 根据可用 QueryBlock 决定；LLM 输出只作为候选和 family_groups 组织参考。
4. 一个 SQL 的 canonical batch 由可用 QueryBlock 中最高结构复杂度决定；带 `unsupported_reasons` 的 QueryBlock 不参与判定。
5. batch 映射固定为：`join -> batch_id 1`、`join_filter -> batch_id 2`、`join_filter_groupby -> batch_id 3`、`other -> batch_id 4`。
6. `other` 是兜底 bucket：只有没有可用 QueryBlock 或可用 QueryBlock 无法归入 join / join_filter / join_filter_groupby 时才进入 `other`。
7. `join_filter_groupby` 表示 join 上的聚合复杂层；只要可用 QueryBlock 有 join 且有 `group_by_exprs` 或 `aggregate_exprs`，即使没有 filter，也应归入 batch 3。
8. 输出必须包含四个 global batch，即使某个 batch 的 `query_ids` 为空。
9. 顶层 `query_ids` 表示该 global batch 中要处理的完整 SQL，必须去重。
10. `family_groups` 只是 batch 内的 family 组织信息，不代表 SQL 会被拆分执行。
11. 先在 QueryFamily 内收集该 family 的成员 QueryBlock，再把其所属 SQL 放入对应 global batch 的 `family_groups`。
12. 如果一个 SQL 的多个 QueryBlock 属于不同 family，该 query_id 可以出现在多个 `family_groups` 中，但顶层 `query_ids` 只能出现一次。
13. 每个 `family_groups[].qb_ids` 只能包含该 family 的可用 QueryBlock。
14. 每个 `family_groups[].query_ids` 必须由该 group 的 `qb_ids` 对应的 query_id 去重得到。
15. 第一次调用先生成 `candidate_complexity_batches`。
16. 第二次调用 evaluate `candidate_complexity_batches`，检查 SQL 是否进入唯一 global batch、顶层 query_ids 是否去重、family_groups 是否遗漏或误分 QueryBlock。
17. evaluate 阶段输出仍然是完整 BatchClusterOutput，不输出单独评审报告。
18. 如果 LLM 输出的顶层 batch 与代码层 canonical rules 不一致，实现层会按 canonical rules 修正，并在 run log 中记录 `corrected_query_ids`。

# 示例

输入 QueryBlock：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "complexity_type": "join_filter_groupby",
      "family_key": "store_sales-date_dim-item"
    },
    {
      "qb_id": "q52.outer",
      "query_id": "q52",
      "complexity_type": "join_filter_groupby",
      "family_key": "store_sales-date_dim-item"
    }
  ],
  "query_to_qbs": {
    "q42": ["q42.outer"],
    "q52": ["q52.outer"]
  },
  "qb_to_query": {
    "q42.outer": "q42",
    "q52.outer": "q52"
  },
  "query_families": [
    {
      "family_id": "family_ss_dd_item",
      "members": ["q42.outer", "q52.outer"]
    }
  ]
}
```

输出：

```json
{
  "complexity_batches": [
    {
      "batch_id": 1,
      "batch_type": "join",
      "query_ids": [],
      "family_groups": []
    },
    {
      "batch_id": 2,
      "batch_type": "join_filter",
      "query_ids": [],
      "family_groups": []
    },
    {
      "batch_id": 3,
      "batch_type": "join_filter_groupby",
      "query_ids": ["q42", "q52"],
      "family_groups": [
        {
          "family_id": "family_ss_dd_item",
          "query_ids": ["q42", "q52"],
          "qb_ids": ["q42.outer", "q52.outer"]
        }
      ]
    },
    {
      "batch_id": 4,
      "batch_type": "other",
      "query_ids": [],
      "family_groups": []
    }
  ]
}
```
