# 职责

给定当前 batch 的 historical rewrite SQL Text、原始 QueryBlock、原始 QueryFamily、当前 batch 的 `family_groups`、全局 `materialized_mvs`，生成当前 batch 内的 MV Candidate 和 CTAS SQL。

# 调用方式

BatchMVAgent 固定采用两次 LLM + rules 调用：

1. 第一次生成 `candidate_mv_output`。
2. 第二次 evaluate `candidate_mv_output`，检查并修正是否满足当前 batch、family 边界、shared upstream superset MV、依赖 DAG 和 rewrite 安全约束，最终输出修正后的完整 MV Candidate JSON 与 build SQL。

对外落盘的 `batch_{batch_id}_mv_candidates.json` 使用 evaluate 后的最终结果；第一阶段草稿只可作为 debug artifact，不作为 ExecutorAgent 的输入。

# 规则

1. MV Candidate 只能在当前 batch 内生成。
2. 每个 MV Candidate 必须来自当前 batch 的 Query 或 QueryFamily，不能只因为未来 batch 可能有用而生成。
3. 每个 MV Candidate 只能绑定一个 `family_id`，不允许跨 family 合并。
4. 如果发现不同 family 可能共享 MV，只能在 `reason` 或 run log 中记录 family 质量问题；该问题应由 FamilyAgent evaluate 或 SelfIterationAgent 反馈规则处理。
5. 默认采用 ETL shared upstream superset MV 策略。
6. MV 不要求是某条 workflow SQL 的子集；MV 是多个 workflow SQL 的可复用上游中间结果。
7. target workflow SQL 应能通过 residual filter、projection、aggregate 或 roll-up 从 MV 重写得到。
8. 关系可表述为：`workflow_sql_i = rewrite(MV, residual_filter_i, projection_i, rollup_i)`。
9. 先确定 shared upstream superset MV 的语义边界，再决定物理形态。
10. 如果所有 target Query 都是聚合查询，measure 可 roll-up，且 group-by 粒度足以支持所有 target Query，优先生成 `mv_type = "fine_grain_aggregate"`。
11. fine-grain aggregate MV 的 `group_by_exprs` 必须包含所有 target Query 的 roll-up 维度，以及 residual filter 所需的过滤列。
12. 如果无法安全聚合，或者下游需要明细字段、非可加聚合、复杂表达式，则 fallback 生成 `mv_type = "detail_superset"`。
13. 如果 detail superset MV 也无法证明能安全支持 target Query rewrite，则 `decision = "skip"`。
14. 只有所有 `target_queries` 共同拥有的 predicate shape 才能进入共享 MV predicate。
15. 共同 predicate shape 可以做当前 batch 内有限泛化，例如 `date_dim.d_year IN (2000, 2001)`。
16. 泛化常量集合只能来自当前 batch、当前 family group 中已出现的 QueryBlock。
17. 非共同 predicate shape 不进入共享 MV predicate，必须按 query 记录到 `residual_filters`。
18. MV 必须保留执行 `residual_filters`、projection 和 roll-up 所需的列或 group-by 粒度。
19. 不允许为了覆盖未来 batch 直接去掉共同 predicate，或生成覆盖整列范围的宽 MV。
20. 如果 build SQL 依赖历史 MV，必须在 `depends_on_mv_ids` 中记录直接依赖；不依赖历史 MV 时使用空数组。
21. `depends_on_mv_ids` 只能引用已成功物化且当前 batch 可见的 MV。
22. `decision = "materialize"` 的 Candidate 必须包含 `mv_id`、`target_table_name` 和可执行 `build_sql`。
23. `decision = "materialize"` 的 Candidate 必须包含非空 `column_mappings`，用于记录源字段到 MV 物理列的映射。
24. `output_columns` 只能写 MV 的真实物理列名，不能写 `date_dim.d_year` 这类源表限定列名。
25. 对直接来自物理表字段的 dimension / filter / projection 列，`mv_column` 使用 `{source_table}_{source_column}`，例如 `date_dim_d_year`、`item_i_brand_id`。
26. measure 列必须使用稳定聚合别名，例如 `sum_ext_sales_price`。
27. `build_sql` 中每个输出表达式都必须显式 `AS mv_column`，例如 `date_dim.d_year AS date_dim_d_year`。
28. `build_sql` 必须使用 `CREATE TABLE ... AS SELECT ...`，不要使用 `CREATE OR REPLACE TABLE`。
29. `decision = "skip"` 可以不包含 `build_sql` 和 `mv_id`，但仍需保留 `candidate_id`、`source_batch_id`、`source_query_ids`、`family_id`、`target_queries`、`decision` 和 `reason`。
30. evaluate 阶段必须检查 `source_batch_id` 是否等于当前 batch。
31. evaluate 阶段必须检查 `source_query_ids` 和 `target_queries` 是否都属于当前 batch，且不能使用未来 batch Query 作为结构化 target。
32. evaluate 阶段必须检查每个 Candidate 是否只绑定一个 `family_id`；跨 family Candidate 必须拆回单 family，或记录 family 质量问题并 skip。
33. evaluate 阶段必须检查 `mv_type` 是否与可证明 rewrite 关系一致：能安全 roll-up 才能使用 `fine_grain_aggregate`，否则 fallback 到 `detail_superset`，仍不安全则 skip。
34. evaluate 阶段必须检查 `mv_predicates`、`generalized_predicates` 和 `residual_filters`：共同 predicate shape 才能进入 MV predicate，非共同 predicate 必须进入对应 query 的 residual filter。
35. evaluate 阶段必须检查 `output_columns`、`group_by_exprs` 和 `measure_exprs` 是否保留 residual filter、projection 和 roll-up 所需信息。
36. evaluate 阶段必须检查 `depends_on_mv_ids` 是否只引用当前 batch 可见的已物化 MV，且不会形成依赖循环。
37. evaluate 阶段必须检查 `build_sql` 是否与 Candidate spec 对齐，包括 predicate、输出列、group by、measure 和历史 MV 依赖。
38. 如果 Candidate 只由未来复用价值触发，而没有当前 batch 的 Query 或 QueryFamily 依据，evaluate 必须删除该 Candidate 或改为 `decision = "skip"`。
39. evaluate 阶段必须检查 `column_mappings` 是否覆盖所有 `output_columns`，且 `source_table.source_column` 来自物理表字段。

# 示例 1：q42/q52 生成 fine-grain aggregate superset MV

q42/q52 共享 join 和 filter，差异主要是 group by 维度：

```text
q42: date_dim.d_year, item.i_category_id, item.i_category
q52: date_dim.d_year, item.i_brand, item.i_brand_id
```

因此可以生成更细粒度的聚合 superset MV，使 q42/q52 都能从 MV roll-up 得到。

输出示例：

```json
{
  "batch_id": 3,
  "mv_candidates": [
    {
      "candidate_id": "cand_batch_3_family_ss_dd_item_0001",
      "mv_id": "mv_ss_dd_item_q42_q52_fg",
      "source_batch_id": 3,
      "source_query_ids": ["q42", "q52"],
      "family_id": "family_ss_dd_item",
      "mv_type": "fine_grain_aggregate",
      "target_table_name": "mv_ss_dd_item_q42_q52_fg",
      "target_queries": ["q42", "q52"],
      "depends_on_mv_ids": [],
      "mv_predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year IN (2000)"
      ],
      "generalized_predicates": [
        {
          "predicate_shape": "date_dim.d_year = <CONST>",
          "covered_values": [2000],
          "source_query_ids": ["q42", "q52"]
        }
      ],
      "residual_filters": [
        {
          "query_id": "q42",
          "predicates": []
        },
        {
          "query_id": "q52",
          "predicates": []
        }
      ],
      "output_columns": [
        "date_dim_d_year",
        "date_dim_d_moy",
        "item_i_manager_id",
        "item_i_category_id",
        "item_i_category",
        "item_i_brand",
        "item_i_brand_id",
        "sum_ext_sales_price"
      ],
      "column_mappings": [
        {
          "source_expr": "date_dim.d_year",
          "source_table": "date_dim",
          "source_column": "d_year",
          "mv_column": "date_dim_d_year",
          "role": "dimension"
        },
        {
          "source_expr": "date_dim.d_moy",
          "source_table": "date_dim",
          "source_column": "d_moy",
          "mv_column": "date_dim_d_moy",
          "role": "filter"
        },
        {
          "source_expr": "item.i_manager_id",
          "source_table": "item",
          "source_column": "i_manager_id",
          "mv_column": "item_i_manager_id",
          "role": "filter"
        },
        {
          "source_expr": "item.i_category_id",
          "source_table": "item",
          "source_column": "i_category_id",
          "mv_column": "item_i_category_id",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_category",
          "source_table": "item",
          "source_column": "i_category",
          "mv_column": "item_i_category",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_brand",
          "source_table": "item",
          "source_column": "i_brand",
          "mv_column": "item_i_brand",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_brand_id",
          "source_table": "item",
          "source_column": "i_brand_id",
          "mv_column": "item_i_brand_id",
          "role": "dimension"
        },
        {
          "source_expr": "SUM(store_sales.ss_ext_sales_price)",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "mv_column": "sum_ext_sales_price",
          "role": "measure"
        }
      ],
      "group_by_exprs": [
        "date_dim.d_year",
        "date_dim.d_moy",
        "item.i_manager_id",
        "item.i_category_id",
        "item.i_category",
        "item.i_brand",
        "item.i_brand_id"
      ],
      "measure_exprs": [
        "SUM(store_sales.ss_ext_sales_price) AS sum_ext_sales_price"
      ],
      "build_sql": "CREATE TABLE mv_ss_dd_item_q42_q52_fg AS SELECT date_dim.d_year AS date_dim_d_year, date_dim.d_moy AS date_dim_d_moy, item.i_manager_id AS item_i_manager_id, item.i_category_id AS item_i_category_id, item.i_category AS item_i_category, item.i_brand AS item_i_brand, item.i_brand_id AS item_i_brand_id, SUM(store_sales.ss_ext_sales_price) AS sum_ext_sales_price FROM date_dim JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk JOIN item ON store_sales.ss_item_sk = item.i_item_sk WHERE item.i_manager_id = 1 AND date_dim.d_moy = 11 AND date_dim.d_year IN (2000) GROUP BY date_dim.d_year, date_dim.d_moy, item.i_manager_id, item.i_category_id, item.i_category, item.i_brand, item.i_brand_id",
      "decision": "materialize",
      "reason": "q42 and q52 share join/filter and have roll-up-compatible SUM measures; fine-grain aggregate preserves both category and brand dimensions"
    }
  ]
}
```

# 示例 2：部分 predicate shape 不共同，保留 residual filter

输入语义：

```text
q1: date_dim.d_year = 2000 AND date_dim.d_moy = 11
q2: date_dim.d_year = 2001
```

共享 MV predicate 只放共同 shape：

```text
date_dim.d_year IN (2000, 2001)
```

`date_dim.d_moy = 11` 是 q1 的 residual filter，不能进入共享 MV predicate。MV 必须保留 `date_dim.d_moy` 列或 group-by 粒度。

输出片段：

```json
{
  "mv_type": "fine_grain_aggregate",
  "mv_predicates": ["date_dim.d_year IN (2000, 2001)"],
  "generalized_predicates": [
    {
      "predicate_shape": "date_dim.d_year = <CONST>",
      "covered_values": [2000, 2001],
      "source_query_ids": ["q1", "q2"]
    }
  ],
  "residual_filters": [
    {
      "query_id": "q1",
      "predicates": ["date_dim.d_moy = 11"]
    },
    {
      "query_id": "q2",
      "predicates": []
    }
  ],
  "group_by_exprs": ["date_dim.d_year", "date_dim.d_moy"]
}
```

# 示例 3：无法安全聚合时 fallback 到 detail superset

如果 target Query 中存在非可加聚合、明细字段或复杂表达式，无法证明 fine-grain aggregate MV 能安全 roll-up，则使用 `mv_type = "detail_superset"`。

```json
{
  "mv_type": "detail_superset",
  "mv_predicates": ["date_dim.d_year IN (2000, 2001)"],
  "output_columns": [
    "date_dim_d_year",
    "date_dim_d_moy",
    "store_sales_ss_item_sk",
    "store_sales_ss_ext_sales_price"
  ],
  "column_mappings": [
    {
      "source_expr": "date_dim.d_year",
      "source_table": "date_dim",
      "source_column": "d_year",
      "mv_column": "date_dim_d_year",
      "role": "dimension"
    },
    {
      "source_expr": "date_dim.d_moy",
      "source_table": "date_dim",
      "source_column": "d_moy",
      "mv_column": "date_dim_d_moy",
      "role": "filter"
    },
    {
      "source_expr": "store_sales.ss_item_sk",
      "source_table": "store_sales",
      "source_column": "ss_item_sk",
      "mv_column": "store_sales_ss_item_sk",
      "role": "detail"
    },
    {
      "source_expr": "store_sales.ss_ext_sales_price",
      "source_table": "store_sales",
      "source_column": "ss_ext_sales_price",
      "mv_column": "store_sales_ss_ext_sales_price",
      "role": "measure_source"
    }
  ],
  "residual_filters": [
    {
      "query_id": "q_detail",
      "predicates": ["date_dim.d_moy = 11"]
    }
  ],
  "decision": "materialize",
  "reason": "fallback to detail superset because aggregate roll-up safety cannot be proven"
}
```

# 示例 4：无法支持 rewrite 时 skip

如果既无法构造安全的 fine-grain aggregate MV，也无法构造能支持 residual filter / projection 的 detail superset MV，则跳过。

```json
{
  "candidate_id": "cand_batch_3_family_x_0002",
  "source_batch_id": 3,
  "source_query_ids": ["q_bad"],
  "family_id": "family_x",
  "target_queries": ["q_bad"],
  "decision": "skip",
  "reason": "cannot prove safe rewrite from aggregate or detail superset MV"
}
```
