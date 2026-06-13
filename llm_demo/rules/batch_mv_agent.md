# 职责

给定当前 batch 的 historical rewrite SQL Text、当前 batch 的原始 QueryBlock、batch-local candidate groups、可选 QueryFamily hint、全局 `materialized_mvs`，生成当前 batch 内的 MV Candidate 和 CTAS SQL。

QueryFamily 现在只是辅助 evidence，不再是 batch 内 MV Candidate 的硬边界。MV Candidate 的硬边界是当前 batch 的 `query_ids`、当前 batch 内存在且 supported 的 `source_qb_ids` / `target_qb_ids`，以及可验证的 CTAS / column mapping。

# 调用方式

BatchMVAgent 固定采用两次 LLM + rules 调用：

1. 第一次生成 `candidate_mv_output`。
2. 第二次 evaluate `candidate_mv_output`，检查并修正是否满足当前 batch、QueryBlock 边界、shared upstream superset MV、依赖 DAG 和 rewrite 安全约束，最终输出修正后的完整 MV Candidate JSON 与 build SQL。

对外落盘的 `batch_{batch_id}_mv_candidates.json` 使用 evaluate 后的最终结果；第一阶段草稿只可作为 debug artifact，不作为 ExecutorAgent 的输入。

# 规则

1. MV Candidate 只能在当前 batch 内生成。
2. 每个 MV Candidate 必须来自当前 batch 的 QueryBlock，不能只因为未来 batch 可能有用而生成。
3. `batch_local_candidate_groups` 是 batch 内共享计算边界的主要候选证据；可以使用其中的 `candidate_group_id` 记录候选来源。
4. `query_family_hints` 只能作为命名、解释、优先级或去重参考；没有 QueryFamily 的 QueryBlock 仍然可以生成 MV Candidate。
5. 默认采用 ETL shared upstream superset MV 策略。
6. MV 不要求是某条 workflow SQL 的子集；MV 是多个 workflow SQL 的可复用上游中间结果。
7. target workflow SQL 应能通过 residual filter、projection、aggregate 或 roll-up 从 MV 重写得到。
8. 关系可表述为：`workflow_sql_i = rewrite(MV, residual_filter_i, projection_i, rollup_i)`。
9. 先确定 shared upstream superset MV 的语义边界，再决定物理形态。
10. 如果所有 target Query 都是聚合查询，measure 可 roll-up，且 group-by 粒度足以支持所有 target Query，优先生成 `mv_type = "fine_grain_aggregate"`。
11. fine-grain aggregate MV 的 `group_by_exprs` 必须包含所有 target Query 的 roll-up 维度，以及 residual filter 所需的过滤列。
12. 如果无法安全聚合，或者下游需要明细字段、非可加聚合、复杂表达式，则 fallback 生成 `mv_type = "detail_superset"`。
13. 如果 MV 只覆盖维表过滤、维表投影或 CTE 维度子结构，且不包含事实表 measure，可使用 `mv_type = "dimension_only"`。
14. 如果 detail superset MV 也无法证明能安全支持 target Query rewrite，则 `decision = "skip"`。
15. 只有所有 `target_queries` 共同拥有的 predicate shape 才能进入共享 MV predicate。
16. 共同 predicate shape 可以做当前 batch 内有限泛化，例如 `date_dim.d_year IN (2000, 2001)`。
17. 泛化常量集合只能来自当前 batch、当前 candidate 涉及的 QueryBlock。
18. 非共同 predicate shape 不进入共享 MV predicate，必须按 query 记录到 `residual_filters`。
19. MV 必须保留执行 `residual_filters`、projection 和 roll-up 所需的列或 group-by 粒度。
20. 不允许为了覆盖未来 batch 直接去掉共同 predicate，或生成覆盖整列范围的宽 MV。
21. 如果 build SQL 依赖历史 MV，必须在 `depends_on_mv_ids` 中记录直接依赖；不依赖历史 MV 时使用空数组。
22. `depends_on_mv_ids` 只能引用已成功物化且当前 batch 可见的 MV。
23. `decision = "materialize"` 的 Candidate 必须包含 `mv_id`、`target_table_name` 和可执行 `build_sql`。
24. `decision = "materialize"` 的 Candidate 必须包含非空 `source_qb_ids` 和 `target_qb_ids`；这些 QueryBlock 必须属于当前 batch，且不能包含 `unsupported_reasons`。
25. `decision = "materialize"` 的 Candidate 必须包含非空 `column_mappings`，用于记录源字段到 MV 物理列的映射。
26. `output_columns` 只能写 MV 的真实物理列名，不能写 `date_dim.d_year` 这类源表限定列名。
27. 对直接来自物理表字段的 dimension / filter / projection 列，默认不写 `AS`，`mv_column` 直接使用物理源字段名，例如 `date_dim.d_year -> d_year`、`item.i_brand_id -> i_brand_id`。
28. measure 列必须使用稳定聚合别名，命名规则严格等于 `{agg_func_lower}_{source_column_lower}`，例如 `SUM(store_sales.ss_ext_sales_price)` 对应 `sum_ss_ext_sales_price`，`SUM(store_sales.ss_sales_price)` 对应 `sum_ss_sales_price`；不要丢掉源字段自身的业务前缀。
29. measure 的 `build_sql` SELECT alias、`output_columns[]`、`column_mappings[].mv_column` 必须完全一致；禁止使用原查询输出别名或语义别名作为 MV 物理列名，例如 `sum_sun_sales`、`ext_price`、`sales`。
30. 如果 measure 的 `source_expr` 无法表达为一个聚合函数作用于单一物理源字段，当前阶段必须 `decision = "skip"`，`reason` 使用 `measure_source_lineage_not_supported` 或 `derived_expression_column_mapping_not_supported`；不要为复杂 measure 编造稳定别名。
31. 只有聚合表达式和普通列同名冲突时才写 `AS mv_column`；无冲突普通列保持 `table.column` 形式，字段来源由 `column_mappings` 记录。
32. 不允许用 SQL 局部 alias 派生 `mv_column`，例如 `d1_quarter_name`、`d2_quarter_name`、`cd1_marital_status`；`mv_column` 必须来自物理列命名规则和 `column_mappings`，不能泄漏 SQL alias。
33. 如果同一物理表在同一个 Candidate 中以多个业务角色参与 join，例如 `date_dim d1/d2/d3` 或 `customer_demographics cd1/cd2`，而当前 `column_mappings` 无法稳定表达这些角色，evaluate 阶段必须把该 Candidate 改成 `decision = "skip"`，`reason` 使用 `duplicate_physical_table_role_not_supported`；不要用 alias 列名 materialize，也不要把语义不同的角色强行折叠到同一个物理列名。
34. `build_sql` 必须使用 `CREATE TABLE ... AS SELECT ...`，不要使用 `CREATE OR REPLACE TABLE`。
35. `decision = "skip"` 可以不包含 `build_sql`、`mv_id`、`family_id` 和 `candidate_group_id`，但仍需保留 `candidate_id`、`source_batch_id`、`source_query_ids`、`target_queries`、`decision` 和 `reason`。
36. evaluate 阶段必须检查 `source_batch_id` 是否等于当前 batch。
37. evaluate 阶段必须检查 `source_query_ids` 和 `target_queries` 是否都属于当前 batch，且不能使用未来 batch Query 作为结构化 target。
38. evaluate 阶段不得因为缺少 `family_id`、`query_family_hints` 或 `family_groups` 删除 Candidate；只要 Candidate 的 QueryBlock 边界和语义 rewrite 关系可验证，就可以保留。
39. evaluate 阶段必须检查 `mv_type` 是否与可证明 rewrite 关系一致：能安全 roll-up 才能使用 `fine_grain_aggregate`，否则 fallback 到 `detail_superset`，仍不安全则 skip。
40. evaluate 阶段必须检查 `mv_predicates`、`generalized_predicates` 和 `residual_filters`：共同 predicate shape 才能进入 MV predicate，非共同 predicate 必须进入对应 query 的 residual filter。
41. evaluate 阶段必须检查 `output_columns`、`group_by_exprs` 和 `measure_exprs` 是否保留 residual filter、projection 和 roll-up 所需信息。
42. evaluate 阶段必须检查 `depends_on_mv_ids` 是否只引用当前 batch 可见的已物化 MV，且不会形成依赖循环。
43. evaluate 阶段必须检查 `build_sql` 是否与 Candidate spec 对齐，包括 predicate、输出列、group by、measure 和历史 MV 依赖。
44. 如果 Candidate 只由未来复用价值触发，而没有当前 batch 的 QueryBlock 依据，evaluate 必须删除该 Candidate 或改为 `decision = "skip"`。
45. evaluate 阶段必须检查 `column_mappings` 是否覆盖所有 `output_columns`，且 `source_table.source_column` 来自物理表字段。
46. 第一次生成 `candidate_mv_output` 时也必须严格满足 BatchMVOutput schema；不要把不完整 Candidate 留给第二次 evaluate 修正。
47. `column_mappings[]` 中的 `source_expr`、`source_table`、`source_column`、`mv_column`、`role` 都必须是非空字符串，禁止使用 `null`、空字符串或占位符表示未知来源。
48. `column_mappings` 只能记录可追溯到单一物理表字段的输出列；如果输出列来自 ratio、CASE、多列组合表达式、常量表达式、复杂派生表达式，且当前 schema 无法稳定记录其源字段 lineage，不要为该列生成半残缺 mapping。
49. 如果一个 `decision = "materialize"` 的 Candidate 需要无法映射到单一 `source_table.source_column` 的输出列，第一次生成阶段就必须直接输出 `decision = "skip"`，`reason` 使用 `column_mapping_source_not_supported` 或 `derived_expression_column_mapping_not_supported`；同时使用空 `column_mappings`、空 `output_columns`、`build_sql = null`、`mv_id = null`、`target_table_name = null`。
50. `column_mappings[].source_column`、`column_mappings[].source_expr`、`output_columns[]` 禁止使用 `*`、`table.*`、`alias.*`；`build_sql` 的 SELECT 列表也禁止使用 `SELECT *` 或 `SELECT table.*`。
51. `detail_superset` 也必须显式列出 rewrite 所需的物理字段和对应 `column_mappings`，不能用通配符代表整行或整张表。
52. 如果 Candidate 需要整行、整表或 wildcard lineage 才能表达，当前阶段必须直接输出 `decision = "skip"`，`reason` 使用 `wildcard_column_mapping_not_supported` 或 `detail_superset_wildcard_not_supported`；不要自动展开整表字段，也不要 materialize 宽表候选。
53. `family_id` 是可选 hint 字段。如果使用，优先复制 `query_family_hints[].family_id`；如果没有合适 hint，可以省略 `family_id`，并使用 `candidate_group_id` 追踪 batch-local group。
54. 如果 `current_batch.family_groups` 为空，仍然可以基于当前 batch 的 supported QueryBlock 和 `batch_local_candidate_groups` 生成 MV Candidate。
55. `materialized_mvs`、`complexity_batches`、QueryFamily hint 和历史 rewrite 只能作为可见性、依赖和上下文参考，不能作为跨 batch target 的依据。

# 示例 1：q42/q52 生成 fine-grain aggregate superset MV

q42/q52 共享 join 和 filter，差异主要是 group by 维度：

```text
q42: date_dim.d_year, item.i_category_id, item.i_category
q52: date_dim.d_year, item.i_brand, item.i_brand_id
```

因此可以生成更细粒度的聚合 superset MV，使 q42/q52 都能从 MV roll-up 得到。

对应 `build_sql` 应保持普通物理列不加 alias，只给聚合表达式加稳定 alias：

```sql
CREATE TABLE mv_ss_dd_item_q42_q52_fg AS
SELECT
  date_dim.d_year,
  item.i_category_id,
  item.i_category,
  item.i_brand,
  item.i_brand_id,
  SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price
FROM date_dim
JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND date_dim.d_moy = 11
  AND date_dim.d_year IN (2000)
GROUP BY
  date_dim.d_year,
  item.i_category_id,
  item.i_category,
  item.i_brand,
  item.i_brand_id
```

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
      "source_qb_ids": ["q42.outer", "q52.outer"],
      "family_id": "family_ss_dd_item",
      "mv_type": "fine_grain_aggregate",
      "target_table_name": "mv_ss_dd_item_q42_q52_fg",
      "target_queries": ["q42", "q52"],
      "target_qb_ids": ["q42.outer", "q52.outer"],
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
        "d_year",
        "i_category_id",
        "i_category",
        "i_brand",
        "i_brand_id",
        "sum_ss_ext_sales_price"
      ],
      "column_mappings": [
        {
          "source_expr": "date_dim.d_year",
          "source_table": "date_dim",
          "source_column": "d_year",
          "mv_column": "d_year",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_category_id",
          "source_table": "item",
          "source_column": "i_category_id",
          "mv_column": "i_category_id",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_category",
          "source_table": "item",
          "source_column": "i_category",
          "mv_column": "i_category",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_brand",
          "source_table": "item",
          "source_column": "i_brand",
          "mv_column": "i_brand",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_brand_id",
          "source_table": "item",
          "source_column": "i_brand_id",
          "mv_column": "i_brand_id",
          "role": "dimension"
        },
        {
          "source_expr": "SUM(store_sales.ss_ext_sales_price)",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "mv_column": "sum_ss_ext_sales_price",
          "role": "measure"
        }
      ],
      "group_by_exprs": [
        "date_dim.d_year",
        "item.i_category_id",
        "item.i_category",
        "item.i_brand",
        "item.i_brand_id"
      ],
      "measure_exprs": [
        "SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price"
      ],
      "build_sql": "CREATE TABLE mv_ss_dd_item_q42_q52_fg AS SELECT date_dim.d_year, item.i_category_id, item.i_category, item.i_brand, item.i_brand_id, SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price FROM date_dim JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk JOIN item ON store_sales.ss_item_sk = item.i_item_sk WHERE item.i_manager_id = 1 AND date_dim.d_moy = 11 AND date_dim.d_year IN (2000) GROUP BY date_dim.d_year, item.i_category_id, item.i_category, item.i_brand, item.i_brand_id",
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
    "d_year",
    "d_moy",
    "ss_item_sk",
    "ss_ext_sales_price"
  ],
  "column_mappings": [
    {
      "source_expr": "date_dim.d_year",
      "source_table": "date_dim",
      "source_column": "d_year",
      "mv_column": "d_year",
      "role": "dimension"
    },
    {
      "source_expr": "date_dim.d_moy",
      "source_table": "date_dim",
      "source_column": "d_moy",
      "mv_column": "d_moy",
      "role": "filter"
    },
    {
      "source_expr": "store_sales.ss_item_sk",
      "source_table": "store_sales",
      "source_column": "ss_item_sk",
      "mv_column": "ss_item_sk",
      "role": "detail"
    },
    {
      "source_expr": "store_sales.ss_ext_sales_price",
      "source_table": "store_sales",
      "source_column": "ss_ext_sales_price",
      "mv_column": "ss_ext_sales_price",
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
