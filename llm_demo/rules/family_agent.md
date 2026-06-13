# 职责

给定 `family_candidates.json` 和 alias-free QueryBlock 证据，按 upstream join-domain MV / shared superset MV 覆盖关系聚合 QueryFamily。

FamilyAgent 不从 FeatureAgent 读取 `family_key`、`complexity_type` 或 batch 标签。FeatureAgent 只输出 SQL 事实；FamilyCandidateBuilder 负责用代码从这些事实生成候选和量化证据；FamilyAgent 负责最终合并、拆分、命名和 evaluate。

# 输入边界

1. 聚合单位是 `qb_id`，不是完整 SQL。
2. 输入的 `family_candidates` 是 code-only 预筛结果，只负责压缩候选空间；最终 family 仍由 FamilyAgent evaluate / merge / split 决定。
3. 不允许把 `candidate_family_id` 原样当作最终结论；必须检查是否需要合并、拆分、删除重复或修正成员。
4. QueryBlock 输入是 alias-free SQL 事实结构：
   - 不包含 `complexity_type`、`block_shape`、`family_key`。
   - 不包含 SQL table alias、relation alias 或 SELECT 输出 alias。
   - `tables` 是物理表去重集合。
   - `relation_instances` 使用物理实例 ID，例如 `date_dim`、`date_dim__sold_date`、`date_dim__1`。
   - `join_edges`、`predicates`、`group_by_exprs`、`aggregate_exprs` 是结构化对象。
5. FamilyAgent 可以读取 QueryBlock 的结构化字段作为证据，但最终判断应优先使用 `family_candidates` 中已归一化的 `table_set`、`core_fact_table_set`、`join_signature`、`predicate_set`、`group_by_set`、`measure_set` 和 `pair_evidence`。
6. 带 `unsupported_reasons` 的 QueryBlock 默认不进入 QueryFamily；它可以作为 candidate 被拒绝的证据，但不能被当作可安全共享 MV 的成员。

# 输出契约

1. 输出必须严格符合 `FamilyOutput` schema。
2. `members` 只保存属于该 family 的 `qb_id`。
3. `family_id` 必须稳定、可读且全局唯一。它不只按表集合命名，而是按“可共享 MV 的语义边界”命名，格式为 `family_{family_type}_{domain}_{distinguishing_feature}`。
4. `family_key` 是 FamilyAgent 派生出的最终 family summary key，不是 FeatureAgent 输入字段；它应能概括 join domain、关键 predicate / measure / derived semantics。
5. `common_tables` 记录 family 成员共同使用的物理表名。
6. `common_join_skeleton` 记录 family 成员共同拥有或可证明等价的 join 边，必须使用物理表名或稳定 `relation_instance_id`，不能使用 SQL alias。
7. `common_predicates` 只记录 family 内所有成员完全相同的完整谓词，例如所有成员都有 `item.i_manager_id = 1`。
8. `predicate_shapes` 记录同构过滤列但常量值可能不同的谓词形状，例如 `date_dim.d_year = <CONST>`。
9. `union_group_by_exprs` 记录 family 内所有成员 group by 表达式的并集。
10. `union_measure_exprs` 记录 family 内所有成员 aggregate 表达式的并集。

# family_id / family_key 命名规则

1. `family_id` 的 `family_type` 使用 `fact`、`dim`、`cte`、`mixed`。
2. fact-based family 使用：

```text
family_fact_{fact_table}_{main_dims}_{predicate_or_measure_feature}
```

例如 q42/q52 可写成：

```text
family_fact_store_sales_date_dim_item_manager_month_year
```

3. 如果使用缩写，缩写必须稳定。第一版允许的常用缩写如下：

```text
store_sales -> ss
catalog_sales -> cs
web_sales -> ws
store_returns -> sr
catalog_returns -> cr
web_returns -> wr
inventory -> inv
date_dim -> dd
time_dim -> td
customer_address -> ca
item -> item
customer -> customer
```

因此 q42/q52 也可写成：

```text
family_fact_ss_dd_item_manager_moy_year
```

4. `family_key` 可以比 `family_id` 更短，但必须由当前 family 事实派生，例如 `store_sales-date_dim-item|manager_moy_year|sum_ss_ext_sales_price`。不要从 QueryBlock 输入复制 `family_key`，因为 Feature artifact 不再包含该字段。
5. dimension-only family 不能只写表名，必须加入关键过滤或派生语义。例如：

```text
family_dim_customer_address_city_mismatch
family_dim_customer_dn_count_range
```

不要输出：

```text
family_customer_customer_address_dn
```

6. CTE / derived QueryBlock 应体现派生含义。如果 `block_name` 只是 `cte_1` 或 `subquery_1`，不要把它当核心语义，应根据来源表、predicate、measure 或 derived logic 命名。
7. 同一表集合但 predicate shape 不同，必须进入命名区分。例如：

```text
family_fact_ss_dd_item_year_manager
family_fact_ss_dd_item_category
```

8. 同一表集合但 measure 不同，也必须进入命名区分。例如：

```text
family_fact_ss_dd_item_sum_sales_price
family_fact_ss_dd_item_sum_net_paid
family_fact_ss_dd_item_count
```

9. q42/q52 这种 measure 相同但 group by 不同的 QueryBlock 仍应进入同一 family，因为可以通过更细粒度 MV roll-up 覆盖；不需要把 category / brand 分别写入两个 family_id。
10. 完全重复的 family 应去重；同名但成员、join 或 predicate 信息不同的 family 必须在 evaluate 阶段生成语义可区分的不同 `family_id`。

# 判定规则

1. `QueryFamily` 不是简单 SQL 相似簇，而是一组可由同一个 upstream join-domain MV 或 shared superset MV 覆盖的 QueryBlock。
2. 第一版优先保守合并；如果无法证明两个 QueryBlock 可被同一个 upstream MV 安全覆盖，保持分离。
3. family candidate 第一层使用表集合量化：`Jaccard(A,B)=|T(A)∩T(B)|/|T(A)∪T(B)|` 衡量相似度，`Containment(A,B)=|T(A)∩T(B)|/min(|T(A)|,|T(B)|)` 识别宽表覆盖关系。
4. core fact table 是 QueryBlock join domain 中承载事实记录或主要 measure 来源的中心表；TPC-DS 第一版可优先识别 `store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。
5. core fact table 相同是 family 合并 hard gate；如果 core fact table 不同，即使 Jaccard / Containment 较高，也默认保持分离。
6. 多 fact table QueryBlock 只允许与具有完全相同 `core_fact_table_set`、且 join graph 可证明不会引入行数膨胀的 QueryBlock 合并；否则单独成 family。
7. 单 fact QueryBlock 和多 fact QueryBlock 不混合进同一个 family；例如 `{store_sales}` 不能和 `{store_sales, store_returns}` 合并。
8. dimension-only QueryBlock 不混入 fact-based family；如果后续 fact query 使用该维表 MV，应通过 RewriteAgent 的局部 rewrite 或后续 MV 依赖表达，而不是 family 合并表达。
9. Jaccard 和 Containment 只负责生成 candidate，不直接决定最终合并。
10. family 最终合并必须通过安全门：core fact table、join graph、predicate shape、measure compatibility 和 roll-up safety 必须可证明兼容。
11. join skeleton 必须相同或可证明等价。等值 join 可用 `join_signature` 或结构化 `join_edges` 判断；非等值、OR 条件、复杂函数 join 默认不直接合并。
12. predicate shape 兼容只允许两类情况：
    - 同一过滤列常量不同，例如 `date_dim.d_year = 2000` 与 `date_dim.d_year = 2001`。
    - 一方比另一方多出过滤条件，且多出的过滤列可在 MV 中保留并作为下游 residual filter。
13. 如果两个 QueryBlock 的过滤列集合完全不同，例如一个只过滤 `date_dim.d_year`，另一个只过滤 `item.i_manager_id`，即使 core fact table、join graph 和 measure 兼容，也必须拆成不同 family。
14. measure set 第一版按严格等价判断兼容：aggregate function 和 source column 必须相同；`COUNT(*)` 只和 `COUNT(*)` 兼容；`COUNT(DISTINCT x)` 暂不与其他 measure 兼容。
15. group-by set 第一版按 roll-up compatible 判断兼容：不要求完全相同；如果 measure、core fact table 和 join graph 兼容，且所有 group-by expr 都能作为 MV 维度列保留，则允许使用 `union_group_by_exprs` 支持后续 roll-up。
16. 表达式型 group-by、函数型 group-by 或无法稳定保留的派生列，不能仅凭 group-by 兼容放行，必须进入 LLM evaluate 或 skip。
17. 如果过滤列集合差异过大、predicate 语义不清或 family 是否可合并无法证明，保持分离，并在后续 SelfIterationAgent 的反馈中记录规则改进建议，而不是在 BatchMVAgent 中跨 family 生成 MV。

# 判定流程

FamilyAgent 必须按下面顺序进行 family 判断。该流程用于 LLM 内部推理，不要求在输出 JSON 中新增 `jaccard`、`containment`、`candidate_level` 或 `core_fact_table` 字段；最终输出仍必须严格符合 `FamilyOutput` schema。

1. 对每个 QueryBlock 提取内部判断特征：
   - `T(qb)`：物理表集合，来自 `tables`。
   - `J(qb)`：join skeleton，来自 `join_edges` 或 `family_candidates[].join_signature`。
   - `F(qb)`：过滤列集合，来自 `predicates[].columns` 或 `family_candidates[].predicate_set`。
   - `P(qb)`：predicate shape 集合，来自 `predicates[].predicate_shape`。
   - `M(qb)`：measure / aggregate 集合，来自 `aggregate_exprs[].agg_func + source_table.source_column`，或无法单字段追溯时来自 `aggregate_exprs[].expr`。
   - `G(qb)`：group by 集合，来自 `group_by_exprs[].expr`。
   - `core_fact_table_set(qb)`：事实表集合，优先从 `family_candidates[].core_fact_table_set` 读取。
2. 对候选 QueryBlock 计算或读取：
   - `Jaccard(A,B)=|T(A)∩T(B)|/|T(A)∪T(B)|`。
   - `Containment(A,B)=|T(A)∩T(B)|/min(|T(A)|,|T(B)|)`。
3. 先按阈值生成或接受 family candidate：
   - `strong candidate`：`Jaccard = 1.0`，或 `Containment = 1.0` 且 `core_fact_table_set` 相同。
   - `medium candidate`：`Jaccard >= 0.6` 且 `core_fact_table_set` 相同，或 `Containment >= 0.8` 且较小 QueryBlock 的表集合主要被较大 join domain 覆盖。
   - 低于上述阈值：默认不合并。
4. 对 `strong` / `medium` candidate 继续执行安全门；任一安全门不通过，都不能合并。
5. evaluate 阶段必须重新检查候选 family：
   - 拆开 core fact table 不同的错误合并。
   - 拆开过滤列集合完全不同的错误合并。
   - 合并被错误拆开的同列不同常量 QueryBlock。
   - 合并被错误拆开的 residual-filter 兼容 QueryBlock。
   - 删除重复 family，并保证每个 `qb_id` 只出现在一个最终 family 中。
6. FamilyAgent 需要先生成候选 QueryFamily，再进行 evaluate。
7. evaluate 阶段输出的仍然是完整 FamilyOutput，不输出单独评审报告。

# 示例 1：相同 join skeleton、相同 predicate、不同 group by

输入 QueryBlock：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "tables": ["date_dim", "store_sales", "item"],
      "relation_instances": [
        {"relation_instance_id": "date_dim", "physical_table": "date_dim", "role": null},
        {"relation_instance_id": "store_sales", "physical_table": "store_sales", "role": null},
        {"relation_instance_id": "item", "physical_table": "item", "role": null}
      ],
      "join_edges": [
        {
          "left_relation_instance_id": "date_dim",
          "left_table": "date_dim",
          "left_column": "d_date_sk",
          "right_relation_instance_id": "store_sales",
          "right_table": "store_sales",
          "right_column": "ss_sold_date_sk",
          "operator": "=",
          "expr": "date_dim.d_date_sk = store_sales.ss_sold_date_sk"
        },
        {
          "left_relation_instance_id": "store_sales",
          "left_table": "store_sales",
          "left_column": "ss_item_sk",
          "right_relation_instance_id": "item",
          "right_table": "item",
          "right_column": "i_item_sk",
          "operator": "=",
          "expr": "store_sales.ss_item_sk = item.i_item_sk"
        }
      ],
      "predicates": [
        {"expr": "item.i_manager_id = 1", "predicate_shape": "item.i_manager_id = ?", "columns": ["item.i_manager_id"], "relation_instance_ids": ["item"]},
        {"expr": "date_dim.d_moy = 11", "predicate_shape": "date_dim.d_moy = ?", "columns": ["date_dim.d_moy"], "relation_instance_ids": ["date_dim"]},
        {"expr": "date_dim.d_year = 2000", "predicate_shape": "date_dim.d_year = ?", "columns": ["date_dim.d_year"], "relation_instance_ids": ["date_dim"]}
      ],
      "group_by_exprs": [
        {"expr": "date_dim.d_year", "columns": ["date_dim.d_year"], "relation_instance_ids": ["date_dim"]},
        {"expr": "item.i_category_id", "columns": ["item.i_category_id"], "relation_instance_ids": ["item"]},
        {"expr": "item.i_category", "columns": ["item.i_category"], "relation_instance_ids": ["item"]}
      ],
      "aggregate_exprs": [
        {
          "expr": "SUM(store_sales.ss_ext_sales_price)",
          "agg_func": "SUM",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "source_relation_instance_id": "store_sales"
        }
      ]
    },
    {
      "qb_id": "q52.outer",
      "query_id": "q52",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        {
          "left_relation_instance_id": "date_dim",
          "left_table": "date_dim",
          "left_column": "d_date_sk",
          "right_relation_instance_id": "store_sales",
          "right_table": "store_sales",
          "right_column": "ss_sold_date_sk",
          "operator": "=",
          "expr": "date_dim.d_date_sk = store_sales.ss_sold_date_sk"
        },
        {
          "left_relation_instance_id": "store_sales",
          "left_table": "store_sales",
          "left_column": "ss_item_sk",
          "right_relation_instance_id": "item",
          "right_table": "item",
          "right_column": "i_item_sk",
          "operator": "=",
          "expr": "store_sales.ss_item_sk = item.i_item_sk"
        }
      ],
      "predicates": [
        {"expr": "item.i_manager_id = 1", "predicate_shape": "item.i_manager_id = ?", "columns": ["item.i_manager_id"], "relation_instance_ids": ["item"]},
        {"expr": "date_dim.d_moy = 11", "predicate_shape": "date_dim.d_moy = ?", "columns": ["date_dim.d_moy"], "relation_instance_ids": ["date_dim"]},
        {"expr": "date_dim.d_year = 2000", "predicate_shape": "date_dim.d_year = ?", "columns": ["date_dim.d_year"], "relation_instance_ids": ["date_dim"]}
      ],
      "group_by_exprs": [
        {"expr": "date_dim.d_year", "columns": ["date_dim.d_year"], "relation_instance_ids": ["date_dim"]},
        {"expr": "item.i_brand", "columns": ["item.i_brand"], "relation_instance_ids": ["item"]},
        {"expr": "item.i_brand_id", "columns": ["item.i_brand_id"], "relation_instance_ids": ["item"]}
      ],
      "aggregate_exprs": [
        {
          "expr": "SUM(store_sales.ss_ext_sales_price)",
          "agg_func": "SUM",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "source_relation_instance_id": "store_sales"
        }
      ]
    }
  ]
}
```

量化判断：

```text
T(q42) = {date_dim, store_sales, item}
T(q52) = {date_dim, store_sales, item}
Jaccard(q42, q52) = 3 / 3 = 1.0
Containment(q42, q52) = 3 / 3 = 1.0
candidate level: strong candidate
```

安全门判断：

```text
core fact table: store_sales，单事实表且相同
join graph: date_dim -> store_sales -> item，相同
predicate shape: 相同
measure: SUM(store_sales.ss_ext_sales_price)，兼容
roll-up: group by 维度可由更细粒度 aggregate MV 覆盖
```

输出 QueryFamily：

```json
{
  "query_families": [
    {
      "family_id": "family_fact_ss_dd_item_manager_moy_year",
      "family_key": "store_sales-date_dim-item|manager_moy_year|sum_ss_ext_sales_price",
      "family_type": "fact_based",
      "core_fact_table_set": ["store_sales"],
      "members": ["q42.outer", "q52.outer"],
      "common_tables": ["date_dim", "store_sales", "item"],
      "common_join_skeleton": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "common_predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year = 2000"
      ],
      "predicate_shapes": [
        "item.i_manager_id = <CONST>",
        "date_dim.d_moy = <CONST>",
        "date_dim.d_year = <CONST>"
      ],
      "union_group_by_exprs": [
        "date_dim.d_year",
        "item.i_category_id",
        "item.i_category",
        "item.i_brand",
        "item.i_brand_id"
      ],
      "union_measure_exprs": ["SUM(store_sales.ss_ext_sales_price)"]
    }
  ]
}
```

# 示例 2：事实表不同，即使维表相似也保持分离

如果一个 QueryBlock 以 `store_sales` 为 core fact table，另一个 QueryBlock 以 `web_sales` 为 core fact table，即使二者都连接 `date_dim` 和 `item`，第一版也不能合并为同一个 family。原因是两个事实表代表不同业务事件流，宽表 MV 会混合事实语义，并可能导致 rewrite 语义错误。

```text
T(q_store) = {store_sales, date_dim, item}
T(q_web) = {web_sales, date_dim, item}
Jaccard(q_store, q_web) = 2 / 4 = 0.5
Containment(q_store, q_web) = 2 / 3
core fact table: store_sales vs web_sales，不同
decision: keep separate
```

# 示例 3：同构 predicate、常量值不同，允许合并

输入片段：

```json
{
  "query_blocks": [
    {
      "qb_id": "q_year_2000.outer",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        {
          "left_table": "date_dim",
          "left_column": "d_date_sk",
          "right_table": "store_sales",
          "right_column": "ss_sold_date_sk",
          "operator": "=",
          "expr": "date_dim.d_date_sk = store_sales.ss_sold_date_sk"
        }
      ],
      "predicates": [
        {"expr": "item.i_manager_id = 1", "predicate_shape": "item.i_manager_id = ?", "columns": ["item.i_manager_id"], "relation_instance_ids": ["item"]},
        {"expr": "date_dim.d_year = 2000", "predicate_shape": "date_dim.d_year = ?", "columns": ["date_dim.d_year"], "relation_instance_ids": ["date_dim"]}
      ],
      "aggregate_exprs": [
        {
          "expr": "SUM(store_sales.ss_ext_sales_price)",
          "agg_func": "SUM",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "source_relation_instance_id": "store_sales"
        }
      ]
    },
    {
      "qb_id": "q_year_2001.outer",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        {
          "left_table": "date_dim",
          "left_column": "d_date_sk",
          "right_table": "store_sales",
          "right_column": "ss_sold_date_sk",
          "operator": "=",
          "expr": "date_dim.d_date_sk = store_sales.ss_sold_date_sk"
        }
      ],
      "predicates": [
        {"expr": "item.i_manager_id = 1", "predicate_shape": "item.i_manager_id = ?", "columns": ["item.i_manager_id"], "relation_instance_ids": ["item"]},
        {"expr": "date_dim.d_year = 2001", "predicate_shape": "date_dim.d_year = ?", "columns": ["date_dim.d_year"], "relation_instance_ids": ["date_dim"]}
      ],
      "aggregate_exprs": [
        {
          "expr": "SUM(store_sales.ss_ext_sales_price)",
          "agg_func": "SUM",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "source_relation_instance_id": "store_sales"
        }
      ]
    }
  ]
}
```

输出要点：

```json
{
  "query_families": [
    {
      "family_id": "family_fact_ss_dd_item_manager_year",
      "family_key": "store_sales-date_dim-item|manager_year|sum_ss_ext_sales_price",
      "members": ["q_year_2000.outer", "q_year_2001.outer"],
      "common_predicates": ["item.i_manager_id = 1"],
      "predicate_shapes": [
        "item.i_manager_id = <CONST>",
        "date_dim.d_year = <CONST>"
      ],
      "union_measure_exprs": ["SUM(store_sales.ss_ext_sales_price)"]
    }
  ]
}
```

# 示例 4：过滤列集合完全不同，保持分离

如果一个 QueryBlock 只过滤 `date_dim.d_year`，另一个 QueryBlock 只过滤 `item.i_manager_id`，即使它们有相同 core fact table、join skeleton 和 measure，第一版也应保持为不同 family。不要为了提高 family 大小而强行合并。

判断要点：

```text
q_year filter columns = {date_dim.d_year}
q_manager filter columns = {item.i_manager_id}
filter column intersection = empty
decision: keep separate
```

```json
{
  "query_families": [
    {
      "family_id": "family_fact_ss_dd_item_year",
      "family_key": "store_sales-date_dim-item|year|sum_ss_ext_sales_price",
      "members": ["q_year.outer"],
      "common_predicates": ["date_dim.d_year = 2000"],
      "predicate_shapes": ["date_dim.d_year = <CONST>"]
    },
    {
      "family_id": "family_fact_ss_dd_item_manager",
      "family_key": "store_sales-date_dim-item|manager|sum_ss_ext_sales_price",
      "members": ["q_manager.outer"],
      "common_predicates": ["item.i_manager_id = 1"],
      "predicate_shapes": ["item.i_manager_id = <CONST>"]
    }
  ]
}
```
