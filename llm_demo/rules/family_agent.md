# 职责

给定 `family_candidates.json` 和 QueryBlock 证据，按 upstream join-domain MV / shared superset MV 覆盖关系聚合 QueryFamily。

# 规则

1. 聚合单位是 `qb_id`，不是完整 SQL。
2. 输入的 `family_candidates` 是 code-only 预筛结果，只负责压缩候选空间；最终 family 仍由 FamilyAgent evaluate / merge / split 决定。
3. 不允许把 `candidate_family_id` 原样当作最终结论；必须检查是否需要合并、拆分、删除重复或修正成员。
4. `members` 只保存属于该 family 的 `qb_id`。
5. `family_id` 必须稳定、可读且全局唯一。它不只按表集合命名，而是按“可共享 MV 的语义边界”命名，格式为 `family_{family_type}_{domain}_{distinguishing_feature}`。
   - `family_type` 只能使用 `fact`、`dim`、`cte`、`mixed`。
   - `domain` 使用核心 fact table，或核心 dimension / join domain。
   - `distinguishing_feature` 使用能区分同表集合下不同 family 的关键 predicate、measure 或 derived semantics。
   - 如果 evaluate 后出现完全重复的 family，应删除重复项；如果只是同名但成员、join 或 predicate 信息不同，应输出语义可区分的不同 `family_id`，不能让同一个 id 表示两个 family。
   - 代码层的 `__2` 后缀只是避免流程中断的最后防线，不是设计命名规则；FamilyAgent 应优先生成语义唯一的 `family_id`。
6. `QueryFamily` 不是简单 SQL 相似簇，而是一组可由同一个 upstream join-domain MV 或 shared superset MV 覆盖的 QueryBlock。
7. 第一版优先保守合并；如果无法证明两个 QueryBlock 可被同一个 upstream MV 安全覆盖，保持分离。
8. `common_tables` 记录 family 成员共同使用的物理表名。
9. `common_join_skeleton` 记录 family 成员共同拥有或可证明等价的 join 边，必须使用物理表名，不使用 SQL alias。
10. family candidate 第一层使用表集合量化：`Jaccard(A,B)=|T(A)∩T(B)|/|T(A)∪T(B)|` 衡量相似度，`Containment(A,B)=|T(A)∩T(B)|/min(|T(A)|,|T(B)|)` 识别宽表覆盖关系。
11. core fact table 是 QueryBlock join domain 中承载事实记录或主要 measure 来源的中心表；TPC-DS 第一版可优先识别 `store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。
12. candidate 分层规则：`strong candidate` = `Jaccard = 1.0`，或 `Containment = 1.0` 且 core fact table 相同；`medium candidate` = `Jaccard >= 0.6` 且 core fact table 相同，或 `Containment >= 0.8` 且较小 QueryBlock 的表集合主要被较大 join domain 覆盖；低于该阈值默认保持分离。
13. core fact table 相同是 family 合并 hard gate；如果 core fact table 不同，即使 Jaccard / Containment 较高，也默认保持分离。
14. 多 fact table QueryBlock 只允许与具有完全相同 core fact table set、且 join graph 可证明不会引入行数膨胀的 QueryBlock 合并；否则单独成 family。
15. Jaccard 和 Containment 只负责生成 candidate，不直接决定最终合并。
16. family 最终合并必须通过安全门：core fact table、join graph、predicate shape、measure compatibility 和 roll-up safety 必须可证明兼容。
17. family 合并采用 predicate 兼容标准，不要求完整 predicate 完全相同。
18. 第一版 predicate shape 兼容只允许两类情况：同一过滤列常量不同，例如 `date_dim.d_year = 2000` 与 `date_dim.d_year = 2001`；或者一方比另一方多出过滤条件，且多出的过滤列可在 MV 中保留并作为下游 residual filter。
19. 如果两个 QueryBlock 的过滤列集合完全不同，例如一个只过滤 `date_dim.d_year`，另一个只过滤 `item.i_manager_id`，即使 core fact table、join graph 和 measure 兼容，也必须拆成不同 family。
20. `common_predicates` 只记录 family 内所有成员完全相同的完整谓词，例如所有成员都有 `item.i_manager_id = 1`。
21. `predicate_shapes` 记录同构过滤列但常量值可能不同的谓词形状，例如 `date_dim.d_year = <CONST>`。
22. 如果两个 QueryBlock 的 join skeleton 相同或可证明等价，过滤列结构同构但常量值不同，且 measure 兼容，可以合并到同一 family。
23. 如果过滤列集合差异过大、predicate 语义不清、join skeleton 不等价或 measure 不兼容，不能强行合并。
24. `union_group_by_exprs` 记录 family 内所有成员 group by 表达式的并集。
25. `union_measure_exprs` 记录 family 内所有成员 aggregate 表达式的并集。
26. 所有表名前缀必须使用物理表名，不使用 SQL alias。
27. FamilyAgent 需要先生成候选 QueryFamily，再进行 evaluate。
28. evaluate 阶段必须检查是否存在重复 family、可合并 family、错误拆分、错误合并、成员 QueryBlock 归属错误、`common_predicates` 与 `predicate_shapes` 混淆等问题。
29. evaluate 阶段输出的仍然是完整 FamilyOutput，不输出单独评审报告。

# family_id 命名规则

1. fact-based family 使用：

```text
family_fact_{fact_table}_{main_dims}_{predicate_or_measure_feature}
```

例如 q42/q52 可写成：

```text
family_fact_store_sales_date_dim_item_manager_month_year
```

如果使用缩写，缩写必须稳定。第一版允许的常用缩写如下：

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

2. dimension-only family 不能只写表名，必须加入关键过滤或派生语义。例如：

```text
family_dim_customer_address_city_mismatch
family_dim_customer_dn_count_range
```

不要输出：

```text
family_customer_customer_address_dn
```

3. CTE / derived QueryBlock 应体现派生含义。例如 `customer_total_return` 可命名为：

```text
family_cte_store_returns_customer_total_return
```

如果 block 名是 `dn` 这类弱语义别名，不要直接把 `dn` 当核心语义，应根据来源逻辑命名，例如：

```text
family_dim_customer_dn_count_range
```

4. 同一表集合但 predicate shape 不同，必须进入命名区分。例如：

```text
family_fact_ss_dd_item_year_manager
family_fact_ss_dd_item_category
```

5. 同一表集合但 measure 不同，也必须进入命名区分。例如：

```text
family_fact_ss_dd_item_sum_sales_price
family_fact_ss_dd_item_sum_net_paid
family_fact_ss_dd_item_count
```

6. q42/q52 这种 measure 相同但 group by 不同的 QueryBlock 仍应进入同一 family，因为可以通过更细粒度 MV roll-up 覆盖；不需要把 category / brand 分别写入两个 family_id。

# 判定流程

FamilyAgent 必须按下面顺序进行 family 判断。该流程用于 LLM 内部推理，不要求在输出 JSON 中新增 `jaccard`、`containment`、`candidate_level` 或 `core_fact_table` 字段；最终输出仍必须严格符合 `FamilyOutput` schema。

1. 对每个 QueryBlock 提取内部判断特征：
   - `T(qb)`：物理表集合，来自 `tables`。
   - `F(qb)`：过滤列集合，来自 `predicates` 中出现的物理列，例如 `date_dim.d_year`、`item.i_manager_id`。
   - `M(qb)`：measure / aggregate 集合，来自 `aggregate_exprs`。
   - `G(qb)`：group by 集合，来自 `group_by_exprs`。
   - `core_fact_table_set(qb)`：事实表集合，优先从 `store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory` 中识别。
2. 对候选 QueryBlock 计算：
   - `Jaccard(A,B)=|T(A)∩T(B)|/|T(A)∪T(B)|`。
   - `Containment(A,B)=|T(A)∩T(B)|/min(|T(A)|,|T(B)|)`。
3. 先按阈值生成 family candidate：
   - `strong candidate`：`Jaccard = 1.0`，或 `Containment = 1.0` 且 `core_fact_table_set` 相同。
   - `medium candidate`：`Jaccard >= 0.6` 且 `core_fact_table_set` 相同，或 `Containment >= 0.8` 且较小 QueryBlock 的表集合主要被较大 join domain 覆盖。
   - 低于上述阈值：默认不合并。
4. 对 `strong` / `medium` candidate 继续执行安全门；任一安全门不通过，都不能合并：
   - `core_fact_table_set` 必须相同。
   - `join_edges` 必须相同或可证明等价。
   - `F(qb)` 必须兼容：允许同一过滤列常量不同；允许一方多出过滤列且该列可作为 residual filter；不允许过滤列集合完全不同。
   - `M(qb)` 必须兼容；第一版不要合并度量语义不清或聚合函数不兼容的 QueryBlock。
   - `G(qb)` 必须满足 roll-up safety；共享 MV 应能保留更细粒度 group by 或 detail 粒度，使成员 QueryBlock 可以从 MV 派生。
5. evaluate 阶段必须重新检查候选 family：
   - 拆开 core fact table 不同的错误合并。
   - 拆开过滤列集合完全不同的错误合并。
   - 合并被错误拆开的同列不同常量 QueryBlock。
   - 合并被错误拆开的 residual-filter 兼容 QueryBlock。
   - 删除重复 family，并保证每个 `qb_id` 只出现在一个最终 family 中。

# 示例 1：相同 join skeleton、相同 predicate、不同 group by

输入 QueryBlock：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year = 2000"
      ],
      "group_by_exprs": [
        "date_dim.d_year",
        "item.i_category_id",
        "item.i_category"
      ],
      "aggregate_exprs": ["sum(store_sales.ss_ext_sales_price)"],
      "complexity_type": "join_filter_groupby",
      "family_key": "store_sales-date_dim-item"
    },
    {
      "qb_id": "q52.outer",
      "query_id": "q52",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year = 2000"
      ],
      "group_by_exprs": [
        "date_dim.d_year",
        "item.i_brand",
        "item.i_brand_id"
      ],
      "aggregate_exprs": ["sum(store_sales.ss_ext_sales_price)"],
      "complexity_type": "join_filter_groupby",
      "family_key": "store_sales-date_dim-item"
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
      "family_key": "store_sales-date_dim-item",
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
      "union_measure_exprs": ["sum(store_sales.ss_ext_sales_price)"]
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

输出要点：

```json
{
  "query_families": [
    {
      "family_id": "family_fact_ss_dd_item_sum_sales_price",
      "members": ["q_store.outer"],
      "common_tables": ["store_sales", "date_dim", "item"]
    },
    {
      "family_id": "family_fact_ws_dd_item_sum_sales_price",
      "members": ["q_web.outer"],
      "common_tables": ["web_sales", "date_dim", "item"]
    }
  ]
}
```

# 示例 3：同构 predicate、常量值不同，允许合并

输入片段：

```json
{
  "query_blocks": [
    {
      "qb_id": "q_year_2000.outer",
      "family_key": "store_sales-date_dim-item",
      "join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_year = 2000"
      ],
      "aggregate_exprs": ["sum(store_sales.ss_ext_sales_price)"]
    },
    {
      "qb_id": "q_year_2001.outer",
      "family_key": "store_sales-date_dim-item",
      "join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_year = 2001"
      ],
      "aggregate_exprs": ["sum(store_sales.ss_ext_sales_price)"]
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
      "family_key": "store_sales-date_dim-item",
      "members": ["q_year_2000.outer", "q_year_2001.outer"],
      "common_predicates": ["item.i_manager_id = 1"],
      "predicate_shapes": [
        "item.i_manager_id = <CONST>",
        "date_dim.d_year = <CONST>"
      ],
      "union_measure_exprs": ["sum(store_sales.ss_ext_sales_price)"]
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
      "family_key": "store_sales-date_dim-item",
      "members": ["q_year.outer"],
      "common_predicates": ["date_dim.d_year = 2000"],
      "predicate_shapes": ["date_dim.d_year = <CONST>"]
    },
    {
      "family_id": "family_fact_ss_dd_item_manager"
      "family_key": "store_sales-date_dim-item",
      "members": ["q_manager.outer"],
      "common_predicates": ["item.i_manager_id = 1"],
      "predicate_shapes": ["item.i_manager_id = <CONST>"]
    }
  ]
}
```
