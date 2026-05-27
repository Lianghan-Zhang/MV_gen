# 职责

给定 QueryBlock 列表，按 `family_key` / join skeleton / predicate shape 聚合 QueryFamily。

# 规则

1. 聚合单位是 `qb_id`，不是完整 SQL。
2. `members` 只保存属于该 family 的 `qb_id`。
3. `family_id` 必须稳定、可读，优先由核心表名生成，例如 `family_ss_dd_item`。
4. 第一版优先保守合并；如果无法证明两个 QueryBlock 属于同一可复用结构，保持分离。
5. `common_tables` 记录 family 成员共同使用的物理表名。
6. `common_join_skeleton` 记录 family 成员共同拥有或可证明等价的 join 边，必须使用物理表名，不使用 SQL alias。
7. family 合并采用 predicate 兼容标准，不要求完整 predicate 完全相同。
8. `common_predicates` 只记录 family 内所有成员完全相同的完整谓词，例如所有成员都有 `item.i_manager_id = 1`。
9. `predicate_shapes` 记录同构过滤列但常量值可能不同的谓词形状，例如 `date_dim.d_year = <CONST>`。
10. 如果两个 QueryBlock 的 join skeleton 相同或可证明等价，过滤列结构同构但常量值不同，且 measure 兼容，可以合并到同一 family。
11. 如果过滤列集合差异过大、predicate 语义不清、join skeleton 不等价或 measure 不兼容，不能强行合并。
12. `union_group_by_exprs` 记录 family 内所有成员 group by 表达式的并集。
13. `union_measure_exprs` 记录 family 内所有成员 aggregate 表达式的并集。
14. 所有表名前缀必须使用物理表名，不使用 SQL alias。
15. FamilyAgent 需要先生成候选 QueryFamily，再进行 evaluate。
16. evaluate 阶段必须检查是否存在重复 family、可合并 family、错误拆分、错误合并、成员 QueryBlock 归属错误、`common_predicates` 与 `predicate_shapes` 混淆等问题。
17. evaluate 阶段输出的仍然是完整 FamilyOutput，不输出单独评审报告。

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

输出 QueryFamily：

```json
{
  "query_families": [
    {
      "family_id": "family_ss_dd_item",
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

# 示例 2：同构 predicate、常量值不同，允许合并

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
      "family_id": "family_ss_dd_item",
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

# 示例 3：过滤列集合差异过大，保持分离

如果一个 QueryBlock 过滤 `date_dim.d_year`，另一个 QueryBlock 过滤完全不同的业务字段，且无法证明它们能共享安全的 ETL superset MV，应保持为不同 family。不要为了提高 family 大小而强行合并。

```json
{
  "query_families": [
    {
      "family_id": "family_ss_dd_item_year",
      "family_key": "store_sales-date_dim-item",
      "members": ["q_year.outer"],
      "common_predicates": ["date_dim.d_year = 2000"],
      "predicate_shapes": ["date_dim.d_year = <CONST>"]
    },
    {
      "family_id": "family_ss_dd_item_manager",
      "family_key": "store_sales-date_dim-item",
      "members": ["q_manager.outer"],
      "common_predicates": ["item.i_manager_id = 1"],
      "predicate_shapes": ["item.i_manager_id = <CONST>"]
    }
  ]
}
```
