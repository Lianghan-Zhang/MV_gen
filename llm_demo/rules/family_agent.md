# 职责

给定 QueryBlock 列表，按 family_key / join skeleton 聚合 QueryFamily。

# 规则

1. 聚合单位是 `qb_id`，不是 Query。
2. 第一版优先 exact family：只有 `family_key` 相同的 QueryBlock 才合并。
3. 不做激进模糊合并；依据不足时保持分离。
4. `family_id` 必须稳定。`family_key = "store_sales-date_dim-item"` 时使用 `family_ss_dd_item`。
5. `members` 中保留所有属于该 family 的 `qb_id`。
6. `common_tables` 是 family 成员共同涉及的表。
7. `common_predicates` 是 family 成员共同出现或语义一致的 filter。
8. `union_group_by_exprs` 和 `union_measure_exprs` 是 family 成员的并集。

# 示例

输入中存在：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "family_key": "store_sales-date_dim-item",
      "tables": ["date_dim", "store_sales", "item"],
      "predicates": ["item.i_manager_id = 1", "date_dim.d_moy = 11", "date_dim.d_year = 2000"],
      "group_by_exprs": ["dt.d_year", "item.i_category_id"],
      "aggregate_exprs": ["sum(ss_ext_sales_price)"]
    },
    {
      "qb_id": "q52.outer",
      "family_key": "store_sales-date_dim-item",
      "tables": ["date_dim", "store_sales", "item"],
      "predicates": ["item.i_manager_id = 1", "date_dim.d_moy = 11", "date_dim.d_year = 2000"],
      "group_by_exprs": ["dt.d_year", "item.i_brand_id"],
      "aggregate_exprs": ["sum(ss_ext_sales_price)"]
    }
  ]
}
```

输出：

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
      "common_predicates": ["item.i_manager_id = 1", "date_dim.d_moy = 11", "date_dim.d_year = 2000"],
      "union_group_by_exprs": ["dt.d_year", "item.i_category_id", "item.i_brand_id"],
      "union_measure_exprs": ["sum(ss_ext_sales_price)"]
    }
  ]
}
```
