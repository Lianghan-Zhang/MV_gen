# 职责

给定一批 TPC-DS Spark SQL Text，提取以 QueryBlock 为最小单位的结构化特征。

# 规则

1. Query 是执行单位，QueryBlock 只是分析单位。
2. 每个 Query 至少输出一个 outer QueryBlock，`qb_id` 固定为 `{query_id}.outer`。
3. 第一版只需要识别 outer QueryBlock；CTE / subquery 可在后续版本扩展。
4. `tables` 必须使用真实基础表名，不使用别名。
5. `join_edges` 记录表间等值连接关系；`predicates` 记录非 join filter。
6. `group_by_exprs` 和 `aggregate_exprs` 按 SQL Text 中出现的表达式输出。
7. `complexity_type` 只能是 `join`、`join_filter`、`join_filter_groupby`、`other`。
8. 如果 SQL 同时包含 join、filter 和 group by，`complexity_type = "join_filter_groupby"`。
9. q42/q52 这类包含 `date_dim`、`store_sales`、`item` 的查询，`family_key` 固定写为 `store_sales-date_dim-item`。
10. 不确定的信息写入 `unsupported_reasons`，不要编造表名、字段名或谓词。

# 示例

输入：

```json
{
  "queries": [
    {
      "query_id": "q42",
      "sql_text": "SELECT dt.d_year, item.i_category_id, sum(ss_ext_sales_price) FROM date_dim dt, store_sales, item WHERE dt.d_date_sk = store_sales.ss_sold_date_sk AND store_sales.ss_item_sk = item.i_item_sk AND item.i_manager_id = 1 AND dt.d_moy = 11 AND dt.d_year = 2000 GROUP BY dt.d_year, item.i_category_id"
    }
  ]
}
```

输出：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "scope_type": "outer",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "predicates": ["item.i_manager_id = 1", "date_dim.d_moy = 11", "date_dim.d_year = 2000"],
      "group_by_exprs": ["dt.d_year", "item.i_category_id"],
      "aggregate_exprs": ["sum(ss_ext_sales_price)"],
      "complexity_type": "join_filter_groupby",
      "family_key": "store_sales-date_dim-item",
      "unsupported_reasons": []
    }
  ],
  "query_to_qbs": {
    "q42": ["q42.outer"]
  },
  "qb_to_query": {
    "q42.outer": "q42"
  }
}
```
