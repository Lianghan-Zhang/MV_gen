# 职责

给定一批 TPC-DS Spark SQL Text，提取以 QueryBlock 为最小单位的结构化特征。

# 规则

1. Query 是执行单位，QueryBlock 只是分析单位。
2. 每个 Query 至少输出一个 outer QueryBlock，`qb_id` 固定为 `{query_id}.outer`。
3. 必须识别 outer / CTE / subquery / set branch QueryBlock。`scope_type` 可写 `outer`、`cte`、`subquery`、`set_branch`。
4. CTE QueryBlock 允许进入 QueryFamily 和 MV Candidate；如果外层 SQL 依赖 CTE，CTE 级 MV/rewrite 可能是核心优化路径。
5. `block_name` 记录 CTE 名称或子查询标识；outer 可为空或写 `outer`。
6. `parent_qb_id` 记录嵌套父 QueryBlock；outer 使用 null。
7. `depends_on_qb_ids` 记录当前 QueryBlock 直接依赖的 CTE / subquery QueryBlock。
8. `structural_flags` 记录结构特征，例如 `has_cte`、`has_subquery`、`has_union`、`has_window`、`has_correlated_subquery`。
9. `tables` 必须使用真实物理表名，不使用 SQL alias。
10. `join_edges` 记录表间等值连接关系；`predicates` 记录非 join filter。
11. 所有 table-qualified 表达式都必须使用真实物理表名作为前缀，不使用 SQL alias。例如 SQL 中 `date_dim dt` 的 `dt.d_year` 必须输出为 `date_dim.d_year`。
12. `group_by_exprs` 和 `aggregate_exprs` 保留 SQL 表达式语义，但其中的表名前缀必须规范化为物理表名。
13. `complexity_type` 只能是 `join`、`join_filter`、`join_filter_groupby`、`other`。
14. `join_filter_groupby` 在本系统中表示 join 上的聚合复杂层；只要 QueryBlock 同时包含 join 和 `group_by_exprs` 或 `aggregate_exprs`，即使没有 filter，也应写为 `join_filter_groupby`。
15. q42/q52 这类包含 `date_dim`、`store_sales`、`item` 的查询，`family_key` 固定写为 `store_sales-date_dim-item`。
16. FeatureAgent 会先执行提取，再执行一次 evaluate。evaluate 的输入包含原始 SQL 和 `candidate_feature_output`。
17. evaluate 阶段必须检查并修正 `candidate_feature_output`：如果发现 SQL alias，需要把它替换为 SQL 中声明的真实物理表名；如果发现未知表名前缀，需要写入 `unsupported_reasons`，不要编造字段。
18. evaluate 阶段输出的仍然是完整 FeatureOutput，不输出单独的评审报告。
19. 不确定的信息写入 `unsupported_reasons`，不要编造表名、字段名或谓词。

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

# 示例 2：set branch 内部是 join + aggregate，无 filter 也归入 join_filter_groupby

输入片段类似 q11 的 CTE 分支：

```json
{
  "query_id": "q11",
  "sql_text": "WITH year_total AS (SELECT c_customer_id, d_year, SUM(ss_ext_list_price - ss_ext_discount_amt) year_total FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, d_year UNION ALL SELECT c_customer_id, d_year, SUM(ws_ext_list_price - ws_ext_discount_amt) year_total FROM customer, web_sales, date_dim WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk GROUP BY c_customer_id, d_year) SELECT * FROM year_total"
}
```

输出要点：

```json
{
  "query_blocks": [
    {
      "qb_id": "q11.year_total.branch1",
      "query_id": "q11",
      "block_name": "branch1",
      "scope_type": "set_branch",
      "parent_qb_id": "q11.year_total",
      "depends_on_qb_ids": [],
      "structural_flags": [],
      "tables": ["customer", "store_sales", "date_dim"],
      "join_edges": [
        "customer.c_customer_sk = store_sales.ss_customer_sk",
        "store_sales.ss_sold_date_sk = date_dim.d_date_sk"
      ],
      "predicates": [],
      "group_by_exprs": ["customer.c_customer_id", "date_dim.d_year"],
      "aggregate_exprs": ["sum(store_sales.ss_ext_list_price - store_sales.ss_ext_discount_amt)"],
      "complexity_type": "join_filter_groupby",
      "family_key": "customer-date_dim-store_sales",
      "unsupported_reasons": []
    }
  ]
}
```

注意：这里没有 filter，但该 QueryBlock 有 join 和 aggregate/group by，因此仍然是 `join_filter_groupby`，不是 `other`。

输出：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "block_name": "outer",
      "scope_type": "outer",
      "parent_qb_id": null,
      "depends_on_qb_ids": [],
      "structural_flags": [],
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "store_sales.ss_item_sk = item.i_item_sk"
      ],
      "predicates": ["item.i_manager_id = 1", "date_dim.d_moy = 11", "date_dim.d_year = 2000"],
      "group_by_exprs": ["date_dim.d_year", "item.i_category_id"],
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
