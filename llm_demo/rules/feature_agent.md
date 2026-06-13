# 职责

给定一批 TPC-DS Spark SQL Text，提取以 QueryBlock 为最小单位的 SQL 事实结构。

FeatureAgent 只负责事实抽取，不负责 batch 分类，也不负责 family 判断。Batch 分类由 `batch_classification.csv` / BatchClusterAgent 处理；family candidate 和最终 QueryFamily 由后续 FamilyCandidateBuilder / FamilyAgent 根据 FeatureAgent 输出推导。

# 输出边界

1. 输出必须符合 `FeatureOutput` schema：`query_blocks`、`query_to_qbs`、`qb_to_query`。
2. `query_blocks[]` 只描述 SQL 中实际存在的 QueryBlock 事实。
3. 不输出 `complexity_type`、`block_shape`、`family_key`。
4. 不输出 `Batch-1`、`Batch-2`、`Batch-3`、`Batch-4`、`Batch-5`、`Other` 或任何 batch 判断字段。
5. 不在最终 Feature artifact 中保留 SQL 局部 alias，包括 table alias、relation alias 和 SELECT 输出 alias。
6. SQL alias 只允许在解析过程中用于定位真实物理表和字段，evaluate 后必须消失。

# QueryBlock 规则

1. Query 是执行单位，QueryBlock 是分析单位。
2. 每个 Query 至少输出一个 outer QueryBlock，`qb_id` 固定为 `{query_id}.outer`。
3. 必须识别 outer / CTE / subquery / set branch QueryBlock。
4. `scope_type` 可写 `outer`、`cte`、`subquery`、`set_branch`。
5. CTE QueryBlock 允许进入 QueryFamily 和 MV Candidate；如果外层 SQL 依赖 CTE，CTE 级 MV/rewrite 可能是核心优化路径。
6. `block_name` 只使用稳定分析名，例如 `outer`、`cte_1`、`subquery_1`、`branch_1`；不要使用 SQL 中的 CTE 名或 alias。
7. `parent_qb_id` 记录嵌套父 QueryBlock；outer 使用 null。
8. `depends_on_qb_ids` 记录当前 QueryBlock 直接依赖的 CTE / subquery QueryBlock。
9. `structural_flags` 只记录无法从字段直接稳定推导、但对后续有用的结构信号，例如 `has_cte`、`has_subquery`、`has_correlated_subquery`、`has_union`、`has_window`、`has_self_join`、`has_having`、`has_distinct`。
10. 不确定的信息写入 `unsupported_reasons`，不要编造表名、字段名、谓词、role 或依赖关系。

# RelationInstance 规则

1. `relation_instances` 记录当前 QueryBlock 内每个物理表实例。
2. `relation_instance_id` 必须与 SQL alias 无关。
3. 如果某个物理表在当前 QueryBlock 中只出现一次，`relation_instance_id` 使用物理表名，例如 `date_dim`。
4. 如果同一物理表出现多次，且能稳定判断业务角色，`relation_instance_id` 使用 `{physical_table}__{role}`，例如 `date_dim__sold_date`、`date_dim__returned_date`。
5. 如果同一物理表出现多次但无法稳定判断 role，`relation_instance_id` 使用按 SQL 出现顺序生成的物理实例号，例如 `date_dim__1`、`date_dim__2`；这不是 SQL alias，只是消歧用的实例编号。
6. `physical_table` 必须是真实物理表名。
7. `role` 仅在能从 SQL 语义稳定判断时填写，例如 `sold_date`、`returned_date`、`bill_customer`、`ship_customer`；无法稳定判断时使用 null，不要编造。
8. `tables` 是当前 QueryBlock 中物理表名的去重集合，只能写真实物理表名。

# Join / Predicate / Projection 规则

1. `join_edges` 记录表间等值连接关系，必须使用结构化对象。
2. `join_edges[].left_relation_instance_id` 和 `right_relation_instance_id` 必须引用 `relation_instances[].relation_instance_id`。
3. `join_edges[].left_table`、`right_table` 必须是真实物理表名。
4. `join_edges[].left_column`、`right_column` 只写物理字段名，不写表前缀。
5. `join_edges[].expr` 使用无 alias 的规范化表达式；普通表可使用 `table.column`，多实例物理表可使用 `relation_instance_id.column` 消歧。
6. `predicates` 只记录非 join filter。
7. `predicates[].expr` 使用无 alias 的规范化表达式。
8. `predicates[].predicate_shape` 把常量替换为 `?`，例如 `date_dim.d_year = ?`；如果无法稳定抽象，可使用 null。
9. `predicates[].columns` 记录谓词中出现的物理列，格式为 `table.column`。
10. `predicates[].relation_instance_ids` 记录谓词涉及的物理表实例。
11. `projections` 记录 SELECT 输出表达式本身，但不记录 SELECT 输出 alias。

# Group / Aggregate / Order 规则

1. `group_by_exprs` 使用结构化对象，`expr` 使用无 alias 的规范化表达式。
2. `aggregate_exprs` 使用结构化对象。
3. `aggregate_exprs[].expr` 使用无 alias 的规范化表达式，例如 `SUM(store_sales.ss_ext_sales_price)`。
4. `aggregate_exprs[].agg_func` 写聚合函数名，例如 `SUM`、`COUNT`、`AVG`。
5. `aggregate_exprs[].source_table` 和 `source_column` 只在聚合可追溯到单一物理源字段时填写。
6. 如果聚合表达式无法追溯到单一物理源字段，例如 `SUM(a - b)`、ratio、CASE、多列组合表达式，仍记录 `expr`，但不要编造 `source_table` 或 `source_column`。
7. `aggregate_exprs[].source_relation_instance_id` 必须引用 `relation_instances[].relation_instance_id`；无法稳定追溯到单一实例时使用 null。
8. `order_by_exprs` 记录 ORDER BY 表达式，但不记录输出 alias；`direction` 可写 `ASC`、`DESC` 或 null。
9. `limit` 记录 LIMIT 表达式；没有 LIMIT 时使用 null。

# Evaluate 规则

1. FeatureAgent 会先执行提取，再执行一次 evaluate。
2. evaluate 的输入包含原始 SQL 和 `candidate_feature_output`。
3. evaluate 阶段必须检查 outer、CTE、subquery、set branch QueryBlock 是否完整。
4. evaluate 阶段必须检查 `query_to_qbs` 和 `qb_to_query` 是否与 `query_blocks` 一致。
5. evaluate 阶段必须检查所有表达式是否已去除 SQL alias，并规范化为物理表名、物理字段名和稳定 `relation_instance_id`。
6. evaluate 阶段必须检查 `relation_instance_ids` 是否引用当前 QueryBlock 的 `relation_instances`。
7. evaluate 阶段必须删除或忽略 `complexity_type`、`block_shape`、`family_key`、batch 判断字段。
8. evaluate 阶段输出的仍然是完整 FeatureOutput，不输出单独的评审报告。

# 示例

输入：

```json
{
  "query_id": "q42",
  "sql_text": "SELECT dt.d_year AS year_key, item.i_category_id, SUM(store_sales.ss_ext_sales_price) AS total_sales FROM date_dim dt, store_sales, item WHERE dt.d_date_sk = store_sales.ss_sold_date_sk AND store_sales.ss_item_sk = item.i_item_sk AND item.i_manager_id = 1 AND dt.d_moy = 11 AND dt.d_year = 2000 GROUP BY dt.d_year, item.i_category_id"
}
```

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
      "relation_instances": [
        {
          "relation_instance_id": "date_dim",
          "physical_table": "date_dim",
          "role": null
        },
        {
          "relation_instance_id": "store_sales",
          "physical_table": "store_sales",
          "role": null
        },
        {
          "relation_instance_id": "item",
          "physical_table": "item",
          "role": null
        }
      ],
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
        {
          "expr": "item.i_manager_id = 1",
          "predicate_shape": "item.i_manager_id = ?",
          "columns": ["item.i_manager_id"],
          "relation_instance_ids": ["item"]
        },
        {
          "expr": "date_dim.d_moy = 11",
          "predicate_shape": "date_dim.d_moy = ?",
          "columns": ["date_dim.d_moy"],
          "relation_instance_ids": ["date_dim"]
        },
        {
          "expr": "date_dim.d_year = 2000",
          "predicate_shape": "date_dim.d_year = ?",
          "columns": ["date_dim.d_year"],
          "relation_instance_ids": ["date_dim"]
        }
      ],
      "projections": [
        {
          "expr": "date_dim.d_year",
          "columns": ["date_dim.d_year"],
          "relation_instance_ids": ["date_dim"]
        },
        {
          "expr": "item.i_category_id",
          "columns": ["item.i_category_id"],
          "relation_instance_ids": ["item"]
        },
        {
          "expr": "SUM(store_sales.ss_ext_sales_price)",
          "columns": ["store_sales.ss_ext_sales_price"],
          "relation_instance_ids": ["store_sales"]
        }
      ],
      "group_by_exprs": [
        {
          "expr": "date_dim.d_year",
          "columns": ["date_dim.d_year"],
          "relation_instance_ids": ["date_dim"]
        },
        {
          "expr": "item.i_category_id",
          "columns": ["item.i_category_id"],
          "relation_instance_ids": ["item"]
        }
      ],
      "aggregate_exprs": [
        {
          "expr": "SUM(store_sales.ss_ext_sales_price)",
          "agg_func": "SUM",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "source_relation_instance_id": "store_sales"
        }
      ],
      "order_by_exprs": [],
      "limit": null,
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
