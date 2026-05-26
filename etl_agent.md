# ETL 场景下基于物化视图的 Agent 查询编排加速系统实现方案

> 版本：v4.1  
> 日期：2026-05-22  
> 设计原则：第一版只做运行顺序编排和 batch 内 MV 生成，不做执行前全局 MV 选择。SQL 是执行单位，QueryBlock 是分析单位；核心结构控制在 `QueryBlock`、`QueryFamily`、`ComplexityBatch` 三类。

## 0. 核心定位

本系统当前阶段先不追求完整运行优化器，而是做一个类似物理执行计划的查询编排器：

```text
执行前：
  扫描 TPC-DS SQL
  以 QueryBlock 为最小单位提取特征
  基于 QueryBlock 提取 QueryFamily
  以 SQL/query_id 为单位按复杂度分配编排 batch
  不生成全局 MV candidate
  不物化 MV

编排流程：
  按 batch 顺序规划
  每个 batch 内基于当前 batch SQL 扫描、生成、物化 MV
  当前 batch 运行前先使用全局已物化 MV rewrite
  再基于 rewrite 后的新 SQL 生成本 batch 的 MV
  本 batch 新生成 MV 进入全局可用集合
  后续 batch 可使用所有历史 batch 生成的 MV
```

一句话主线：

```text
QueryBlock 特征提取
  -> QueryFamily 聚合
  -> SQL 级 ComplexityBatch 编排
  -> Batch-k 使用历史 MV rewrite
  -> 基于 rewrite 后的 Batch-k SQL 生成并物化本 batch MV
  -> 本 batch MV 全局可用
  -> Batch-k+1 继续复用
```

技术栈：

| 模块 | 技术 |
|---|---|
| SQL 解析 | Python + SQLGlot，Spark dialect |
| 数据集 | TPC-DS 数据和 Spark SQL |
| 执行引擎 | Spark SQL |
| Agent 框架 | 自研轻量 Agent，参考 `dataintel_client` 形式 |
| 中间结果 | JSON + SQL 文件 + 执行日志 |

## 1. 核心数据结构

### 1.1 QueryBlock

`QueryBlock` 是最小分析单位，不是执行单位。一个 SQL 可以包含 outer query、CTE、subquery 等多个 QueryBlock；执行时仍然以整条 SQL 的 `query_id` 为单位。

```python
QueryBlock:
  qb_id: str
  query_id: str
  file_path: str
  scope_type: str              # outer / cte / subquery
  raw_sql: str

  tables: list[str]
  aliases: dict[str, str]
  join_edges: list[JoinEdge]
  predicates: list[Predicate]
  group_by_exprs: list[str]
  aggregate_exprs: list[AggregateExpr]
  projection_exprs: list[str]
  order_by_exprs: list[str]
  limit: int | None

  has_join: bool
  has_filter: bool
  has_group_by: bool
  has_aggregate: bool
  has_window: bool
  has_subquery: bool
  has_rollup: bool

  complexity_type: str         # join / join_filter / join_filter_groupby / other
  family_key: str
  unsupported_reasons: list[str]
```

复杂度分类规则：

| `complexity_type` | 条件 |
|---|---|
| `join` | 有 join，无 filter，无 group by |
| `join_filter` | 有 join，有 filter，无 group by |
| `join_filter_groupby` | 有 group by 或 aggregate |
| `other` | window、rollup、复杂 subquery、set op、unsupported |

### 1.2 QueryFamily

`QueryFamily` 是基于 QueryBlock 的 family 集合。第一版按 `family_key` 精确分组，不做复杂 join 子图匹配。

```python
QueryFamily:
  family_id: str
  family_key: str
  members: list[str]           # qb_id list

  common_tables: list[str]
  common_join_edges: list[JoinEdge]
  common_predicates: list[Predicate]
  predicate_clusters: list[PredicateCluster]

  union_group_by_exprs: list[str]
  union_measure_exprs: list[AggregateExpr]
  family_priority: float
```

职责：

1. 识别共享 join skeleton 的 QueryBlock。
2. 统计公共 filter 和高频 predicate。
3. 识别 group by 分叉。
4. 为 batch 内 MV 生成提供 family 依据。

### 1.3 ComplexityBatch

`ComplexityBatch` 是工作流编排 batch，用来表达 SQL 复杂度层级和 MV 生成/复用顺序；当前阶段不处理 batch 内并发切分。它保存的是 `query_id`，不是 `qb_id`。

```python
ComplexityBatch:
  batch_id: int
  batch_type: str              # join / join_filter / join_filter_groupby / other
  query_ids: list[str]          # SQL 文件级执行单位

  input_sql_paths: dict[str, str]
  rewritten_sql_paths: dict[str, str]
  materialized_mv_ids: list[str]
  status: str                    # planned / rewritten / materialized / executed
```

Batch 生成规则：

```text
Batch-1: query_complexity = join
Batch-2: query_complexity = join_filter
Batch-3: query_complexity = join_filter_groupby
Batch-4: query_complexity = other
```

其中 `query_complexity` 由该 SQL 内所有 QueryBlock 的最高复杂度决定：

```text
other > join_filter_groupby > join_filter > join
```

例如：

```text
q42.outer = join_filter_groupby
=> q42 进入 Batch-3

q70.outer = other, q70.subquery = join_filter_groupby
=> q70 整条 SQL 进入 Batch-4
```

并发上限、子 batch 拆分、资源调度先不在第一版处理。后续如果需要真实执行调度，可以在 `ComplexityBatch` 之上再增加执行器层面的切分逻辑，但不影响当前的编排 batch 定义。

### 1.4 materialized_mvs

运行时只维护一个轻量全局 map，不单独设计复杂 registry。

```python
materialized_mvs = {
  "mv_ss_dd_item_mgr1_y2000_m11_fg": {
    "table_name": "mv_ss_dd_item_mgr1_y2000_m11_fg",
    "status": "success",
    "built_in_batch": 1,
    "available_from_batch": 2,
    "build_runtime_ms": 12345,
    "row_count": 100000
  }
}
```

全局可用规则：

```text
status = success
available_from_batch <= current_batch_id
```

也就是说，Batch-1 和 Batch-2 生成的 MV 都可以给 Batch-3 使用。

### 1.5 轻量派生索引

为了让 SQL 执行单位和 QueryBlock 分析单位能互相定位，需要两个轻量索引。它们不是新的核心数据结构，只是 `query_blocks.json` 的派生视图。

```python
query_to_qbs = {
  "q42": ["q42.outer"],
  "q70": ["q70.outer", "q70.subquery.1"]
}

qb_to_query = {
  "q42.outer": "q42",
  "q70.subquery.1": "q70"
}
```

用途：

1. `BatchClusterAgent` 以 `query_id` 分 batch 时，查看该 SQL 下所有 QB 的最高复杂度。
2. `FamilyAgent` 按 `qb_id` 聚 family 后，能反查这些 QB 属于哪些 SQL。
3. `BatchMVAgent` 在当前 batch 中先拿到 `query_ids`，再展开为 `batch_qbs` 做 MV 生成和 rewrite 判断。

## 2. 执行前编排阶段

执行前只做运行顺序编排，不做全局 MV candidate 获取。

```text
SQLLoaderAgent
  -> FeatureAgent
      -> query_blocks.json
      -> query_to_qbs / qb_to_query
  -> parallel:
      FamilyAgent
      BatchClusterAgent
```

输出：

| Artifact | 说明 |
|---|---|
| `query_blocks.json` | 全量 QueryBlock 特征 |
| `query_families.json` | 基于 qb_id 的 QueryFamily 集合 |
| `complexity_batches.json` | 基于 query_id 的编排 batch 顺序 |
| `query_to_qbs.json` | SQL 到 QueryBlock 的轻量索引 |
| `qb_to_query.json` | QueryBlock 到 SQL 的轻量索引 |

不输出：

```text
全局 mv_candidates.json
全局 MV 选择计划
全局 MV 物化计划
```

原因：MV 是否值得生成，要结合当前 batch 已被历史 MV 改写后的新 SQL 再判断。提前全局生成 candidate 容易和实际运行时 SQL 不一致。

`BatchClusterAgent` 和 `FamilyAgent` 可以并行，因为二者都只依赖 `FeatureAgent` 的 QueryBlock 结果：

```text
BatchClusterAgent:
  输入 query_blocks + query_to_qbs
  输出 SQL 级 ComplexityBatch

FamilyAgent:
  输入 query_blocks
  输出 QB 级 QueryFamily
```

二者完成后，`BatchMVAgent` 使用 `query_to_qbs` 把当前 batch 的 `query_ids` 展开成 `batch_qbs`，再结合 QueryFamily 做 MV 生成和 rewrite 判断。

## 3. Batch 编排逻辑

每个 batch 的编排流程分成 6 步。这里描述的是后续工作流如何组织 MV 生成、rewrite 和执行顺序，不处理 batch 内并发切分。

```text
for batch in complexity_batches:
  1. 读取当前 batch.query_ids 对应的 SQL
  2. 使用全局 materialized_mvs rewrite 当前 batch SQL
  3. 对 rewrite 后的新 SQL 重新提取 QueryBlock / Family
  4. 基于当前 batch 的新 SQL 扫描、生成、选择 MV
  5. 物化本 batch 选中的 MV，并写入 materialized_mvs
  6. 使用更新后的 materialized_mvs 对当前 batch SQL 做最终 rewrite 并执行
```

这套顺序同时满足三个要求：

1. Batch-1 没有历史 MV，因此先基于 Batch-1 自身 SQL 生成和物化 MV，再重写相关 SQL。
2. Batch-2 先用 Batch-1 的 MV 改写 SQL，再基于改写后的 Batch-2 新 SQL 生成 Batch-2 的 MV。
3. Batch-3 可以同时使用 Batch-1 和 Batch-2 已物化成功的 MV。

为避免同一个 batch 内无限迭代，第一版只做一次：

```text
历史 MV rewrite
  -> batch-local MV 生成和物化
  -> final rewrite
  -> execute
```

不在同一个 batch 内反复生成 MV、反复 rewrite。

### 3.1 Batch-1

```text
Batch-1:
  输入：Batch-1 query_ids 对应的原始 SQL
  历史 MV：空
  操作：
    1. 扫描 Batch-1 SQL，并提取 batch_qbs
    2. 基于 Batch-1 family 生成 MV candidate
    3. 物化选定 MV
    4. 使用这些 MV rewrite Batch-1 相关 SQL
    5. 执行 rewritten/original SQL
  输出：
    Batch-1 执行结果
    materialized_mvs += Batch-1 MV
```

### 3.2 Batch-2

```text
Batch-2:
  输入：Batch-2 query_ids 对应的原始 SQL
  历史 MV：Batch-1 MV
  操作：
    1. 使用 Batch-1 MV rewrite Batch-2 SQL
    2. 对 rewrite 后的 Batch-2 新 SQL 重新提取 QueryBlock / Family
    3. 基于新 SQL 生成 Batch-2 MV candidate
    4. 物化选定 Batch-2 MV
    5. 使用 Batch-1 + Batch-2 MV 做 final rewrite
    6. 执行 rewritten/original SQL
  输出：
    Batch-2 执行结果
    materialized_mvs += Batch-2 MV
```

### 3.3 Batch-3 及后续

```text
Batch-3:
  可用 MV = Batch-1 MV + Batch-2 MV
  先 rewrite，再基于 rewrite 后的新 SQL 生成 Batch-3 MV
  Batch-3 MV 继续进入全局 materialized_mvs，供 Batch-4 使用
```

## 4. Batch 内 MV 生成

MV 生成只发生在 batch 编排/运行阶段。

```text
执行前：不生成全局 MV candidate
Batch 内：扫描当前 batch 的当前 SQL，生成本 batch MV candidate
```

这里的“当前 SQL”指：

```text
Batch-1: 原始 SQL
Batch-k(k>1): 先被历史 MV rewrite 后的新 SQL
```

这里的“当前 batch”指 SQL 集合，不是 QB 集合：

```text
current_batch.query_ids
  -> query_to_qbs
  -> batch_qbs
  -> batch_families
  -> MV candidate
```

### 4.1 生成规则

第一版优先生成：

```text
filtered fine-grain aggregate MV
```

生成依据：

| 组成 | 策略 |
|---|---|
| Join | 当前 batch family 的公共 join skeleton |
| Filter | 当前 batch 内公共或高频 predicate |
| Group By | 当前 batch / 下游 batch 可服务 SQL 的 group by 并集 |
| Measure | 可分解聚合并集 |
| Output | group key + measure + residual filter/order 所需列 |

### 4.2 选择规则

第一版评分：

```text
score =
  estimated_reuse_count
  - alpha * estimated_build_cost
  - beta * estimated_output_size
  - gamma * rewrite_risk
```

选择条件：

1. 能服务当前 batch 或后续 batch 的 QueryBlock。
2. `score > threshold`。
3. 当前 batch 物化 MV 数量不超过配置上限。
4. MV 构建失败则标记 failed，不参与后续 rewrite。

## 5. Rewrite 规则

Rewriter 只基于已经成功物化的 MV 做重写。

```text
候选 MV 存在 != 可以 rewrite
MV 成功物化且全局可用 == 可以进入 rewrite 检查
```

合法性检查：

| 检查 | 规则 |
|---|---|
| Join Family | QueryBlock 与 MV family 兼容 |
| Predicate | Query predicate 范围必须小于或等于 MV predicate |
| Grain | Query group by 必须是 MV group by 子集 |
| Measure | Query measure 可由 MV measure 推导 |
| Columns | Select / group / order / residual filter 字段在 MV 输出中 |

第一版支持：

```text
SUM
COUNT
AVG = SUM / COUNT
MIN
MAX
AND + equality + IN + BETWEEN
```

第一版 fallback：

```text
OR
COUNT DISTINCT
STDDEV
复杂 window
rollup / cube
correlated subquery
set operation
```

## 6. Agent 设计

Agent 数量保持克制。

| Agent | 职责 |
|---|---|
| `SQLLoaderAgent` | 读取 workflow SQL |
| `FeatureAgent` | 用 SQLGlot 提取 QueryBlock |
| `FamilyAgent` | 从 QueryBlock 聚合 QueryFamily，分析单位是 `qb_id` |
| `BatchClusterAgent` | 按 SQL 最高复杂度生成 ComplexityBatch，执行单位是 `query_id` |
| `BatchMVAgent` | 在 batch 内扫描、生成、选择并物化 MV |
| `RewriteAgent` | 基于全局已物化 MV 改写当前 batch SQL |
| `ExecutorAgent` | 执行 SQL、收集日志、维护 `materialized_mvs` |
| `SelfIterationAgent` | 基于日志输出规则反馈 |

`BatchMVAgent` 可以先内部拆成函数，而不是拆更多 Agent：

```text
extract_batch_features()
generate_batch_mv_candidates()
select_batch_mvs()
materialize_selected_mvs()
```

`BatchMVAgent` 的关键输入不是单一结构，而是三者合并后的视图：

```text
batch.query_ids
query_to_qbs / qb_to_query
query_families
```

它先通过 `query_ids -> batch_qbs` 找到当前 batch 的 QueryBlock，再通过 QueryFamily 找到复用簇。

## 7. MVP：q42 / q52

### 7.1 执行前

`q42` 和 `q52` 是 SQL 执行单位；它们各自的 outer QueryBlock 被提取出来，并进入同一个 QueryFamily：

```text
q42.outer, q52.outer:
  family: store_sales ⋈ date_dim ⋈ item
  complexity_type: join_filter_groupby

q42, q52:
  query_complexity: join_filter_groupby
  batch: Batch-3
```

共同特征：

| 项 | 内容 |
|---|---|
| Join | `date_dim + store_sales + item` |
| Filter | `i_manager_id = 1`, `d_moy = 11`, `d_year = 2000` |
| Measure | `SUM(ss_ext_sales_price)` |

差异：

| Query | Group By |
|---|---|
| `q42` | `d_year`, `i_category_id`, `i_category` |
| `q52` | `d_year`, `i_brand_id`, `i_brand` |

### 7.2 运行时

因为 `q42/q52` 属于 Batch-3，能够服务它们的 MV 应该在 Batch-1 或 Batch-2 中生成并物化。

```text
Batch-1:
  生成并物化一批 Batch-1 MV

Batch-2:
  使用 Batch-1 MV rewrite Batch-2 SQL
  基于 rewrite 后的 Batch-2 SQL 生成并物化 Batch-2 MV

Batch-3:
  使用 Batch-1 + Batch-2 的全局 MV rewrite q42/q52
  执行 rewritten q42/q52
```

如果在 Batch-2 生成了以下 MV：

```text
mv_ss_dd_item_mgr1_y2000_m11_fg
```

则 Batch-3 可以用它改写 q42/q52。

### 7.3 MV SQL

```sql
CREATE OR REPLACE TABLE mv_ss_dd_item_mgr1_y2000_m11_fg AS
SELECT
  dt.d_year,
  dt.d_moy,
  item.i_manager_id,
  item.i_category_id,
  item.i_category,
  item.i_brand_id,
  item.i_brand,
  SUM(store_sales.ss_ext_sales_price) AS sum_ext_sales_price
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 2000
GROUP BY
  dt.d_year,
  dt.d_moy,
  item.i_manager_id,
  item.i_category_id,
  item.i_category,
  item.i_brand_id,
  item.i_brand;
```

### 7.4 q42 rewritten

```sql
SELECT
  d_year,
  i_category_id,
  i_category,
  SUM(sum_ext_sales_price) AS sum_ext_sales_price
FROM mv_ss_dd_item_mgr1_y2000_m11_fg
GROUP BY
  d_year,
  i_category_id,
  i_category
ORDER BY
  sum_ext_sales_price DESC,
  d_year,
  i_category_id,
  i_category
LIMIT 100;
```

### 7.5 q52 rewritten

```sql
SELECT
  d_year,
  i_brand_id AS brand_id,
  i_brand AS brand,
  SUM(sum_ext_sales_price) AS ext_price
FROM mv_ss_dd_item_mgr1_y2000_m11_fg
GROUP BY
  d_year,
  i_brand_id,
  i_brand
ORDER BY
  d_year,
  ext_price DESC,
  brand_id
LIMIT 100;
```

## 8. 自迭代反馈

`SelfIterationAgent` 在一轮 workflow 全部跑完后读取日志。

输入：

| 日志 | 内容 |
|---|---|
| `feature_log` | SQL 解析成功率、unsupported 原因 |
| `family_log` | family 大小、复用潜力 |
| `batch_log` | 每个 batch 执行耗时 |
| `mv_log` | MV 构建时间、大小、复用次数 |
| `rewrite_log` | rewrite 成功率、失败原因 |
| `validation_log` | 正确性验证结果 |

输出：

| 情况 | 反馈 |
|---|---|
| MV build cost 大于节省收益 | 降低该 family 优先级 |
| MV 输出太大 | 提高 size penalty |
| rewrite 因缺字段失败 | 补充 MV output column 规则 |
| 某 family 多次正收益 | 提高 family priority |
| 某 family 多次负收益 | 提高 min reuse count |
| 某类 SQL 频繁 unsupported | 增加规则或明确 fallback |

第一版反馈只影响下一轮 workflow，不在当前 batch 内动态改规则。

## 9. 开发路线图

### 阶段 1：QueryBlock 提取

交付：

```text
SQLLoaderAgent
FeatureAgent
query_blocks.json
```

验收：

```text
能解析 q42/q52
能提取 join/filter/groupby/aggregate
能给出 complexity_type
```

### 阶段 2：QueryFamily 与 Batch

交付：

```text
FamilyAgent
BatchClusterAgent
query_families.json
complexity_batches.json
```

验收：

```text
q42.outer/q52.outer 属于同一 QueryFamily
q42/q52 作为 SQL 进入 join_filter_groupby batch
```

### 阶段 3：Batch 内 MV 生成与 Rewrite

交付：

```text
BatchMVAgent
RewriteAgent
batch_mv_logs.json
rewritten SQL
```

验收：

```text
Batch-1 能生成 MV
Batch-2 能使用 Batch-1 MV rewrite
Batch-2 能基于 rewrite 后 SQL 生成新的 MV
Batch-3 能同时使用 Batch-1 和 Batch-2 MV
```

### 阶段 4：执行与验证闭环

交付：

```text
ExecutorAgent
materialized_mvs map
execution logs
validation logs
```

验收：

```text
rewritten SQL 可以执行
rewritten 结果与 original 一致
能输出 batch runtime 和 MV build cost
```

### 阶段 5：反馈闭环

交付：

```text
SelfIterationAgent
performance_report.md
feedback_rules.json
```

验收：

```text
能输出正向/负向反馈
能解释某个 MV 是否值得继续生成
```

## 10. 最小闭环

第一版只需要跑通：

```text
Input:
  q42.sql
  q52.sql
  tpcds_simple.json

Plan:
  QueryBlock
  QueryFamily
  ComplexityBatch
  query_to_qbs / qb_to_query

Batch-1:
  batch 内生成并物化 MV

Batch-2:
  用 Batch-1 MV rewrite
  基于 rewritten SQL 生成并物化 Batch-2 MV

Batch-3:
  用 Batch-1 + Batch-2 MV rewrite q42/q52
  执行 rewritten SQL

Evaluate:
  correctness
  runtime
  net speedup
```

这个版本的关键是：

```text
执行前不做全局 MV candidate
MV 只在 batch 内生成和物化
每个 batch 先复用历史 MV，再生成自己的 MV
所有成功物化的 MV 进入全局集合，供后续所有 batch 使用
```
