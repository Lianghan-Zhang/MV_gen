# 临时决策记录

本文档记录当前为了推进 MV / rewrite 测试而作出的短期决策。后续需要统一回收这些临时约束，并在 schema、validator、rules、tests 和 notebook 中做成体系化修改。

## 2026-06-07：暂时跳过缺少稳定角色 lineage 的多角色物理表 MV Candidate

### 背景

Batch-3 测试中出现以下校验错误：

```text
ValueError: Candidate cand_batch_3_family_catalog_sales_store_returns_store_sales_multi_fact_0001 mv_column d1_quarter_name must be date_dim_d_quarter_name
```

触发场景是同一个物理表在 SQL 中以多个局部 alias 参与 join，例如 `date_dim d1/d2/d3`。当前 `MVColumnMapping` 只记录 `source_table` 和 `source_column`，无法表达 sold date、returned date、catalog sold date 这类稳定的表实例业务角色。

### 临时决策

当前阶段，`BatchMVAgent` 不允许 materialize 需要 SQL alias 派生 MV 列名的 Candidate，例如 `d1_quarter_name`、`d2_quarter_name`、`cd1_marital_status`。

如果同一个物理表在一个 Candidate 中承担多个语义角色，而当前 `column_mappings` 无法稳定表达这些角色，evaluate 阶段必须将该 Candidate 改为：

```json
{
  "decision": "skip",
  "reason": "duplicate_physical_table_role_not_supported"
}
```

这是规则层短期保护，不放宽 validator 去接受 alias 派生的 MV 物理列名。

### 后续统一修改方向

- 为 `MVColumnMapping` 增加可选稳定角色字段，例如 `source_relation_role`。
- 定义基于角色的 MV 列命名，例如 `store_sales_sold_date_d_quarter_name`。
- 同步修改 `BatchMVAgent` rules、validator、tests 和 rewrite 预期。
- 持久化 MV artifact 中继续禁止泄漏 SQL 局部 alias，例如 `d1`、`d2`、`cd1`、`cd2`。

### 临时触达文件

- `llm_demo/rules/batch_mv_agent.md`

## 2026-06-12：暂时不为没有 QueryFamily 的 batch 发明 family_id

> 2026-06-13 更新：本节是旧短期策略，已被“取消 QueryFamily 作为 BatchMVAgent 的硬边界”覆盖。保留本节用于记录决策演进。

### 背景

在按 `batch_classification.csv` 重新跑 dry-run 闭环时，Batch 6 / Other 中的 q41 没有进入任何 `QueryFamily`，对应 `current_batch.family_groups` 为空。BatchMVAgent 的 LLM 输出了如下不存在的 family：

```text
family_item_q41
```

随后 validator 报错：

```text
ValueError: Candidate cand_batch_6_family_item_q41_0001 has unknown family_id family_item_q41
```

这说明 MV 生成阶段越过了 FamilyAgent / BatchClusterAgent 的边界，直接根据表名和 Query ID 臆造了 `family_id`。

### 临时决策

当前短期测试链路不引入 deterministic QueryFamily，也不允许 BatchMVAgent 为孤立 query 临时创建 family。

BatchMVAgent 只能使用 `current_batch.family_groups[].family_id` 中已经存在的 family：

- 如果 `current_batch.family_groups` 为空，当前 batch 的 MV Candidate 必须为空。
- 如果 LLM 输出了未知 `family_id`，或者输出了不属于当前 batch `family_groups` 的 Candidate，运行时先删除该 Candidate，再进入严格 validator。
- 不把未知 family 改写成 skip Candidate，因为 skip Candidate 仍然需要合法的 `family_id` 才能进入当前 schema。

该决策只影响 MV 生成阶段，不影响 RewriteAgent 对当前 batch query 的 historical / final rewrite 测试；没有新 MV 时，final rewrite 应按已有物化 MV 或原 SQL fallback 继续执行。

### 后续统一修改方向

- 如果后续确认孤立 query 也需要进入 MV 生成，需要先设计显式的 deterministic QueryFamily 规则，并由 FamilyAgent 或 FamilyCandidateBuilder 产出，而不是由 BatchMVAgent 临时发明。
- 同步修改 QueryFamily schema、BatchClusterAgent、BatchMVAgent rules / validator、coverage summary 和测试。

## 2026-06-13：取消 QueryFamily 作为 BatchMVAgent 的硬边界

### 背景

在按 `batch_classification.csv` 明确分 batch 后，当前测试目标转向 batch 内 MV 生成和 rewrite。实际运行中，大量 fallback 的主因之一是 `QueryFamily` / `family_groups` 覆盖不足，导致 supported QueryBlock 即使有可复用计算结构，也无法进入 BatchMVAgent 的候选生成范围。

### 临时决策

短期测试链路取消 `QueryFamily` 作为 BatchMVAgent 生成 MV Candidate 的硬边界：

- `BatchMVAgent` 以当前 batch 的 `query_ids` 和 supported `QueryBlock` 作为硬边界。
- `source_qb_ids` / `target_qb_ids` 必须存在于当前 batch，且不能包含 `unsupported_reasons`。
- `family_id` 改为可选 hint；没有 QueryFamily 或 `family_groups` 为空时，仍允许基于 batch-local QueryBlock 生成 MV Candidate。
- 新增 `candidate_group_id` 作为 batch-local candidate group 的可选追踪字段。
- QueryFamily 仍可作为命名、解释、优先级和去重参考，但不能阻断 Candidate 生成。

### 后续统一修改方向

- 如果 batch-local MV 生成效果明显改善，可以继续弱化或删除 FamilyAgent。
- 如果 Candidate 质量变差，应把 batch-local candidate group 的生成规则进一步 deterministic 化，并增加 coverage / rejection artifact。
- 长期 schema 可以考虑正式移除 `family_id` 必填语义，改为以 `source_qb_ids` / `target_qb_ids` 和 `candidate_group_id` 表达 MV 边界。

### 临时触达文件

- `llm_demo/src/agents/batch_mv_agent.py`
- `llm_demo/rules/batch_mv_agent.md`

## 2026-06-12：RewriteAgent 输出必须收敛到 current batch

### 背景

全量 notebook 在 `BatchWorkflowRunner.run_all_batches(...)` 的 batch final rewrite 阶段出现：

```text
ValueError: Rewrite output query_ids {..., 'q42', 'q52', ...} do not match expected {...}
```

具体表现是当前 batch 的 `final rewrite` 输出中混入了不属于当前 batch 的 `q42`、`q52`。这两个 query 来自 `Batch-1`，但在后续 batch 的 rewrite 输出中被 LLM 从历史 MV provenance、全量 QueryBlock 或规则示例中带了回来。

### 临时决策

当前阶段，`RewriteAgent` 的输出范围必须严格等于 `current_batch.query_ids`：

- `materialized_mvs.source_query_ids`、`target_queries` 和规则示例中的 query 只能作为上下文或 provenance，不能成为当前 rewrite target。
- 传给 LLM 的 QueryBlock 只保留当前 batch 的 QueryBlock，减少跨 batch query 泄漏。
- 代码层在 validate 前做 scope normalize：丢弃 current batch 之外的 rewrite record；如果 current batch query 缺失，则补 original-equivalent fallback。
- 自动补齐的 fallback 使用 `fallback_reason = "rewrite_output_missing_query"`。

### 后续统一修改方向

- 把 RewriteAgent 的输入 artifact 拆成 current batch context 与 historical MV provenance，避免 LLM 混淆。
- 为 scope normalize 增加更完整的测试，包括 extra query、missing query 和 duplicate query。
- 如果后续需要断点恢复 batch workflow，应设计明确的 batch resume 入口，而不是复用半完成 run 的混合状态。

### 临时触达文件

- `llm_demo/rules/rewrite_agent.md`
- `llm_demo/src/agents/rewrite_agent.py`

## 2026-06-07：暂时强制 measure MV 列使用确定性物理字段聚合别名

### 背景

BatchMVAgent evaluate 阶段出现以下 measure 列命名校验错误：

```text
ValueError: Candidate cand_batch_2_family_store_sales_date_dim_single_0001 measure mv_column sum_sun_sales must be sum_ss_sales_price
```

这说明 LLM 已经识别出 measure 来源字段是 `store_sales.ss_sales_price`，但把 MV 物理列名写成了原查询或语义层别名 `sum_sun_sales`。当前 validator 要求 `role = "measure"` 的 `mv_column` 严格由 `source_expr` 的聚合函数和 `source_column` 生成，例如 `SUM(store_sales.ss_sales_price)` 必须对应 `sum_ss_sales_price`。

### 临时决策

当前阶段，measure MV 物理列名不使用原查询输出别名、语义别名或展示别名，只使用确定性物理字段聚合别名：

```text
{agg_func_lower}_{source_column_lower}
```

例如：

```text
SUM(store_sales.ss_sales_price) -> sum_ss_sales_price
SUM(store_sales.ss_ext_sales_price) -> sum_ss_ext_sales_price
```

同一个 measure 在以下位置必须完全一致：

- `build_sql` SELECT alias
- `output_columns[]`
- `column_mappings[].mv_column`

原查询输出别名应由 RewriteAgent 在最终 rewritten SQL 的 projection 中恢复，不写入持久化 MV artifact 的物理列名。

如果 measure 的 `source_expr` 无法表达为一个聚合函数作用于单一物理源字段，则当前阶段不编造稳定别名，直接输出：

```json
{
  "decision": "skip",
  "reason": "measure_source_lineage_not_supported",
  "column_mappings": [],
  "output_columns": [],
  "build_sql": null,
  "mv_id": null,
  "target_table_name": null
}
```

复杂派生 measure 也可以继续使用：

```text
derived_expression_column_mapping_not_supported
```

### 后续统一修改方向

- 设计显式的 measure 语义别名映射，让 MV 物理列名和最终查询输出别名分层记录。
- RewriteAgent 使用映射恢复原查询输出 schema，而不是要求 MV 物理列名等于原查询输出别名。
- 同步修改 BatchMVAgent rules、validator、materialized state、RewriteAgent 和测试。

### 临时触达文件

- `llm_demo/rules/batch_mv_agent.md`

## 2026-06-07：暂时跳过使用通配符字段 lineage 的 MV Candidate

### 背景

BatchMVAgent evaluate 阶段出现以下物理 schema 校验错误：

```text
ValueError: Unknown physical column: store_sales.*
```

这说明 LLM 在 `column_mappings[].source_column` 中使用了 `*`，试图用 `store_sales.*` 表示整表或整行字段来源。当前 `MVColumnMapping` 的语义是一个 MV 输出列对应一个可验证的物理源字段，`store_sales.*` 不是物理字段，也无法支持后续 RewriteAgent 做列级覆盖判断。

### 临时决策

当前阶段，`BatchMVAgent` 不支持 wildcard lineage，不允许在以下位置使用 `*`、`table.*` 或 `alias.*`：

- `column_mappings[].source_column`
- `column_mappings[].source_expr`
- `output_columns[]`
- `build_sql` 的 SELECT 列表

即使 Candidate 是 `detail_superset`，也必须显式列出 rewrite 所需字段和对应 `column_mappings`，不能用通配符代表整行或整张表。

如果某个 Candidate 需要整行、整表或 wildcard lineage 才能表达，则第一次生成阶段或 evaluate 阶段必须将其改为：

```json
{
  "decision": "skip",
  "reason": "wildcard_column_mapping_not_supported",
  "column_mappings": [],
  "output_columns": [],
  "build_sql": null,
  "mv_id": null,
  "target_table_name": null
}
```

如果已确认是 `detail_superset` 依赖通配符，也可以使用：

```text
detail_superset_wildcard_not_supported
```

不要自动展开整表字段，也不要把本应显式列级追踪的 Candidate 变成宽表 materialize。

### 后续统一修改方向

- 如果后续确实需要支持整行或宽表 MV，先设计表级 lineage 或显式字段展开策略。
- 明确宽表 MV 的字段裁剪规则，避免默认 materialize 整张事实表。
- 同步修改 `MVColumnMapping` schema、BatchMVAgent validator、Executor materialized state、RewriteAgent 列覆盖判断和测试。

### 临时触达文件

- `llm_demo/rules/batch_mv_agent.md`

## 2026-06-07：暂时跳过无法映射到单一物理字段的派生输出列

### 背景

BatchMVAgent 第一阶段生成 `candidate_mv_output` 时出现 Pydantic schema 校验错误：

```text
ValidationError: mv_candidates.1.column_mappings.4.source_table
Input should be a valid string, input_value=None

ValidationError: mv_candidates.1.column_mappings.4.source_column
Input should be a valid string, input_value=None
```

这说明 LLM 试图 materialize 某个输出列，但该列无法映射到单一物理表字段，于是把 `column_mappings[].source_table` 和 `source_column` 写成了 `null`。当前 `MVColumnMapping` schema 不支持这种表达，后续 validator / Executor / RewriteAgent 也依赖完整的物理字段 lineage。

### 临时决策

当前阶段，`BatchMVAgent` 不允许在 `column_mappings` 中输出 `null`、空字符串或占位符来源。

如果某个 Candidate 需要输出 ratio、CASE、多列组合表达式、常量表达式、复杂派生表达式，且当前 schema 无法稳定记录它的 `source_table.source_column` lineage，则第一次生成阶段就必须直接输出：

```json
{
  "decision": "skip",
  "reason": "column_mapping_source_not_supported",
  "column_mappings": [],
  "output_columns": [],
  "build_sql": null,
  "mv_id": null,
  "target_table_name": null
}
```

如果已确认是复杂派生表达式导致，也可以使用：

```text
derived_expression_column_mapping_not_supported
```

不要生成半残缺的 `column_mappings`，也不要把无法追溯来源的派生列强行写成 materialized MV 输出。

### 后续统一修改方向

- 设计更丰富的表达式 lineage schema，用于表达 ratio、CASE、多列组合表达式、常量表达式和复杂派生 measure。
- 明确哪些派生表达式可以作为 MV 物理列持久化，哪些只能在 rewrite 阶段重新计算。
- 同步修改 `MVColumnMapping` schema、BatchMVAgent validator、rules、tests、Executor materialized state 和 RewriteAgent 的列覆盖判断。

### 临时触达文件

- `llm_demo/rules/batch_mv_agent.md`
