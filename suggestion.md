# BatchMV / Rewrite 覆盖率问题排查与修改建议

## 0. 诊断范围

本次只分析当前已落盘 run，不修改代码。

证据来源：

- `llm_demo/artifacts/20260612_193446/08_coverage/coverage_summary.json`
- `llm_demo/artifacts/20260612_193446/02_families/family_candidates.json`
- `llm_demo/artifacts/20260612_193446/02_families/query_families.json`
- `llm_demo/artifacts/20260612_193446/03_batches/complexity_batches.json`
- `llm_demo/artifacts/20260612_193446/04_batch_mvs/batch_*_mv_candidates.json`
- `llm_demo/artifacts/20260612_193446/04_batch_mvs/materialized_mvs.json`
- `llm_demo/artifacts/20260612_193446/05_rewritten_sql/batch_*/final_rewrite/*_rewrite_meta.json`
- 当前代码：`llm_demo/src/agents/batch_mv_agent.py`、`llm_demo/src/agents/rewrite_agent.py`

当前覆盖摘要：

```text
total_queries: 105
feature_status_counts: success 79, partial_success 26
family_candidate_count: 97
family_count: 34
mv_candidate_counts: materialize 16, skip 14
rewrite_status_counts: final:rewritten 13, final:fallback 91, historical:fallback 105
execution_step_counts: materialize_mv:success 16, materialize_mv:skipped 14, run_query:planned 104
```

其中 91 个 final fallback 的原因是：

```text
82 no_matching_mv
5  unsupported_sql_pattern
3  mv_unknown_column
1  group_by_not_compatible
```

## 1. 当前 BatchMVAgent 的 MV 生成逻辑

当前 `BatchMVAgent` 不是规则程序自动枚举所有可能 MV，而是以下链路：

1. 读取当前 batch、当前 batch 的 QueryBlock、当前 batch 的 QueryFamily、historical rewrite、已有 `materialized_mvs`。
2. 只把 `current_batch.family_groups` 中出现的 family 传给 LLM。
3. 第一次 LLM 生成 `candidate_mv_output`。
4. 第二次 LLM evaluate，修正 batch 边界、family 边界、MV 类型、predicate、依赖和 build SQL。
5. Python validator 强校验：
   - candidate 必须属于当前 batch。
   - `family_id` 必须来自已有 `query_families`，且属于当前 batch 的 `family_groups`。
   - `source_qb_ids` / `target_qb_ids` 必须属于 candidate family，且不能引用 unsupported QueryBlock。
   - materialize candidate 必须有 `mv_id`、`target_table_name`、`build_sql`。
   - `column_mappings` 必须完整、非空、可追溯到物理表字段。
   - `output_columns` 必须是 MV 物理列名，不能是 `table.column`。
   - measure 列名必须等于稳定规则，例如 `sum_ss_ext_sales_price`。
   - CTAS 必须是 `CREATE TABLE ... AS SELECT ...`。
6. 落盘：
   - `04_batch_mvs/batch_N_mv_candidates.json`
   - `04_batch_mvs/batch_N_mv_build.sql`

因此，BatchMVAgent 的候选空间强依赖上游：

```text
FamilyCandidateBuilder -> FamilyAgent -> BatchClusterAgent.family_groups -> BatchMVAgent
```

如果某个 query 的 QueryBlock 没进入最终 `query_families.json`，或者没有被 BatchClusterAgent 放进当前 batch 的 `family_groups`，BatchMVAgent 基本不会为它生成 MV candidate。

## 2. no_matching_mv 的四类来源

82 个 `no_matching_mv` 按当前 artifact 可分为四类：

```text
64 no_family_group
11 only_skipped_candidates
4  has_materialized_family_but_not_matched
3  family_group_but_no_candidate
```

这说明主要瓶颈不是 Executor，也不只是 BatchMVAgent skip，而是：

```text
大量 query 在进入 BatchMVAgent 前，没有可用 family_group。
```

### 2.1 no_family_group：64 个

代表 query：

```text
q11,q15,q16,q18,q19,q2,q21,q23a,q23b,q24a,q24b,q30,q31,q32,q33,q34,q37,q38,q39a,q39b,q4,q40,q43,q45,q46,q49,q51,q54,q56,q57,q59,q6,q60,q62,q64,q65,q66,q68,q70,q71,q72,q73,q74,q75,q76,q77,q78,q79,q80,q81,q82,q84,q85,q87,q88,q90,q91,q92,q93,q94,q96,q97,q98,q99
```

这类 query 的共同点：

- `family_candidates.json` 往往已有候选。
- 但最终 `query_families.json` 没有接收这些候选。
- 因此 `complexity_batches.json` 的 `family_groups` 里也没有它们。
- BatchMVAgent 当前只看 `family_groups`，所以无法为这些 query 生成 MV candidate。

这类是当前最大问题，优先级最高。

## 3. 高价值 no_family_group query 排查

### 3.1 q24a / q24b

现象：

- `family_candidates.json` 中有 `candidate_family_0029`。
- 类型：`fact_based`。
- 成员：
  - `q24a.cte_1`
  - `q24b.cte_1`
- join domain：
  - `store_sales`
  - `store_returns`
  - `store`
  - `item`
  - `customer`
  - `customer_address`
- unsupported 数量为 0。
- 最终 `query_families.json` 没有对应 family。
- batch 3 的 `family_groups` 没有 q24a/q24b。
- final rewrite 结果：`no_matching_mv`。

判断：

这是高价值缺口。q24a/q24b 的 CTE 结构本身已经被候选构建器识别，而且两个 query 共享同构的多 fact CTE 聚合边界。当前失败点更像是 FamilyAgent evaluate 阶段过度保守，没有把多 fact CTE candidate promote 成 QueryFamily。

建议修改：

1. FamilyAgent 规则增加：supported CTE QueryBlock 可以作为 QueryFamily 成员和 MV 边界，不能因为 outer query 复杂就丢弃 CTE 候选。
2. 对多 fact CTE 增加显式安全门：
   - `core_fact_table_set` 完全相同。
   - join_signature 完全相同或可证明等价。
   - group_by / measure 可形成 shared upstream superset。
   - 所有成员 unsupported 为空。
3. BatchClusterAgent 应把 promoted CTE family 写入 batch 的 `family_groups`。
4. RewriteAgent 后续需要支持用 CTE-level MV 替换 CTE 定义，类似 q1 的 `customer_total_return` 思路。

建议优先测试：

```text
q24a.cte_1 + q24b.cte_1 -> QueryFamily -> BatchMV candidate -> CTE-level rewrite
```

### 3.2 q34 / q73 / q79

现象：

- `family_candidates.json` 中有两个相关候选：
  - `candidate_family_0040`：dimension_only，成员是 `q34.outer`、`q73.outer`、`q79.outer`。
  - `candidate_family_0041`：fact_based，成员是 `q34.subquery_1`、`q73.subquery_1`、`q79.subquery_1`。
- fact_based candidate 的 join domain：
  - `store_sales`
  - `date_dim`
  - `store`
  - `household_demographics`
- unsupported 数量为 0。
- 最终 `query_families.json` 没有相关 family。
- batch 2 没有这些 query 的 family_group。
- final rewrite：`no_matching_mv`。

判断：

这是高价值缺口。outer block 是 customer 过滤壳，真正可复用的计算在 subquery block。当前 FamilyAgent 没有把 subquery-level fact candidate promote 成 QueryFamily，导致 BatchMVAgent 无法生成针对 subquery 的 MV。

建议修改：

1. FamilyAgent 对 correlated / IN / EXISTS 型查询，应优先识别可复用的 fact subquery block，而不是只看 outer block。
2. 对 q34/q73/q79 这类模式，允许输出 QueryFamily：
   - 成员：`q34.subquery_1`、`q73.subquery_1`、`q79.subquery_1`
   - 类型：fact subquery family
   - 语义：store_sales 上的 grouped customer/ticket/count 或相关度量中间结果。
3. BatchMVAgent 生成的是 subquery-local MV，不要求一次覆盖完整 outer query。
4. RewriteAgent 后续要支持 subquery replacement：outer SQL 保持，内部 subquery 替换为 MV 或从 MV 聚合。

### 3.3 q46 / q68

现象：

- `family_candidates.json` 中有：
  - `candidate_family_0050`：dimension_only，成员包含 q46/q68 outer。
  - `candidate_family_0051`：fact_based，成员是 `q46.subquery_1`、`q68.subquery_1`。
- fact_based candidate 的 join domain：
  - `store_sales`
  - `customer_address`
  - `date_dim`
  - `household_demographics`
  - `store`
- unsupported 数量为 0。
- 最终没有 QueryFamily。
- batch 2 没有 q46/q68 的 family_group。
- final rewrite：`no_matching_mv`。

判断：

和 q34/q73/q79 类似，这是 subquery-level family 没有被 promote。q46/q68 的 outer 负责 customer/address 过滤，subquery 负责可复用事实计算。

建议修改：

1. FamilyAgent 增加规则：如果 outer block 与 subquery block 都有候选，优先保留 fact_based subquery family。
2. 不要因为 dimension outer predicate 不完全共同而丢弃 fact subquery family。
3. BatchClusterAgent 应把 `q46.subquery_1`、`q68.subquery_1` 对应 family_group 写入 Batch-2。
4. RewriteAgent 增强 subquery-local rewrite。

### 3.4 q90

现象：

- `q90.outer` 有 unsupported：
  - 外层 SELECT 表达式引用子查询输出列 `amc`、`pmc`。
  - ORDER BY 引用输出别名 `am_pm_ratio`。
- 但 `q90.subquery_1` 和 `q90.subquery_2` 都是 supported。
- `family_candidates.json` 中有 `candidate_family_0099`：
  - 类型：fact_based。
  - 成员：`q90.subquery_1`、`q90.subquery_2`。
  - join domain：`web_sales + household_demographics + time_dim + web_page`。
  - measure：`count(*)`。
  - unsupported 数量为 0。
- 最终没有 QueryFamily。
- batch 2 没有 family_group。
- final rewrite：`no_matching_mv`。

判断：

这是典型的“outer unsupported 不应阻止 supported subquery 进入 MV 测试”的案例。当前 FamilyAgent 或 evaluate 可能因为整条 SQL 外层复杂而丢弃了内部可复用 QueryBlock。

建议修改：

1. Feature/Family contract 明确：outer unsupported 不等于整条 query 的所有 QueryBlock 都不可用于 MV。
2. FamilyAgent 应允许 supported subquery 成为 QueryFamily，即使同 query 的 outer block unsupported。
3. RewriteAgent 后续可以先只替换两个 count subquery，再在 outer 层保留 ratio 计算和 ORDER BY。

### 3.5 q72 / q85 / q81 / q82 / q84 / q91

现象：

这些 query 在 `family_candidates.json` 中都有单 query fact_based candidate，unsupported 数量为 0，但最终没有 QueryFamily，也没有 family_group：

```text
q72 -> candidate_family_0077
q85 -> candidate_family_0096
q81 -> candidate_family_0090
q82 -> candidate_family_0091
q84 -> candidate_family_0095
q91 -> candidate_family_0100
```

判断：

当前 FamilyAgent 可能倾向于保留多 query family，而对单 query fact candidate 过滤较多。但在当前测试目标中，单 query MV 仍然有价值，因为你正在测试 batch 内 MV 生成和 rewrite 能力，不只是跨 query 共享。

建议修改：

1. 给 FamilyAgent 增加配置或规则：在测试链路中，supported 单 query fact candidate 也可以成为 QueryFamily。
2. FamilyAgent 输出中标注或命名为 single-query family，例如：
   - `family_fact_cr_dd_q81_single`
   - `family_fact_ws_wr_q85_single`
3. BatchMVAgent 可根据单 query family 生成精确 MV，用于验证 rewrite 能力。
4. 如果未来正式生产链路不希望单 query MV 泛滥，可以在 Executor 或 policy 层筛选，而不是在 FamilyAgent 阶段丢弃。

## 4. only_skipped_candidates：BatchMVAgent 整个 family 被 skip

代表 query：

```text
q01,q10,q25,q28,q29,q35,q53,q58,q63,q89,q9
```

### 4.1 q53 / q63 / q89

现象：

- family：`family_fact_ss_dd_item_store_month_seq`
- 成员：`q53.subquery_1`、`q63.subquery_1`、`q89.subquery_1`
- BatchMVAgent skip 原因：
  - q89 缺少 `d_month_seq` filter。
  - 三者没有所有成员共同的 predicate shape。

判断：

这不是必须整体 skip。q53 和 q63 可能可以形成一个子集 MV，q89 单独处理。当前 BatchMVAgent 把 family 当成不可拆整体，导致一个成员破坏整个 family。

建议修改：

1. BatchMVAgent 支持 family 内 target subset：
   - 对同一 family，可以生成覆盖 `q53,q63` 的 candidate。
   - q89 可单独 skip 或生成更宽 MV。
2. rules 增加：当 family 中部分成员不兼容时，优先尝试最大安全子集，而不是直接 skip 整个 family。
3. run log 记录被排除成员和排除原因。

### 4.2 q25 / q29 被 q17 拖累

现象：

- family：`family_fact_ss_dd_item_store_ss_sold_date_sk.gt`
- 成员：`q17,q25,q29`
- skip 原因：
  - 多 date_dim 角色，当前 `column_mappings` 无法区分 sold_date / returned_date / catalog_sold_date。
  - q17 包含 `avg`、`stddev_samp` 等非可加聚合。

判断：

q17 确实复杂，但 q25/q29 未必应和 q17 一起整体 skip。这里需要 family 内拆分和 role-aware lineage 两个方向。

建议修改：

1. FamilyAgent 或 BatchMVAgent 在 evaluate 阶段拆出 q25/q29 子集。
2. 对多 date_dim 角色，后续设计 role-aware column lineage：
   - 记录 `source_relation_instance_id`。
   - MV 列名允许带稳定业务角色，例如 `sold_date_d_year`、`returned_date_d_year`。
3. 在 schema 扩展前，q17 可以继续 skip，q25/q29 不应被连带 skip。

### 4.3 q10 / q35、q58、q9、q28

判断：

这些 skip 大多是合理保守：

- q10/q35 涉及 correlated EXISTS 和多 fact 条件。
- q58 涉及 date_week 动态子查询 predicate。
- q9/q28 是多 bucket 子查询，不同范围和 CASE 输出较多。

建议：

短期不优先放开。它们可以作为后续复杂 SQL 能力扩展目标，不应先于 q24a/q24b、q34/q73/q79、q46/q68、q90。

## 5. family_group_but_no_candidate：有 family_group 但 BatchMVAgent 没产出 candidate

代表 query：

```text
q5,q69,q8
```

另有 q14a 的多个 branch family 在 batch 5 里也没有 candidate：

```text
family_fact_cs_dd_item_branch8
family_fact_ss_dd_item_substr_item_desc_count
family_fact_ws_dd_item_branch9
```

现象：

- `complexity_batches.json` 中有 family_group。
- `batch_5_mv_candidates.json` 没有对应 family_id 的 materialize 或 skip candidate。
- 这意味着 BatchMVAgent evaluate 输出没有覆盖所有 family_group。

判断：

这是可观测性和契约问题。当前 schema 允许 BatchMVAgent 只输出它选择处理的 candidates，但没有要求“每个 family_group 都必须有 materialize 或 skip 记录”。这会让覆盖缺口变成静默遗漏。

建议修改：

1. BatchMVAgent 输出契约增加覆盖要求：
   - 当前 batch 的每个非空 family_group 必须至少有一个 candidate。
   - 如果不生成 MV，也必须输出 `decision = "skip"` 和明确 reason。
2. validator 增加 coverage 检查：
   - `current_batch.family_groups[].family_id - output.mv_candidates[].family_id` 不应静默为空。
   - 对允许遗漏的 family，需要写入 `omitted_family_groups` 或 run log。
3. Notebook summary 增加：
   - family_groups_total
   - family_groups_with_candidate
   - family_groups_omitted

## 6. has_materialized_family_but_not_matched：已有 MV 但 final rewrite 没匹配

代表 query：

```text
q14a,q14b,q44,q67
```

### 6.1 q14a / q14b

现象：

- 已有 MV：`mv_cs_dd_item_year_q14a_q14b_ds`
- target_queries：`q14a,q14b`
- target_qb_ids：`q14a.branch_2,q14b.branch_2`
- final rewrite 仍然 `no_matching_mv`。
- rewrite meta 中 `target_qb_ids` 为空，`used_mv_ids` 为空。

判断：

MV 是 branch-local，不是 whole-query MV。RewriteAgent 当前没有证明用该 MV 替换 branch 后整条 q14a/q14b 仍安全，因此回退到 `no_matching_mv`。

建议修改：

1. RewriteAgent 支持 branch-local rewrite。
2. `materialized_mvs` 中的 `target_qb_ids` 应作为候选匹配入口，而不只是整 query target。
3. RewriteOutput 应能表达：
   - `rewrite_mode = "query_block_replacement"` 或类似模式。
   - `target_qb_ids = ["q14a.branch_2"]`。
   - outer query 保持原结构，只替换该 branch 的 FROM / subquery 来源。

### 6.2 q44

现象：

- 已有 MV：`mv_ss_store_filter_q44_ds`
- target_qb_ids：`q44.subquery_4,q44.subquery_8`
- final rewrite 仍然 `no_matching_mv`。

判断：

q44 是多 subquery / ranking 结构。当前 MV 只覆盖两个 store_sales filter subquery，RewriteAgent 没有局部替换能力或没有足够语义证明。

建议修改：

1. RewriteAgent 支持同一 query 内多个 subquery 的局部替换。
2. 对每个 `target_qb_id` 独立判断 MV 是否覆盖，而不是要求一个 MV 覆盖 whole query。
3. 如果只能替换一部分 subquery，RewriteOutput 应记录 partial rewrite 的 target_qb_ids 和保留的原 SQL 片段。

### 6.3 q67

现象：

- q67 与 q36/q47 在同一个 family_group：`family_fact_ss_dd_item_store_month_seq`。
- materialized MV 只 target q36/q47，没有 target q67。
- q67 的关键 measure 是：
  - `SUM(COALESCE(store_sales.ss_sales_price * store_sales.ss_quantity, 0))`
- 当前 `MVColumnMapping` 只能稳定表达单一物理字段 lineage。

判断：

q67 被排除是当前 schema 的合理限制。它需要派生 measure lineage，不能强行映射到单一 `source_table.source_column`。

建议修改：

1. 短期：BatchMVAgent 应为 q67 输出 skip candidate，reason 写 `derived_measure_lineage_not_supported`，避免静默遗漏。
2. 长期：扩展 `MVColumnMapping`：
   - 支持 expression lineage。
   - 支持多源字段表达式。
   - 支持 `COALESCE(a * b, 0)` 这类派生 measure。

## 7. 非 no_matching_mv 但必须关注的质量问题

### 7.1 q1: mv_unknown_column

现象：

- 已有 MV：`mv_sr_dd_year_q1_cte1_fg`
- MV 输出列：
  - `sr_customer_sk`
  - `sr_store_sk`
  - `sum_sr_return_amt`
- q1 原始 CTE 输出别名：
  - `ctr_customer_sk`
  - `ctr_store_sk`
  - `ctr_total_return`
- final rewrite 被 safety fallback 为 `mv_unknown_column`。

判断：

这是 CTE 输出别名和 MV 物理列名之间缺少映射。当前 MV 列名采用物理 lineage 命名，但 RewriteAgent 替换 CTE 时需要恢复 CTE 对外 schema。

建议修改：

1. materialized MV artifact 增加 logical output mapping：
   - query_id
   - target_qb_id
   - original_output_name / alias
   - mv_column
2. RewriteAgent 替换 CTE 时使用 `SELECT mv_col AS original_alias` 包装 MV。
3. 不要把 MV 物理列名改成 CTE alias；物理列名和 query 输出 schema 应分层。

### 7.2 q27: group_by_not_compatible

现象：

- q27 使用 `GROUP BY ROLLUP(i_item_id, s_state)` 和 `grouping(s_state)`。
- MV `mv_ss_cd_store_item_q27_fg` 只保存普通 group-by 粒度。
- final fallback：`group_by_not_compatible`。

判断：

当前 MV 无法产生 ROLLUP 总计行，也无法正确表达 `grouping()` 结果。这是 MV 形态不适配 SQL 语义。

建议修改：

1. BatchMVAgent 识别 `ROLLUP` / `GROUPING SETS`。
2. 如果要 materialize，MV 需要保存 grouping set 层级，或保留 detail grain 供 final rewrite 重新 rollup。
3. 在未支持前，BatchMVAgent 应直接 skip 这类 candidate，避免 materialize 后再 fallback。

### 7.3 q36 / q47: mv_unknown_column

现象：

- 已有 MV：`mv_ss_dd_item_store_month_seq_q36_q47_fg`
- q36/q47 涉及 ROLLUP、window、ratio、lag/lead 风格计算。
- final fallback：`mv_unknown_column`。

判断：

这类 query 的 rewrite 不只是简单 filter/projection/rollup，还要恢复 window / rank / ratio 的中间列语义。当前 MV 输出列不足以直接支撑 RewriteAgent 生成安全 SQL，或者 RewriteAgent 生成了不在 MV output_columns 内的列。

建议修改：

1. 短期：BatchMVAgent 对 window / rollup / ratio 组合 query 更保守，先 skip 或只生成局部可证明 MV。
2. 中期：RewriteAgent 支持基于 MV 的二阶段 SQL：
   - inner 从 MV 聚合。
   - outer 计算 ratio/window/rank。
3. 长期：FeatureAgent / QueryBlock schema 显式记录 window_exprs、rollup_exprs、derived_output_exprs，供 BatchMVAgent 和 RewriteAgent 使用。

## 8. 建议的修改顺序

### P0：先补诊断可观测性

目标：以后不要只看到 `no_matching_mv`，要知道断在哪一层。

建议增加以下 artifact 或 summary 字段：

```text
family_candidate_to_query_family_coverage.json
batch_family_group_coverage.json
mv_candidate_family_group_coverage.json
rewrite_mv_match_coverage.json
```

最小字段：

```text
query_id
qb_id
candidate_family_id
final_family_id
batch_id
family_group_id
candidate_id
mv_id
rewrite_status
fallback_reason
missing_stage
```

这样可以直接判断：

```text
candidate 存在但没进 QueryFamily
QueryFamily 存在但没进 family_group
family_group 存在但没 candidate
candidate materialized 但 RewriteAgent 没匹配
```

### P1：修 FamilyAgent promote 缺口

优先样本：

```text
q24a/q24b
q34/q73/q79
q46/q68
q90
q72/q85/q81/q82/q84/q91
```

修改目标：

1. supported CTE / subquery QueryBlock 可以成为 QueryFamily。
2. outer unsupported 不阻止内部 supported QueryBlock 进入 QueryFamily。
3. 单 query fact candidate 在测试链路中也可以成为 QueryFamily。
4. 多 fact CTE 只要 core_fact_table_set、join_signature、measure/group-by 可证明兼容，就可以 promote。

验收标准：

```text
上述 query 的关键 QueryBlock 在 query_families.json 中出现。
complexity_batches.json 的 family_groups 包含这些 QueryBlock。
BatchMVAgent 能看到它们并生成 materialize 或 skip candidate。
```

### P2：修 BatchMVAgent family 内子集与遗漏问题

优先样本：

```text
q53/q63/q89
q25/q29/q17
q5/q69/q8
q14a branch family
q67
```

修改目标：

1. 一个 family 内部分成员不兼容时，优先生成最大安全子集 candidate。
2. 每个 family_group 至少输出一个 materialize 或 skip candidate。
3. 对未覆盖成员显式写 reason。
4. 对派生 measure / 多角色表 / ROLLUP 等暂不支持场景，输出明确 skip reason，不静默遗漏。

验收标准：

```text
batch_*_mv_candidates.json 中每个 family_group 都有记录。
q53/q63 能独立于 q89 生成候选或明确 skip。
q25/q29 不被 q17 连带整体 skip，除非有单独 reason。
q67 有明确 derived_measure_lineage_not_supported。
```

### P3：修 RewriteAgent 局部 QueryBlock 替换能力

优先样本：

```text
q14a/q14b
q44
q90
q24a/q24b
q34/q73/q79
q46/q68
```

修改目标：

1. 支持 `target_qb_ids` 驱动的 QueryBlock-local rewrite。
2. 支持替换 CTE、subquery、set branch。
3. 支持 MV 物理列到原 QueryBlock 输出别名的映射。
4. 对局部 rewrite 生成的 SQL 保留 outer query 的 ORDER BY / LIMIT / alias schema。

验收标准：

```text
q14a/q14b 不再因为已有 branch MV 仍 no_matching_mv。
q44 能至少替换 q44.subquery_4/q44.subquery_8。
q1 CTE 替换不再 mv_unknown_column。
```

### P4：扩展 lineage / 复杂 SQL 表达能力

优先样本：

```text
q1
q27
q36/q47
q67
q17
```

修改目标：

1. 增加 logical output alias mapping，解决 CTE 输出别名。
2. 增加 role-aware lineage，解决多 date_dim / 多 customer_demographics 角色。
3. 增加 derived expression lineage，解决 `SUM(COALESCE(a*b,0))`。
4. 增加 ROLLUP / GROUPING SETS / grouping() 语义记录。
5. 增加 window_exprs / rank / lag-lead 支持。

验收标准：

```text
q1 不再 mv_unknown_column。
q27 在未支持 ROLLUP 前应由 BatchMVAgent skip，而不是 materialize 后 fallback。
q36/q47 要么可安全 rewrite，要么明确被 skip。
q67 有明确派生 measure lineage 策略。
```

## 9. 建议先改哪些文件

第一轮建议只改规则和诊断，不直接大改 schema：

```text
llm_demo/rules/family_agent.md
llm_demo/rules/batch_mv_agent.md
llm_demo/rules/rewrite_agent.md
llm_demo/src/core/coverage_summary_builder.py
```

第二轮再改 agent 行为：

```text
llm_demo/src/agents/family_agent.py
llm_demo/src/agents/batch_cluster_agent.py
llm_demo/src/agents/batch_mv_agent.py
llm_demo/src/agents/rewrite_agent.py
```

第三轮再考虑 schema 扩展：

```text
llm_demo/src/core/schemas.py
```

不建议一开始就扩展 schema，因为当前最大缺口是 QueryFamily / family_group 没传到 BatchMVAgent，而不是所有问题都来自 schema 表达能力。

## 10. 最小可验证修改路线

建议按以下顺序推进：

1. 先加 coverage 诊断 artifact，固定当前四类缺口统计。
2. 修 FamilyAgent，使 q24a/q24b、q34/q73/q79、q46/q68、q90 的候选进入 QueryFamily。
3. 跑新 run，确认 `no_family_group` 数量显著下降。
4. 修 BatchMVAgent 的 family 内子集 candidate 和 family_group 覆盖要求。
5. 跑新 run，确认 `only_skipped_candidates` 和 `family_group_but_no_candidate` 下降。
6. 修 RewriteAgent 的 QueryBlock-local rewrite。
7. 跑新 run，确认 q14a/q14b/q44/q90 等从 `no_matching_mv` 转为 rewritten 或更精确的 fallback。
8. 最后处理 q1/q27/q36/q47/q67 这类 lineage / ROLLUP / window 问题。

## 11. 总结判断

当前 rewrite 覆盖率低的主因不是单一的 BatchMVAgent 生成少，而是以下链条共同造成：

```text
FamilyCandidate 已有候选
-> FamilyAgent 未 promote 成 QueryFamily
-> BatchCluster 无 family_group
-> BatchMVAgent 看不到候选空间
-> RewriteAgent no_matching_mv
```

其次是 BatchMVAgent 对整个 family 过度保守 skip，没有尝试 family 内安全子集。

第三是 RewriteAgent 对已有局部 MV 的替换能力不足，导致 q14a/q14b/q44 这类已有 materialized MV 的 query 仍然 `no_matching_mv`。

因此下一步最有效的修改不是直接“让 BatchMVAgent 多生成 MV”，而是：

```text
先扩大并稳定传入 BatchMVAgent 的 QueryFamily / family_group，
再要求 BatchMVAgent 对每个 family_group 给出 materialize 或 skip 结论，
最后增强 RewriteAgent 使用局部 QueryBlock MV 的能力。
```
