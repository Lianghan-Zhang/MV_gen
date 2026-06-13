# ETL MV Agent-Only 全量 SQL 收敛版实现清单

> 来源文档：`etl_agent_only_all_sql.md` v1.51-all-sql.35  
> 日期：2026-06-02  
> 目标：把全量 `tpcds-spark/` 原型从设计收敛为可开发、可测试、可复盘的第一轮实现任务。

## 1. 本轮目标

第一轮只追求三件事：

1. 直接读取完整 `tpcds-spark/`，完成全量 SQL intake。
2. 串起 Agent-only dry-run 闭环，产出稳定 artifact 和日志。
3. 通过 `coverage_summary.json` 和 `feedback_rules_{run_id}.json` 观察失败分布、fallback 原因和规则改进方向。

第一轮不追求高 MV 命中率、高 rewrite 成功率或真实 Spark 加速收益。

## 2. 硬约束

- [ ] 每个 `run_id` 是一次孤立实验，不做跨 run cache、artifact 复用、prompt hash 或 SQL hash。
- [ ] 同一 `run_id` 内只允许 Feature intake resume；Family / Batch / MV / Rewrite / Execution 不做断点续跑。
- [ ] Feature intake 完全串行，按 `sql_manifest.queries` 顺序逐条调用 LLM，不设计并发参数。
- [ ] Feature retry 只做一次简单重试，不把上一轮错误上下文注入 prompt。
- [ ] 单条 SQL 不做 SQLGlot 预拆分，不做 chunk prompt；复杂结构通过 `partial_success` 和 `unsupported_reasons` 表达。
- [ ] 所有 artifact 写入 `llm_demo/artifacts/{run_id}/`。
- [ ] 不新增 Agent IO 状态对象；Agent、Runner、notebook 之间只传显式 artifact path。
- [ ] `materialized_mvs.json` 只保存成功物化且可用于 rewrite 的 MV。
- [ ] MV Candidate 只能在当前 batch 内生成，且不能跨 family 合并。
- [ ] `ExecutorAgent` 固定 dry-run，不连接 Spark。
- [ ] SQL 解析、SELECT 输出列、CTAS 校验、源表限定列检测等 SQL 操作必须使用 SQLGlot，不用正则。

## 3. 开发顺序

### Phase 0: 目录、配置与 rules

- [ ] 确认 `llm_demo/` 下存在 `notebooks/`、`configs/`、`rules/`、`src/core/`、`src/agents/`、`tests/`。
- [ ] 确认项目根目录存在 `.env`、`tpcds_simple.json`、`tpcds-spark/`。
- [ ] 确认 `requirements.txt` 至少包含 `pytest`、`pydantic`、`openai`、`python-dotenv`、`PyYAML`、`nbformat`、`sqlglot`。
- [ ] 更新 `configs/default.yaml` 与 `configs/paths.yaml`，让 notebook 可以指向项目根目录完整 `tpcds-spark/`。
- [ ] 更新 `rules/_prompt_template.md`，保持中文模板、规则、输入 artifact、输出 schema、示例模块。
- [ ] 更新全部 LLM Agent rules：
  - `rules/feature_agent.md`
  - `rules/family_agent.md`
  - `rules/batch_cluster_agent.md`
  - `rules/batch_mv_agent.md`
  - `rules/rewrite_agent.md`
  - `rules/self_iteration_agent.md`
- [ ] `executor_agent.md` 只保留 dry-run 规则说明；`ExecutorAgent` 本身仍用代码实现。
- [ ] 不新增独立 `rules/examples/` 目录；示例写入对应 rules Markdown。

### Phase 1: 基础设施

- [ ] 更新 `ArtifactStore` 命名方法，覆盖全部全量 SQL artifact path。
- [ ] 更新 `agent_base.py` / `LLMRulesAgent`，统一加载 `_prompt_template.md`、对应 Agent rules、输入 artifact、输出 schema，并执行 JSON schema 校验。
- [ ] 更新 `schemas.py`，补齐 QueryBlock、Feature status、family candidates、coverage summary 等 schema。
- [ ] 更新 `physical_schema.py`，读取根目录 `tpcds_simple.json`，只做物理表字段白名单校验，不自动修复 SQL。
- [ ] 更新 `sql_utils.py`，集中封装 SQLGlot 解析、CTAS 检查、SELECT 输出列名提取、源表限定列检测、ORDER BY / LIMIT 校验。
- [ ] 确认 `LLMClient` 仍只读取根目录 `.env`，不打印 key。
- [ ] 确认 `run_log.jsonl` 每条事件都有同一 `run_id` 内唯一 `event_id`。
- [ ] 涉及 MV Candidate 的 run log 必须带 `candidate_id`；普通 Agent 事件可为 `null`。

### Phase 2: SQLLoaderAgent

- [ ] `SQLLoaderAgent` 读取完整 `tpcds-spark/`。
- [ ] 输出 `00_raw_sql/sql_manifest.json`。
- [ ] manifest 只记录 `query_id`、原始路径、相对路径、文件大小等元信息。
- [ ] manifest 顺序稳定，后续 Feature 串行处理按该顺序执行。
- [ ] 不复制 SQL 文件，不把 SQL Text 落入 manifest。

### Phase 3: QueryAnalysisRunner

- [ ] 新增或完善 `QueryAnalysisRunner.run_all(sql_manifest_path, resume_feature=False)`。
- [ ] 按 manifest 顺序串行处理 Query。
- [ ] 每次只调用 `FeatureAgent.extract_one(query)`。
- [ ] 成功结果增量合并到：
  - `01_query_blocks/query_blocks.json`
  - `01_query_blocks/query_to_qbs.json`
  - `01_query_blocks/qb_to_query.json`
- [ ] 输出 `01_query_blocks/feature_extract_status.json`。
- [ ] status 至少包含 `success`、`partial_success`、`feature_failed`、`unsupported_sql_pattern`。
- [ ] 失败记录包含 `query_id`、`attempt`、`stage`、`error_type`、`error_message` 和可选 `unsupported_reasons`。
- [ ] 失败 Query 不中断 corpus，首轮结束后统一 retry 1 次。
- [ ] retry 不携带上一轮错误上下文。
- [ ] `resume_feature=True` 时只跳过当前 run 中 `success` / `partial_success` 的 Query。

### Phase 4: FeatureAgent

- [ ] 保持 `LLM + rules`。
- [ ] 输入完整单条 SQL Text，不做 chunk。
- [ ] 输出 outer / CTE / subquery / set branch QueryBlock。
- [ ] QueryBlock 至少包含：
  - `qb_id`
  - `query_id`
  - `scope_type`
  - `block_name`
  - `parent_qb_id`
  - `depends_on_qb_ids`
  - `tables`
  - `join_edges`
  - `predicates`
  - `group_by_exprs`
  - `aggregate_exprs`
  - `complexity_type`
  - `family_key`
  - `structural_flags`
  - `unsupported_reasons`
- [ ] 保留 generate + evaluate 两次 LLM 调用。
- [ ] evaluate 检查 SQL alias 是否被错误保留；表名应转换为物理表名。
- [ ] 不确定时写 `unsupported_reasons`，不编造字段。
- [ ] 至少一个可用 QueryBlock 时标记 `partial_success` 而不是整条失败。

### Phase 5: FamilyCandidateBuilder

- [ ] 代码实现，不是 Agent。
- [ ] 输入 QueryBlock artifact。
- [ ] 输出 `02_families/family_candidates.json`。
- [ ] 先按 `core_fact_table_set` blocking。
- [ ] 再按 table set signature 建 primary group。
- [ ] 识别 TPC-DS fact table 白名单：`store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。
- [ ] 支持 `dimension_only` family candidate，但不能和 fact-based group 混合。
- [ ] 计算 Jaccard、Containment、predicate column set、measure set、group-by set。
- [ ] 用 SQLGlot 计算或校验 join graph / predicate / aggregate 相关结构。
- [ ] 生成 `join_signature` 和 `normalized_join_edges`。
- [ ] primary group 内先按 `join_signature` 拆分 sub-group。
- [ ] 只在满足基础门槛时记录 `related_groups`，供 FamilyAgent evaluate。
- [ ] `family_candidates.json` 采用 group-level 为主、必要 `pair_evidence` 为辅，不保存完整 O(n^2) pair 矩阵。
- [ ] `unsupported_reasons` 非空的 QueryBlock 只作为诊断证据，不进入可合并成员。

### Phase 6: FamilyAgent

- [ ] 保持“代码预筛 + LLM 判定”。
- [ ] 输入 `family_candidates.json`，不直接全量聚类所有 QueryBlock。
- [ ] 输出 `02_families/query_families.json`。
- [ ] LLM 负责 evaluate candidate groups，执行 merge / split / keep separate。
- [ ] hard gate:
  - core fact table 不同默认不合并。
  - predicate column set 完全不同默认不合并。
  - 多 fact table 只和相同 `core_fact_table_set` 比较。
  - dimension-only family 不和 fact-based family 混合。
- [ ] FamilyAgent 采用 generate + evaluate 两次 LLM 调用。
- [ ] evaluate 检查重复 family、可合并 family、错误拆分、错误合并、成员 QueryBlock 归属错误。
- [ ] `family_id` 按 `family_{family_type}_{domain}_{distinguishing_feature}` 语义命名，不能只按表集合命名。
- [ ] `family_id` 必须全局唯一；完全重复 family 去重，同名但内容不同的 family 应在 evaluate 阶段生成语义可区分名称，代码层 `__2` 只作为 collision fallback 并写入日志。
- [ ] 输出 `common_predicates`、`predicate_shapes`、`union_group_by_exprs`、`union_measure_exprs` 等供 BatchMVAgent 使用的字段。

### Phase 7: BatchClusterAgent

- [ ] 保持 `LLM + rules`。
- [ ] 输入 QueryBlock、`query_to_qbs`、QueryFamily。
- [ ] 输出 `03_batches/complexity_batches.json`。
- [ ] 固定四个 global batch:
  - `join`
  - `join_filter`
  - `join_filter_groupby`
  - `other`
- [ ] batch 执行单位是完整 SQL / `query_id`。
- [ ] 一个 SQL 的顶层 batch 由代码层 canonical rules 根据完整 SQL 的 QueryBlock 结构信号汇总决定。
- [ ] 带 `unsupported_reasons` 的 QueryBlock 可以贡献 SQL 级复杂度信号，但不能进入 `family_groups` / MV Candidate / rewrite target。
- [ ] 完整 SQL 中同时存在 join 与有物理输入域的 group/aggregate/window/having/rollup 信号时，即使二者不在同一个 QueryBlock，也归入 `join_filter_groupby`。
- [ ] 没有 `tables` 和 `join_edges` 的纯派生 wrapper 聚合不单独把 SQL 抬到 `join_filter_groupby`。
- [ ] `other` 只作为完整 SQL 结构信号无法归入前三类时的兜底 batch。
- [ ] `family_groups` 只作为 batch 内组织信息。
- [ ] 顶层 `query_ids` 必须去重；同一 SQL 可出现在多个 `family_groups`，但最终 rewrite / execution 只能发生一次。
- [ ] `family_groups[].qb_ids` 只包含可用 QueryBlock，不包含带 `unsupported_reasons` 的 QueryBlock。
- [ ] BatchClusterAgent 采用 generate + evaluate 两次 LLM 调用，但 LLM 顶层 batch 输出必须被代码层 canonicalize。
- [ ] LLM 输出与 canonical batch 不一致时，run log 记录 `corrected_query_ids`。
- [ ] 不做并发上限和子 batch 拆分。

### Phase 8: BatchWorkflowRunner

- [ ] 代码实现，不是 Agent。
- [ ] 输入 `complexity_batches.json`、`sql_manifest.json`、QueryBlock paths、QueryFamily、`materialized_mvs.json`。
- [ ] 按 global batch 顺序执行：
  1. historical rewrite
  2. batch-local MV generation
  3. dry-run materialize
  4. final rewrite
  5. dry-run query order
- [ ] 如果 batch 为空，跳过。
- [ ] Runner 只编排路径和调用顺序，不判断 MV 是否应生成，也不判断 SQL 是否可 rewrite。
- [ ] historical rewrite 和 final rewrite 都以当前 batch 的 original SQL 为语义锚点。
- [ ] 每个 batch 只执行一次固定闭环，不做 batch 内无限迭代。

### Phase 9: BatchMVAgent

- [ ] 保持 `LLM + rules`。
- [ ] 只在当前 batch 内生成 MV Candidate。
- [ ] 单个 MV Candidate 不能跨 family。
- [ ] 输入 historical rewrite SQL、原始 QueryBlock、QueryFamily、当前 batch `family_groups`、`materialized_mvs.json`。
- [ ] 保留 generate + evaluate 两次 LLM 调用。
- [ ] 输出：
  - `04_batch_mvs/batch_{batch_id}_mv_candidates.json`
  - `04_batch_mvs/batch_{batch_id}_mv_build.sql`
- [ ] `build_sql` 使用 `CREATE TABLE ... AS SELECT ...`，不能使用 `CREATE OR REPLACE TABLE`。
- [ ] 普通物理列默认不 `AS`；聚合表达式必须显式 `AS {agg_func}_{source_column}`。
- [ ] `output_columns` 是 MV 真实物理列名，不能包含 `.`。
- [ ] `column_mappings.source_table.source_column` 必须存在于 `tpcds_simple.json`。
- [ ] `decision = materialize` 必须包含 `candidate_id`、`mv_id`、`target_table_name`、`build_sql`、`column_mappings`、`source_query_ids`、`target_queries`。
- [ ] `source_query_ids` / `target_queries` 都必须属于当前 batch。
- [ ] `source_qb_ids` / `target_qb_ids` 必须属于对应 Query、同一 family，且不能包含带 `unsupported_reasons` 的 QueryBlock。
- [ ] 支持 `mv_type = fine_grain_aggregate | detail_superset | dimension_only`。
- [ ] shared upstream superset MV 要记录 `residual_filters`；有限泛化谓词要记录 `generalized_predicates`。
- [ ] `depends_on_mv_ids` 只能引用可见历史 MV，且依赖关系不能成环。
- [ ] 如果发现跨 family 复用机会，只能写入 `reason` 或 run log，不能跨 family 生成 Candidate。
- [ ] `decision = skip` 保留在 candidate artifact 和 run log，不写入 `materialized_mvs.json`。

### Phase 10: RewriteAgent

- [ ] 保持 `LLM + rules`。
- [ ] 支持 `historical` 和 `final` 两个阶段。
- [ ] 每个阶段、每条 SQL 都输出：
  - `{query_id}_rewritten.sql`
  - `{query_id}_rewrite_meta.json`
- [ ] 即使无可用 MV，也必须输出 original-equivalent rewritten SQL。
- [ ] fallback 时 `status = "fallback"`、`used_mv_ids = []`、`fallback_reason` 非空。
- [ ] rewritten 时 `status = "rewritten"`、`used_mv_ids` 非空且均存在于 `materialized_mvs.json`。
- [ ] 只能使用 `available_from_batch <= current_batch_id` 的 MV。
- [ ] 使用 MV 时只能引用 MV 物理列名，不能引用源表限定列名。
- [ ] final rewrite 必须保持 original SQL 的输出列名、`ORDER BY`、`LIMIT`。
- [ ] final rewrite 若丢失 `ORDER BY` / `LIMIT`，最多触发 2 次 LLM 修正，仍失败则 fallback。
- [ ] 无法证明等价时 fallback。
- [ ] QueryBlock-local rewrite 只允许改写可证明安全的 CTE / subquery body，并且 meta 必须包含非空 `target_qb_ids`。
- [ ] QueryBlock-local rewrite 不能改变外层 Query 的 SELECT / JOIN / WHERE / GROUP BY / ORDER BY / LIMIT 结构。

### Phase 11: ExecutorAgent

- [ ] 固定 dry-run，不连接 Spark。
- [ ] `materialize_mvs(...)` 将 `decision = materialize` 且依赖满足的 Candidate 写入 `materialized_mvs.json`。
- [ ] 写入 `materialized_mvs.json` 时补齐 `source_candidate_id`、`source_batch_id`、`available_from_batch`、`family_id`、`source_qb_ids`、`target_qb_ids`、`depends_on_mv_ids`、`mv_predicates`、`generalized_predicates`、`residual_filters`、`output_columns`、`column_mappings`、`build_sql_path`。
- [ ] 失败或 skip Candidate 只写 run log。
- [ ] `run_queries(...)` 读取 final rewrite SQL 和 meta。
- [ ] 输出 `06_execution_logs/batch_{batch_id}_execution_order.json`。
- [ ] `run_query.depends_on_mv_ids` 必须来自 rewrite meta 的 `used_mv_ids`；rewrite meta 缺少该字段时直接失败。

### Phase 12: CoverageSummaryBuilder

- [ ] 代码实现，不调用 LLM。
- [ ] 输入已有 artifact 和 run log。
- [ ] 输出 `08_coverage/coverage_summary.json`。
- [ ] 汇总：
  - SQL manifest 覆盖
  - Feature success / partial_success / failed / unsupported
  - family coverage
  - batch coverage
  - MV Candidate materialize / skip / failed
  - historical / final rewrite fallback
  - execution order 覆盖
- [ ] 汇总 top unsupported reasons、top fallback reasons、unassigned query ids、queries using MV 等关键诊断字段。
- [ ] 只做诊断，不参与业务决策。

### Phase 13: SelfIterationAgent

- [ ] 保持 `LLM + rules`。
- [ ] 输入 run log、coverage summary、family candidates、MV candidates、rewrite meta、execution order。
- [ ] 输出 `07_feedback/feedback_rules_{run_id}.json`。
- [ ] 不自动修改 `rules/*.md`、代码或配置。
- [ ] 输出按 `target_agent` 分组。
- [ ] `suggested_rule_text` 使用中文；SQL、字段名、JSON key、Agent 名称保持英文。
- [ ] 每条 suggestion 必须包含 `evidence_refs`。
- [ ] 如果 evidence 引用 run log，必须包含 `event_id`。
- [ ] 如果 evidence 引用 MV Candidate，必须包含 `candidate_id`。

### Phase 14: Notebook 与后续 main.py

- [ ] 更新 `llm_demo/notebooks/etl_agent_flow.ipynb`，支持完整 `tpcds-spark/` coverage run。
- [ ] Notebook cell 保持人工可检查：
  1. 加载 `.env` 和 config
  2. 初始化 `ArtifactStore` / `LLMClient`
  3. 运行 SQLLoaderAgent
  4. 运行 QueryAnalysisRunner
  5. 查看 Feature status
  6. 运行 FamilyAgent
  7. 运行 BatchClusterAgent
  8. 运行 BatchWorkflowRunner
  9. 运行 CoverageSummaryBuilder
  10. 运行 SelfIterationAgent
- [ ] Notebook 跑通且流程稳定后，再收敛为 `main.py`。

## 4. 必须落盘的 Artifact

- [ ] `00_raw_sql/sql_manifest.json`
- [ ] `01_query_blocks/query_blocks.json`
- [ ] `01_query_blocks/query_to_qbs.json`
- [ ] `01_query_blocks/qb_to_query.json`
- [ ] `01_query_blocks/feature_extract_status.json`
- [ ] `02_families/family_candidates.json`
- [ ] `02_families/query_families.json`
- [ ] `03_batches/complexity_batches.json`
- [ ] `04_batch_mvs/batch_{batch_id}_mv_candidates.json`
- [ ] `04_batch_mvs/batch_{batch_id}_mv_build.sql`
- [ ] `04_batch_mvs/materialized_mvs.json`
- [ ] `05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewritten.sql`
- [ ] `05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewrite_meta.json`
- [ ] `05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewritten.sql`
- [ ] `05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewrite_meta.json`
- [ ] `06_execution_logs/batch_{batch_id}_execution_order.json`
- [ ] `08_coverage/coverage_summary.json`
- [ ] `06_execution_logs/run_log.jsonl`
- [ ] `07_feedback/feedback_rules_{run_id}.json`

## 5. 最小测试清单

### Offline Tests

- [ ] `SQLLoaderAgent` 生成 manifest，且不复制 SQL 文件。
- [ ] `ArtifactStore` 生成所有命名路径。
- [ ] `QueryAnalysisRunner` 串行调用 FeatureAgent。
- [ ] `QueryAnalysisRunner` 首轮失败后统一 retry 1 次。
- [ ] `QueryAnalysisRunner(resume_feature=True)` 跳过当前 run 已成功 Query。
- [ ] Feature artifact merge 保持 `query_to_qbs` / `qb_to_query` 双向一致。
- [ ] `physical_schema.py` 能用 `tpcds_simple.json` 校验存在 / 不存在的 `table.column`。
- [ ] `sql_utils.py` 能用 SQLGlot 检查 CTAS、SELECT 输出列、源表限定列、ORDER BY / LIMIT。
- [ ] `FamilyCandidateBuilder` 输出 group-level candidates，不保存完整 O(n^2) pair 矩阵。
- [ ] BatchCluster 输出四个 global batch。
- [ ] BatchWorkflowRunner 按固定五步调用各 Agent。
- [ ] BatchMVAgent 校验 `output_columns` 不能包含 `.`。
- [ ] BatchMVAgent 校验 `CREATE OR REPLACE TABLE` 不允许。
- [ ] BatchMVAgent 校验 `column_mappings.source_table.source_column` 必须存在于 `tpcds_simple.json`。
- [ ] BatchMVAgent 校验 `depends_on_mv_ids` 只能引用可见历史 MV，且不能成环。
- [ ] RewriteAgent 遇到源表限定列名、输出列名丢失、ORDER BY / LIMIT 丢失时 fallback。
- [ ] Executor dry-run 正确维护 `materialized_mvs.json`。
- [ ] execution order 的 `depends_on_mv_ids` 来自 rewrite meta。
- [ ] CoverageSummaryBuilder 能在部分失败 artifact 下生成 summary。
- [ ] SelfIterationAgent 输出按 target agent 分组，且每条 suggestion 有 `evidence_refs`。

### Live / Integration Tests

- [ ] q42 / q52 smoke test 跑通完整 dry-run 闭环。
- [ ] 完整 `tpcds-spark/` coverage run 能完成 Feature intake。
- [ ] 完整 `tpcds-spark/` run 允许 fallback / skip / unsupported，但必须生成 coverage summary。
- [ ] SelfIterationAgent 能基于全量日志输出 feedback rules。

### 推荐命令

```bash
conda run -n mv_gen python -m compileall llm_demo/src
conda run -n mv_gen pytest llm_demo/tests -k 'not live'
conda run -n mv_gen pytest llm_demo/tests
```

## 6. 本轮明确不做

- [ ] 不连接 Spark。
- [ ] 不做真实执行性能评估。
- [ ] 不做跨 run cache。
- [ ] 不做全量 Feature 并发。
- [ ] 不做单条 SQL chunk prompt。
- [ ] 不做下游阶段断点续跑。
- [ ] 不做 Batch 内子 batch 拆分。
- [ ] 不让 SelfIterationAgent 自动修改 rules。
- [ ] 不新增 Agent IO 状态对象。
- [ ] 不把 `family_candidates.json`、`coverage_summary.json` 升级为核心业务结构。

## 7. 第一轮验收标准

- [ ] q42 / q52 smoke test 可以稳定跑通。
- [ ] 完整 `tpcds-spark/` Feature intake 可以串行完成，失败 Query 有明确状态。
- [ ] 至少生成完整 artifact 树和 `run_log.jsonl`。
- [ ] `coverage_summary.json` 能回答“哪里成功、哪里失败、为什么 fallback / skip”。
- [ ] `feedback_rules_{run_id}.json` 能给出带证据的规则建议。
- [ ] 没有为了处理全量 SQL 引入新的核心业务数据结构或复杂并发机制。
