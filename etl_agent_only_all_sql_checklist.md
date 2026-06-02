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

### Phase 1: 基础设施

- [ ] 更新 `ArtifactStore` 命名方法，覆盖全部全量 SQL artifact path。
- [ ] 更新 `schemas.py`，补齐 QueryBlock、Feature status、family candidates、coverage summary 等 schema。
- [ ] 确认 `LLMClient` 仍只读取根目录 `.env`，不打印 key。
- [ ] 确认 `run_log.jsonl` 每条事件都有同一 `run_id` 内唯一 `event_id`。

### Phase 2: SQLLoaderAgent

- [ ] `SQLLoaderAgent` 读取完整 `tpcds-spark/`。
- [ ] 输出 `00_raw_sql/sql_manifest.json`。
- [ ] manifest 只记录 `query_id`、原始路径、相对路径、文件大小等元信息。
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
- [ ] 计算 Jaccard、Containment、predicate column set、measure set、group-by set。
- [ ] 用 SQLGlot 计算或校验 join graph / predicate / aggregate 相关结构。
- [ ] 生成 `join_signature` 和 `normalized_join_edges`。
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
- [ ] 一个 SQL 的 batch 由可用 QueryBlock 的最高复杂度决定。
- [ ] `family_groups` 只作为 batch 内组织信息。
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
- [ ] `decision = skip` 保留在 candidate artifact 和 run log，不写入 `materialized_mvs.json`。

### Phase 10: RewriteAgent

- [ ] 保持 `LLM + rules`。
- [ ] 支持 `historical` 和 `final` 两个阶段。
- [ ] 每个阶段、每条 SQL 都输出：
  - `{query_id}_rewritten.sql`
  - `{query_id}_rewrite_meta.json`
- [ ] 即使无可用 MV，也必须输出 original-equivalent rewritten SQL。
- [ ] 使用 MV 时只能引用 MV 物理列名，不能引用源表限定列名。
- [ ] final rewrite 必须保持 original SQL 的输出列名、`ORDER BY`、`LIMIT`。
- [ ] 无法证明等价时 fallback。
- [ ] QueryBlock-local rewrite 只允许改写可证明安全的 CTE / subquery body。

### Phase 11: ExecutorAgent

- [ ] 固定 dry-run，不连接 Spark。
- [ ] `materialize_mvs(...)` 将 `decision = materialize` 且依赖满足的 Candidate 写入 `materialized_mvs.json`。
- [ ] 失败或 skip Candidate 只写 run log。
- [ ] `run_queries(...)` 读取 final rewrite SQL 和 meta。
- [ ] 输出 `06_execution_logs/batch_{batch_id}_execution_order.json`。
- [ ] `run_query.depends_on_mv_ids` 必须来自 rewrite meta 的 `used_mv_ids`。

### Phase 12: CoverageSummaryBuilder

- [ ] 代码实现，不调用 LLM。
- [ ] 输入已有 artifact 和 run log。
- [ ] 输出 `06_execution_logs/coverage_summary.json`。
- [ ] 汇总：
  - SQL manifest 覆盖
  - Feature success / partial_success / failed / unsupported
  - family coverage
  - batch coverage
  - MV Candidate materialize / skip / failed
  - historical / final rewrite fallback
  - execution order 覆盖
- [ ] 只做诊断，不参与业务决策。

### Phase 13: SelfIterationAgent

- [ ] 保持 `LLM + rules`。
- [ ] 输入 run log、coverage summary、family candidates、MV candidates、rewrite meta、execution order。
- [ ] 输出 `07_feedback/feedback_rules_{run_id}.json`。
- [ ] 不自动修改 `rules/*.md`、代码或配置。
- [ ] 每条 suggestion 必须包含 `evidence_refs`。
- [ ] 如果 evidence 引用 run log，必须包含 `event_id`。

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
- [ ] `06_execution_logs/coverage_summary.json`
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
- [ ] `FamilyCandidateBuilder` 输出 group-level candidates，不保存完整 O(n^2) pair 矩阵。
- [ ] BatchCluster 输出四个 global batch。
- [ ] BatchWorkflowRunner 按固定五步调用各 Agent。
- [ ] Executor dry-run 正确维护 `materialized_mvs.json`。
- [ ] execution order 的 `depends_on_mv_ids` 来自 rewrite meta。
- [ ] CoverageSummaryBuilder 能在部分失败 artifact 下生成 summary。

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
