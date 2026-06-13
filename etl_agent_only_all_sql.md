# ETL MV Agent-Only 全量 SQL 原型实现方案

> 版本：v1.51-all-sql.35
> 日期：2026-06-02
> 目标：在 `llm_demo/` 中把 Agent-only 原型从 q42/q52 扩展到更大范围的 `tpcds-spark/` SQL。除 `SQLLoaderAgent` 和 `ExecutorAgent` 外，其余 Agent 仍采用 `LLM + rules` 实现；全量适配优先解决 Query Analysis intake、分批提取、失败隔离和 coverage 观测问题。

## 0. 定位

本方案参考 `dataintel_client/agent` 的实现风格，但不要求完全贴合。第一版重点不是做一个完整 Agent 框架，而是用最小的自研框架验证：

```text
SQLLoaderAgent(code)
  -> QueryAnalysisRunner(code)
       -> FeatureAgent(llm+rules, per query)
  -> FamilyAgent(llm+rules)
  -> BatchClusterAgent(code, from classification CSV)
  -> BatchWorkflowRunner(code)
       -> for each batch:
            RewriteAgent(llm+rules)
            BatchMVAgent(llm+rules)
            ExecutorAgent(code)
            RewriteAgent(llm+rules)
            ExecutorAgent(code)
  -> CoverageSummaryBuilder(code)
  -> SelfIterationAgent(llm+rules)
```

核心约束：

1. `SQLLoaderAgent` 使用代码实现，负责 SQL 文件读取校验和 SQL manifest artifact 落盘，不复制原始 SQL 文件。
2. `ExecutorAgent` 使用代码实现，当前阶段只负责 dry-run 执行计划、MV 状态维护和日志落盘，不连接 Spark。
3. `FeatureAgent`、`FamilyAgent`、`BatchMVAgent`、`RewriteAgent`、`SelfIterationAgent` 使用 `LLM + rules` 实现；当前 CSV 驱动链路中，`BatchClusterAgent` 的顶层 batch 分配由外部分类 CSV 决定，代码层只负责 artifact 组装和校验。
4. `rules` 是类似 skill 的 Markdown 规则文件，由 LLM 在运行时读取。
5. 原型代码、rules、artifact、日志均放在 `llm_demo/` 下。
6. LLM 密钥只从项目根目录 `.env` 读取，禁止硬编码在代码、notebook 或 artifact 中。
7. 核心业务数据结构保持简单：`QueryBlock`、`QueryFamily`、`ComplexityBatch`、`materialized_mvs`。
8. 不额外设计 Agent IO 状态对象；Agent 直接读取和写入 artifact。
9. MV Candidate 只能在当前 batch 内生成；允许 `BatchMVAgent` 只读参考完整的 `complexity_batches.json` 和 `query_families.json` 来评估当前 batch MV 的后续复用价值，但不能提前生成未来 batch 的 MV Candidate。
10. 当前 batch 的新 MV 允许基于 historical rewrite SQL 中使用到的历史 MV 构建，形成增量式 MV 扩充；这种依赖必须通过 `depends_on_mv_ids` 显式记录。
11. 一个新 MV 可以依赖多个历史 MV；`depends_on_mv_ids` 只记录直接依赖，整体依赖关系必须保持 DAG，不能形成循环。
12. 如果 MV Candidate 依赖的历史 MV 在执行时不可用，该 Candidate 物化失败，只记录到 `run_log.jsonl`，不写入 `materialized_mvs.json`，且不阻断当前 batch。
13. historical rewrite 和 final rewrite 都以当前 batch 的 original SQL 为语义锚点；historical rewrite 用于当前 batch 的增量式 MV 扩充，final rewrite 用于生成最终执行 SQL。
14. historical rewrite 阶段必须产出 SQL 文件；即使没有可用历史 MV，也要把与 original SQL 等价的 SQL Text 落盘，保持 batch 流程一致。
15. `used_mv_ids = []` 不影响当前 batch 生成 MV Candidate；Batch-1 初始就是从空 Materialized View State 开始生产 MV。
16. MV Candidate 必须来自当前 batch 的 Query 或 QueryFamily；下游 batch / 全局 QueryFamily 只读上下文只能影响物化决策，不能凭空触发当前 batch 没有结构依据的 MV Candidate。
17. `decision = skip` 的 MV Candidate 不进入 `materialized_mvs.json`，但必须保留在 MV Candidate artifact 和 run log 中，用于 SelfIterationAgent 分析。
18. SelfIterationAgent 不允许直接修改 rules 文件；它只输出带 `run_id` 的反馈 JSON，供人工 review 后再决定是否调整 rules。
19. SelfIterationAgent 可以输出 `suggested_rule_text` 作为可复制的规则建议片段，但不能自动写回 `rules/*.md`。
20. `suggested_rule_text` 使用中文撰写；SQL、字段名、JSON key、Agent 名称保持原样英文。
21. SelfIterationAgent 的反馈必须按 `target_agent` 分组，便于逐个 Agent 人工 review。
22. SelfIterationAgent 的每条反馈建议必须包含 `evidence_refs`，引用已有 run log、MV Candidate、query 或 batch 信息，避免无证据的规则修改建议。
23. `run_log.jsonl` 的每条事件必须包含稳定的 `event_id`，供 SelfIterationAgent 的 `evidence_refs` 精确引用。
24. 每个 MV Candidate 必须包含稳定的 `candidate_id`；`candidate_id` 用于追踪候选对象，`mv_id` 只在成功物化后作为可用 MV 身份进入 `materialized_mvs.json`。
25. RewriteAgent 只有在能够说明 rewritten SQL 与 original SQL 语义等价时才允许使用 MV；否则必须 fallback 到与 original SQL 等价的 SQL Text，并记录 `fallback_reason`。
26. 每个 MV Candidate 必须包含 `source_query_ids`，表示候选来自当前 batch 的哪些 Query。
27. MV Candidate 的 `target_queries` 只记录当前 batch 内可被该 Candidate 服务或覆盖的 Query；下游 batch 只能作为物化决策参考写入 `reason`，不能成为结构化 target。
28. `BatchClusterAgent` 在当前链路中读取 `batch_classification.csv` 作为 batch source of truth，生成全局 `ComplexityBatch`，再把可用 QueryBlock 按 QueryFamily 组织到对应 batch 的 `family_groups`。
29. batch 的执行单位始终是完整 SQL/query_id；QueryBlock 只作为 family 归属、MV 生成和 rewrite 辅助结构，不再承担 batch 分类判断。
30. 一个 SQL 的全局 batch 由外部分类 CSV 决定；带 `unsupported_reasons` 的 QueryBlock 不能进入 `family_groups`、MV Candidate source/target 或 rewrite target，只保留为诊断和 fallback 证据。
31. 如果一个 SQL 的多个 QueryBlock 属于不同 family，该 `query_id` 可以出现在同一 global batch 的多个 `family_groups` 中，但顶层 `query_ids` 必须去重，最终执行和 final rewrite 只能发生一次。
32. `BatchMVAgent` 不允许跨 family 合并生成一个 MV Candidate；如果两个结构确实可共享 MV，应先由 `FamilyAgent` 的 evaluate 阶段合并或修正 QueryFamily。
33. `QueryFamily` 不是简单 SQL 相似簇，而是一组可由同一个 upstream join-domain MV 或 shared superset MV 覆盖的 QueryBlock。
34. `FamilyAgent` 使用 Jaccard 衡量表集合相似度，使用 Containment 识别 ETL 宽表覆盖关系；二者只负责生成 family candidate，不直接决定最终合并。候选分层为：`strong` = `Jaccard = 1.0` 或 `Containment = 1.0` 且 core fact table 相同；`medium` = `Jaccard >= 0.6` 且 core fact table 相同，或 `Containment >= 0.8` 且较小 QueryBlock 的表集合主要被较大 join domain 覆盖；其余默认保持分离。
35. core fact table 是 QueryBlock join domain 中承载业务事实、度量或高基数交易记录的中心表，例如 TPC-DS 中的 `store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。第一版把 core fact table 相同作为 family 合并 hard gate；如果 core fact table 不同，即使 Jaccard / Containment 较高，也默认不合并。
36. 多 fact table QueryBlock 第一版只和具有完全相同 core fact table set 且 join graph 可证明不会引入行数膨胀的 QueryBlock 合并；否则保持为独立 family 或 `other`，避免宽表 MV 把不同事实语义错误混合。
37. `FamilyAgent` 的最终合并必须通过 core fact table、join graph、predicate shape、measure compatibility 和 roll-up safety 检查；join skeleton 相同或可证明等价、过滤列结构同构但常量值不同的 QueryBlock 可以进入同一 family。
38. predicate shape 兼容只覆盖两类第一版允许场景：同一过滤列常量不同；或者一方比另一方多出过滤条件，且多出的过滤列可在 MV 中保留为 residual filter 所需列。若过滤列集合完全不同，第一版必须拆成不同 family。
39. `QueryFamily.common_predicates` 只记录 family 内完全相同的谓词；同构过滤列但常量值不同的谓词用 `predicate_shapes` 表达。
40. `BatchMVAgent` 默认构造 shared upstream superset MV：MV 不要求是某条 workflow SQL 的子集，而是使多个 workflow SQL 能通过 filter / projection / aggregate / roll-up 从 MV 重写得到。
41. `BatchMVAgent` 先判断 shared upstream superset MV 的语义边界，再决定物理形态；若可证明聚合后仍支持所有 target Query，优先生成 fine-grain aggregate MV，否则 fallback 到 detail superset MV，仍无法安全 rewrite 时 skip。
42. superset MV 只把 target Query 共同拥有的 predicate shape 放入 MV predicate，并且泛化范围只能来自当前 batch 已出现的常量集合；非共同 predicate 不进入 MV predicate，而是记录为下游 residual filter。
43. RewriteAgent 永远不覆盖 original SQL；每个 rewrite 阶段、每条 SQL 都必须输出 `{query_id}_rewritten.sql`、对应 `{query_id}_rewrite_meta.json`，并追加 run log。
44. 即使没有安全可用的 MV，RewriteAgent 也必须生成 original-equivalent 的 rewritten SQL，后续阶段统一从 rewritten SQL 产物读取。
45. `BatchMVAgent` 采用两次 LLM + rules 调用：第一次生成 `candidate_mv_output`，第二次 evaluate 并修正为最终 MV Candidate 输出。
46. `BatchMVAgent` 的 evaluate 阶段不新增核心数据结构；它只校验并修正 MV Candidate 是否满足当前 batch、family 边界、shared upstream superset、依赖 DAG 和 rewrite 安全约束。
47. `BatchMVAgent` 输出的 `output_columns` 必须是 MV 表的真实物理列名，不能写 `date_dim.d_year` 这类源表限定列名。
48. 源字段身份通过 MV Candidate / Materialized View State 内的 `column_mappings` 保存，例如 `date_dim.d_year -> d_year`；这是 MV 元数据字段，不是新的独立核心业务结构。
49. 普通物理列默认不写 `AS`，`mv_column` 使用源字段名；只有聚合表达式和普通列同名冲突时才显式 `AS mv_column`。
50. `tpcds_simple.json` 只作为物理表字段白名单，用于校验 `source_table.source_column` 是否存在；不生成新的 artifact，不做 SQL 自动修复。
51. RewriteAgent 使用 MV 时只能引用 MV 物理列名；如果 rewritten SQL 仍引用源表限定列名，或丢失 original SQL 的输出列名，必须 fallback。
52. `ExecutorAgent.run_queries(...)` 输出的 execution order 中，`run_query.depends_on_mv_ids` 必须来自对应 rewrite meta 的 `used_mv_ids`。
53. `BatchMVAgent` 的 `build_sql` 统一使用 `CREATE TABLE ... AS SELECT ...`；不生成 `CREATE OR REPLACE TABLE`，避免原型阶段引入覆盖已有表的语义。
54. 代码层凡涉及 SQL 解析、SELECT 输出列名提取、CTAS 规范化、源表限定列检测等 SQL 操作，统一使用 SQLGlot；不使用正则表达式解析、判断或改写 SQL。
55. measure 类型的 MV 物理列名统一使用 `{agg_func}_{source_column}`，例如 `SUM(store_sales.ss_ext_sales_price)` 输出 `sum_ss_ext_sales_price`，保留源字段 `ss_ext_sales_price` 的业务前缀。
56. final rewrite 必须保留 original SQL 的 `ORDER BY` 和 `LIMIT`；如果 LLM 输出遗漏，RewriteAgent 最多重试 2 次，仍无法修正则 fallback。
57. CTE / subquery QueryBlock 不是二等对象；只要能提取清晰的物理表 join domain，且没有不可安全改写的结构风险，就允许进入 `QueryFamily` 和 MV Candidate。
58. batch 的执行单位仍然是完整 SQL；CTE / subquery QueryBlock 只作为 family、MV 生成和局部 rewrite 的分析目标，不单独形成执行 batch。
59. 针对 CTE / subquery 的 MV rewrite 可以采用 QueryBlock-local rewrite：只改写 CTE / subquery body，外层 SQL 结构保持不变。
60. QueryBlock-local rewrite 必须保持被替换 block 的输出列名和父子依赖契约；如果无法证明外层 Query 仍可正确引用该 CTE / subquery，必须 fallback。
61. 全量 SQL 的 Query Analysis intake 由 code-level `QueryAnalysisRunner` 负责；它不是 LLM Agent，不新增核心业务结构，只负责 per-query Feature 调用、失败隔离、retry、Artifact merge 和 coverage status。
62. `FeatureAgent` 保持聚焦于单条 SQL 的 QueryBlock 抽取与 evaluate，不承担全量 SQL 循环、merge 和 retry 编排。
63. 全量 SQL 的 QueryFamily formation 采用“代码预筛 + LLM 判定”：代码负责 deterministic blocking / scoring，LLM 负责候选 family 的安全门判断、合并、拆分和解释。
64. `FamilyAgent` 不应直接把所有 QueryBlock 交给 LLM 做全局聚类；它应读取代码生成的 family candidates，并在 rules 指导下 evaluate。
65. family 预筛阶段如果需要解析 SQL 表达式、谓词列、聚合表达式或 join graph，应使用 SQLGlot；不使用正则表达式做 SQL 结构判断。
66. `FamilyCandidateBuilder` 的输出落盘为轻量诊断 artifact：`02_families/family_candidates.json`；它可供 FamilyAgent evaluate、debug 和 SelfIterationAgent 引用，但下游主流程仍以 `query_families.json` 为准。
67. `family_candidates.json` 采用 group-level 为主、pair evidence 为辅的结构；不保存完整 O(n²) pair 矩阵，只保存进入候选组或代表边界判断的 pair evidence。
68. `FamilyCandidateBuilder` 使用两级 blocking：先按 core fact table 粗分，再按 table set signature 形成 primary group；对 table set 存在 containment 关系的邻近 group，记录为 related groups，供 LLM 判断宽表覆盖可能性。
69. `related_groups` 第一版只在满足基础语义门时建立：core fact table 相同、`Containment >= 0.8`、join graph 不出现明显多事实表混合、predicate column set 不能完全不相交。
70. 如果两个 candidate group 的 predicate column set 只在 date/time 维度列上相交，例如 `date_dim.*` 或 `time_dim.*`，还必须满足 measure set 或 group-by set 兼容，才允许进入 `related_groups`。
71. measure set 第一版采用保守兼容定义：aggregate function 和 source column 必须相同；`COUNT(*)` 只和 `COUNT(*)` 兼容；`COUNT(DISTINCT x)` 暂不与其他 measure 兼容。
72. group-by set 第一版采用 roll-up compatible 定义：不要求完全相同；在 measure、core fact table 和 join graph 兼容时，只要 group-by expr 都能作为 MV 维度列保留，就允许用 `union_group_by_exprs` 支持多个 query 的 roll-up。
73. join graph 第一版采用代码级基础 hard gate：`FamilyCandidateBuilder` 计算 `join_signature`，primary group 内先按 `join_signature` 拆分 sub-group；join signature 不同的 group 不直接合并，只能作为 related group 交给 LLM evaluate。
74. 如果 join key 涉及未知表达式、非等值条件或复杂条件，`join_signature = "unknown"`，该候选必须进入 LLM evaluate 或 skip，不能由代码层直接合并。
75. `join_signature` 使用 fact-centered edge signature，例如 `store_sales.ss_item_sk->item.i_item_sk`；同时保留 `normalized_join_edges` 作为可读证据，供 LLM evaluate、debug 和 SelfIterationAgent 引用。
76. 多 fact table QueryBlock 第一版允许进入 family，但只允许和 `core_fact_table_set` 完全相同的 QueryBlock 比较；单 fact 和多 fact 不混合，避免行数膨胀和事实语义混淆。
77. `core_fact_table_set` 第一版由 `FamilyCandidateBuilder` 基于 TPC-DS fact table 白名单代码识别；FeatureAgent / LLM 可以给 reason，但不作为 fact table 身份的事实来源。
78. 不含 fact table 的 QueryBlock 可以进入 `family_type = "dimension_only"` 的独立 family；dimension-only family 可以生成维表过滤 / 映射类 MV Candidate，但不能和 fact-based family 混合。
79. dimension-only MV Candidate 允许物化；它可用于 QueryBlock-local rewrite，也可作为后续 fact-based MV 的 `depends_on_mv_ids` 构建依赖，但不能让 dimension-only family 与 fact-based family 合并。
80. Batch 运行闭环由 code-level `BatchWorkflowRunner` 编排；它不是 Agent，不使用 LLM，不新增核心业务结构，只负责按固定顺序调用 RewriteAgent、BatchMVAgent 和 ExecutorAgent，并集中管理 batch 级 artifact 路径、`materialized_mvs.json` 状态路径和 run log。
81. Artifact 路径契约由 `ArtifactStore` 的命名方法统一维护；Agent、Runner 和 notebook 不应手写深层 artifact 路径字符串。该设计只收敛路径生成方式，不引入 artifact registry 或新的业务数据结构。
82. 全量 SQL 实验必须生成轻量 `coverage_summary.json` 诊断 artifact；它由代码汇总已有 artifact 和 run log，不参与 family、batch、MV、rewrite 的业务决策，只用于人工复盘和 SelfIterationAgent 的证据输入。
83. 当前阶段不设计真实 Spark 执行边界，也不新增 `ExecutionAdapter`；`ExecutorAgent` 固定 dry-run，只把可物化 MV 视为已成功物化并输出 SQL 运行顺序。真实执行留到后续阶段重新设计。
84. 每个 `run_id` 都视为一次孤立实验；系统不做跨 run 的 LLM 调用缓存、Feature 结果复用或 artifact 复用。第一版也不引入 prompt hash、rules hash、SQL hash 等缓存键，避免把实验复现和成本优化混在一起。
85. Feature 提取允许 `partial_success`：如果一条 SQL 至少有一个可用 QueryBlock，即使其他 CTE / subquery / set branch 带有 `unsupported_reasons`，该 SQL 也可以进入后续 family、batch 和 final rewrite 流程；但 unsupported QueryBlock 不能作为 MV Candidate 来源或 rewrite 目标，只能保留为诊断证据并触发 skip / fallback。
86. 当前可行性 demo 中，`BatchClusterAgent` 使用 CSV 驱动入口：顶层 `query_id -> batch_id` 来自 `batch_classification.csv`，代码层根据 QueryFamily 组装 `family_groups` 并做校验；不再让 FeatureAgent、FamilyAgent 或 LLM 判断 batch。
87. 当前可行性 demo 中，`FamilyAgent` 保持“代码预筛 + LLM 判定”的混合方案：`FamilyCandidateBuilder` 只负责 deterministic blocking / scoring 和证据生成，`FamilyAgent` 负责候选 family 的合并、拆分、evaluate 和 reason；不改成纯 LLM 聚类，也不改成纯代码分组。
88. 当前可行性 demo 中，`BatchMVAgent` 保持 `LLM + rules` 主导实现，负责生成 MV Candidate、`build_sql`、`decision` 和 `reason`；代码层只做 schema 校验、SQLGlot 安全检查、物理列校验和依赖合法性检查，不提前拆成代码候选骨架。
89. 当前可行性 demo 中，`RewriteAgent` 保持 `LLM + rules` 主导实现，负责生成 rewritten SQL 和 rewrite meta；代码层只做 SQLGlot 解析、可用 MV 校验、MV 物理列引用检查、输出列名 / `ORDER BY` / `LIMIT` 契约校验和 fallback 触发，不提前改成模板化或 AST rewrite。
90. 当前可行性 demo 中，`SelfIterationAgent` 保持 `LLM + rules` 主导实现，读取 `run_log.jsonl`、`coverage_summary.json`、`family_candidates.json`、MV Candidate、rewrite meta 和 execution order 等已有证据，输出 `feedback_rules_{run_id}.json`；它不自动修改 rules、代码或配置。
91. 第一轮全量 SQL 实验直接覆盖完整 `tpcds-spark/`，不先筛代表性子集；该轮目标是观察 coverage、unsupported / failed 分布、fallback 原因和 SelfIterationAgent 反馈，而不是追求高 rewrite 成功率或高 MV 命中率。
92. 同一 `run_id` 内只允许 Feature intake 阶段 resume：`QueryAnalysisRunner` 可以基于当前 run 的 `feature_extract_status.json` 和 QueryBlock artifact 跳过已 `success` / `partial_success` 的 Query，只继续处理 `feature_failed`、`unsupported_sql_pattern` 或未处理 Query；Family、Batch、MV、Rewrite 和 Execution 阶段不做局部断点续跑，必须从当前 QueryBlock artifact 重新生成下游 artifact。
93. 第一版不为单条 SQL 设计 SQLGlot 预拆分或 chunk prompt 机制；TPC-DS Spark SQL 通常不是超长上下文问题，`FeatureAgent` 仍以 one query per call 读取完整 SQL Text。遇到复杂结构时优先输出 `partial_success`、可用 QueryBlock 和明确的 `unsupported_reasons`，而不是把 SQL 预切成多个 LLM 输入。
94. Feature retry 不注入上一轮错误上下文。重试时仍使用同一 SQL Text、同一 rules 和同一 prompt 结构；上一轮的 schema validation、evaluate、API 或超时错误只写入 `run_log.jsonl`、`feature_extract_status.json` 和后续 `coverage_summary.json`，供人工复盘，不作为下一次 LLM 输入。
95. 第一版全量 Feature intake 的 LLM 调用完全串行执行，不设计并发参数、不并行处理多个 Query。并发、限流队列和批量吞吐优化留到后续阶段；当前优先保证日志顺序清晰、失败归因简单和实验行为可复盘。

### 0.1 全量 SQL Query Analysis Intake 决策

当输入从 q42/q52 扩展到 `tpcds-spark/` 多条 SQL 后，Query Analysis intake 不应把所有 SQL Text 一次性塞入同一个 LLM prompt。全量模式采用“manifest 全量、Feature 分次、Artifact 合并”的策略。

1. `SQLLoaderAgent` 可以一次读取全量 SQL 路径并生成 `00_raw_sql/sql_manifest.json`。它只记录 `query_id`、SQL 路径和文件大小，不把 SQL Text 放入 LLM prompt，因此不会造成上下文爆炸。
2. `FeatureAgent` 不再一次处理 manifest 中的所有 Query。全量模式下应按 `query_id` 循环调用，每次只输入一个 Query 的 SQL Text；后续可扩展为小批量输入，但默认粒度先保持 one query per call。
3. 第一版不对单条 SQL 做 SQLGlot 预拆分，也不把一个 Query 拆成多个 chunk prompt。TPC-DS SQL 存在 CTE、subquery、set operation 等复杂结构，但通常不属于超长 SQL 上下文问题；复杂结构通过 QueryBlock 提取、`partial_success` 和 `unsupported_reasons` 处理。
4. 每次 Feature 提取仍执行 generate + evaluate 两阶段，但 evaluate 只针对当前 Query 的 candidate feature output，不读取全量 SQL Text。
5. 每个 Query 的 Feature 结果必须增量合并到同一组全局 QueryBlock Artifact：
   - `01_query_blocks/query_blocks.json`
   - `01_query_blocks/query_to_qbs.json`
   - `01_query_blocks/qb_to_query.json`
6. 每个 `run_id` 是一次孤立实验。`QueryAnalysisRunner` 不读取其他 run 的 QueryBlock、Feature status 或 LLM 输出作为缓存；即使同一 SQL 在历史 run 中成功处理，本轮也重新调用 FeatureAgent。
7. 合并规则由代码层保证：同一 `qb_id` 不重复写入；同一 `query_id` 只允许在当前 run 的 retry 或人工调试重跑中覆盖该 Query 的旧 QueryBlock；`query_to_qbs` 与 `qb_to_query` 必须保持双向一致。
8. 单个 Query 的 Feature 提取失败不能中断整个 corpus。首轮处理时，失败 Query 只写入 run log 和失败列表，系统继续处理后续 Query；已经成功提取的 QueryBlock Artifact 继续可用。
9. 如果一个 Query 同时包含可用 QueryBlock 和带 `unsupported_reasons` 的 QueryBlock，该 Query 标记为 `partial_success`，继续进入下游流程。完整 SQL 按 CSV 进入 ComplexityBatch；可用 QueryBlock 可进入 QueryFamily / family_groups / BatchMVAgent；unsupported QueryBlock 只保留为诊断和 rewrite fallback 证据。
10. 如果一个 Query 没有任何可用 QueryBlock，首轮结束后进入失败列表统一 retry。retry 仍失败的 Query 才放弃 Feature 输出，并在 coverage summary 中标记为 `feature_failed` 或 `unsupported_sql_pattern`。
11. retry 策略保持克制：第一版只做“首轮 + 失败 Query 统一重试 1 轮”，不为单个 Query 做无限重试，也不在失败时立即阻塞全量处理。
12. retry 不携带上一轮错误上下文。无论上一轮失败来自 schema validation、evaluate、API timeout 还是网络错误，第二次 Feature 调用仍使用同一 SQL Text、同一 rules 和同一 prompt 结构；错误原因只落入日志和状态 artifact。
13. Feature LLM 调用完全串行执行，按 `sql_manifest.queries` 顺序逐条处理，不在第一版设计并发参数或 worker pool。
14. 失败记录必须包含 `query_id`、`attempt`、`stage`、`error_type`、`error_message` 和可选 `unsupported_reasons`。其中 `stage` 至少区分 `generate`、`evaluate`、`schema_validation` 和 `artifact_merge`。
15. 为了方便全量实验复盘和同一 `run_id` 内的 Feature-only resume，Query Analysis intake 可以输出一个轻量诊断 Artifact：`01_query_blocks/feature_extract_status.json`。它不是新的核心业务数据结构，也不是跨 run 缓存索引；只记录当前 run 中每个 Query 的提取状态、尝试次数和失败原因。状态至少包括 `success`、`partial_success`、`feature_failed` 和 `unsupported_sql_pattern`。下游核心流程仍以 `query_blocks.json`、`query_to_qbs.json` 和 `qb_to_query.json` 为准。
16. 全量 Query Analysis intake 的核心目标不是一次性得到完美 QueryBlock，而是稳定产出“可继续向下游推进的 QueryBlock 覆盖面”和“明确的 unsupported / failed 归因”。

因此，用户提出的“for 循环分次调用，每次只处理一个 SQL，再把 feature 信息保存到同一个 JSON 文件中”是合理方向；但该循环不应长期放在 notebook 中，也不应塞进 `FeatureAgent`。第一版采用 code-level `QueryAnalysisRunner` 承担全量 intake 编排：它读取 `sql_manifest.json`，逐条调用 `FeatureAgent.extract_one(...)`，合并成功结果，记录失败并在全量首轮结束后统一 retry。

`QueryAnalysisRunner` 的定位：

1. 它不是 Agent，不读取 rules，不调用 LLM。
2. 它不新增核心业务数据结构，只维护已有 QueryBlock artifact 和轻量诊断 artifact。
3. 它把全量 SQL intake 的工程不变量集中起来，避免 notebook、测试和未来 `main.py` 重复实现 for 循环。
4. 它的输出仍然只面向下游稳定 Artifact：`query_blocks.json`、`query_to_qbs.json`、`qb_to_query.json`、`feature_extract_status.json` 和 `run_log.jsonl`。
5. 它可以在 notebook 中被一行调用，也可以在后续 `main.py` 中复用。
6. 它不是跨 run 缓存管理器，不比较历史 run，也不根据历史 artifact 跳过某个 Query；每次新 `run_id` 都从当前 manifest 重新处理。
7. 它只支持同一 `run_id` 内的 Feature intake resume：当 `resume_feature = true` 时，可以跳过当前 run 中已经 `success` 或 `partial_success` 的 Query，只处理失败或未处理 Query。
8. 它第一版不负责并发调度，所有 Feature 调用按 manifest 顺序串行执行；并发、限流和吞吐优化留到后续实现阶段。

### 0.2 第一轮全量 SQL 实验范围

第一轮全量实验直接使用项目根目录的完整 `tpcds-spark/` 作为输入 corpus，不再先挑选代表性子集。原因是当前阶段的核心任务是验证 Agent-only 编排在真实 workflow 覆盖面下的稳定性，并尽早暴露 Feature、Family、Batch、MV、Rewrite 和 dry-run Execution 各阶段的失败分布。

该实验的成功标准不是“所有 SQL 都能成功 rewrite”，而是：

1. `SQLLoaderAgent` 能生成完整 `sql_manifest.json`。
2. `QueryAnalysisRunner` 能逐条调用 `FeatureAgent`，失败 Query 记录后继续处理。
3. 首轮失败 Query 在 corpus 处理结束后统一 retry 1 次。
4. `feature_extract_status.json` 能明确区分 `success`、`partial_success`、`feature_failed` 和 `unsupported_sql_pattern`。
5. `CoverageSummaryBuilder` 能汇总每个阶段的通过、skip、fallback 和失败原因。
6. `SelfIterationAgent` 能基于 coverage 与日志输出有证据的 `feedback_rules_{run_id}.json`。

因此，全量第一轮允许出现大量 fallback、skip 和 unsupported 标记。只要这些结果被稳定记录，并且不会阻断其他 SQL 的继续处理，该 run 就有实验价值。q42/q52 仍保留为最小闭环 smoke test，但不作为全量方案的唯一验证范围。

Feature 阶段允许在同一 `run_id` 内恢复运行。如果全量 SQL intake 因 LLM 超时、API 限流或 notebook 断开中断，`QueryAnalysisRunner` 可以读取当前 run 的 `feature_extract_status.json`、`query_blocks.json`、`query_to_qbs.json` 和 `qb_to_query.json`，跳过已经成功或 partial_success 的 Query，只继续处理失败或未处理 Query。这个 resume 只覆盖 Feature intake，不覆盖 Family 之后的下游编排。

Feature resume 完成后，`02_families/` 及之后的 artifact 必须从当前 QueryBlock artifact 重新生成。系统不支持从某个 Family、Batch、MV、Rewrite 或 Execution 中间点继续跑；如果这些下游 artifact 已经存在，应视为旧结果，由本轮重新生成的下游流程覆盖或重新落盘。

全量模式的 Query Analysis intake 推荐流程：

```text
SQLLoaderAgent.run(all_sql_paths)
  -> 生成 sql_manifest.json

QueryAnalysisRunner.run_all(sql_manifest_path)
  for query in sql_manifest.queries:
    FeatureAgent.extract_one(query)
      success -> merge 到 query_blocks.json / query_to_qbs.json / qb_to_query.json
      partial_success -> merge 可用 QueryBlock，记录 unsupported QueryBlock 与原因
      failed  -> 记录到 failed_queries

  for query in failed_queries:
    FeatureAgent.extract_one(query, retry_attempt=1)
      success -> merge 到全局 QueryBlock Artifact
      partial_success -> merge 可用 QueryBlock，记录 partial status
      failed  -> 写入 feature_extract_status.json，标记放弃原因
```

该循环第一版保持串行，不使用并发 worker，也不同时发起多个 LLM 请求。

`retry_attempt=1` 只表示同一 `run_id` 内的第二次尝试，不表示 prompt 会携带上一轮错误。错误上下文只进入日志和 coverage，不进入 LLM 输入。

如果同一 `run_id` 内恢复 Feature intake，则调用等价的 `QueryAnalysisRunner.run_all(sql_manifest_path, resume_feature=true)`；该模式只跳过当前 run 已成功或 partial_success 的 Query，不读取历史 run，也不跳过下游阶段。

### 0.3 CTE / Subquery QueryBlock 提取决策

对 `tpcds-spark/` 的初步扫描显示，全量 SQL 中复杂结构并不少见：

```text
WITH / CTE 文件数：34
UNION 文件数：20
INTERSECT 文件数：4
EXCEPT 文件数：1
window / OVER 文件数：9
ROLLUP 文件数：11
outer join 文件数：11
subquery-ish 文件数：34
```

因此，全量模式不能只提取 outer QueryBlock。FeatureAgent 应当提取 CTE、subquery、set operation branch 等 QueryBlock；但复杂结构不要求第一版完整支持 MV rewrite，无法安全表达时写入 `unsupported_reasons`。

第一版提取原则：

1. 每个 Query 必须至少有 `{query_id}.outer`。
2. CTE 应提取为独立 QueryBlock，例如 `q1.cte.001`；CTE 名只作为解析辅助，不进入最终 Feature artifact。
3. subquery 应提取为独立 QueryBlock，例如 `q1.subq.001`；subquery alias 只作为解析辅助，不进入最终 Feature artifact。
4. UNION / UNION ALL / INTERSECT / EXCEPT 的每个分支可以提取为独立 QueryBlock，例如 `q56.setop.001`、`q56.setop.002`。
5. QueryBlock 的 `tables` 仍只记录物理表名，不把 CTE 名、derived table alias 或 subquery alias 当作物理表。
6. 如果 outer QueryBlock 主要从 CTE / derived table 读取，且无法还原为清晰物理表 join domain，则 outer QueryBlock 可以保留较少物理表信息，并在 `unsupported_reasons` 中记录 `derived_source_not_expanded` 或 `cte_dependency_not_rewritable`。
7. CTE / subquery 内部如果能提取物理表、join、predicate、group by、aggregate，就应正常进入后续 QueryFamily 分析和 MV Candidate 生成；这正是全量 TPC-DS 中发现复用机会的关键。
8. window、ROLLUP、INTERSECT、EXCEPT、复杂相关子查询、非可分解聚合等结构，可以提取 QueryBlock 结构特征，但应在 `unsupported_reasons` 或 `structural_flags` 中标记，供 BatchMVAgent / RewriteAgent 保守 skip 或 fallback。

进入下游的边界：

1. `unsupported_reasons = []` 的 QueryBlock 可以作为 FamilyCandidateBuilder、FamilyAgent 和 BatchMVAgent 的主要输入。
2. `unsupported_reasons` 非空的 QueryBlock 可以保留在 `query_blocks.json` 中，作为父子依赖、coverage、SelfIterationAgent 和 RewriteAgent fallback 的证据；第一版默认不把它作为 MV Candidate 的 `source_qb_ids` 或 `target_qb_ids`。
3. 如果一条 SQL 的 outer QueryBlock 可用，但部分 CTE / subquery unsupported，该 SQL 仍可进入 batch；final rewrite 对 unsupported block 必须保持 original-equivalent 或 fallback。
4. 如果 outer QueryBlock 本身不可安全表达，但某个 CTE / subquery QueryBlock 可用，该可用 block 仍可进入 QueryFamily 和 MV Candidate；完整 SQL 的 final rewrite 必须保持外层输出契约，无法证明时 fallback。
5. 如果一条 SQL 没有任何可用 QueryBlock，则不进入 FamilyAgent / BatchClusterAgent / BatchMVAgent，只进入 `coverage_summary.json` 和 run log。

### 0.4 QueryBlock 数据结构评估

当前核心数据结构仍应保持为 `QueryBlock`，不新增 `CTEBlock`、`SubqueryBlock` 或 `SetOpBlock`。但在新的 batch CSV 化和 family 后置生成决策下，FeatureAgent 的 QueryBlock 只保存可复用 SQL 事实，不保存 batch 判断、family 判断或 SQL 局部 alias。

旧字段：

```text
qb_id
query_id
scope_type
tables
join_edges
predicates
group_by_exprs
aggregate_exprs
complexity_type
family_key
unsupported_reasons
```

这些字段能表达一个 block 的局部结构，但不能稳定表达：

1. 同一物理表的多实例语义，例如 `date_dim` 作为 sold date / returned date 多次出现。
2. block 父子关系，例如 outer QueryBlock 引用哪个 CTE 或 subquery。
3. set operation 分支来源，例如 UNION ALL 中的第几个 branch。
4. window / rollup / set operation / correlated subquery 等复杂结构的显式标记。
5. predicate / group by / aggregate 的列级 lineage。

因此，当前 QueryBlock v2 仍保持单一核心结构，但把表达式升级为结构化对象，并移除 `complexity_type` 与 `family_key`：

```json
{
  "qb_id": "q1.cte.001",
  "query_id": "q1",
  "scope_type": "cte",
  "block_name": "cte_1",
  "parent_qb_id": "q1.outer",
  "depends_on_qb_ids": [],
  "structural_flags": [],
  "relation_instances": [
    {
      "relation_instance_id": "store_returns",
      "physical_table": "store_returns",
      "role": null
    },
    {
      "relation_instance_id": "date_dim",
      "physical_table": "date_dim",
      "role": null
    }
  ],
  "tables": ["store_returns", "date_dim"],
  "join_edges": [
    {
      "left_relation_instance_id": "store_returns",
      "left_table": "store_returns",
      "left_column": "sr_returned_date_sk",
      "right_relation_instance_id": "date_dim",
      "right_table": "date_dim",
      "right_column": "d_date_sk",
      "operator": "=",
      "expr": "store_returns.sr_returned_date_sk = date_dim.d_date_sk"
    }
  ],
  "predicates": [
    {
      "expr": "date_dim.d_year = 2000",
      "predicate_shape": "date_dim.d_year = ?",
      "columns": ["date_dim.d_year"],
      "relation_instance_ids": ["date_dim"]
    }
  ],
  "projections": [
    {
      "expr": "store_returns.sr_customer_sk",
      "columns": ["store_returns.sr_customer_sk"],
      "relation_instance_ids": ["store_returns"]
    }
  ],
  "group_by_exprs": [
    {
      "expr": "store_returns.sr_customer_sk",
      "columns": ["store_returns.sr_customer_sk"],
      "relation_instance_ids": ["store_returns"]
    }
  ],
  "aggregate_exprs": [
    {
      "expr": "SUM(store_returns.sr_return_amt)",
      "agg_func": "SUM",
      "source_table": "store_returns",
      "source_column": "sr_return_amt",
      "source_relation_instance_id": "store_returns"
    }
  ],
  "order_by_exprs": [],
  "limit": null,
  "unsupported_reasons": []
}
```

字段含义：

1. `block_name`：稳定分析名，例如 `outer`、`cte_1`、`subquery_1`、`branch_1`；不保存 SQL 中的 CTE 名或 alias。
2. `parent_qb_id`：直接父 QueryBlock；outer QueryBlock 可为空。
3. `depends_on_qb_ids`：当前 QueryBlock 直接依赖的 CTE / subquery QueryBlock。它只表达 QueryBlock 依赖，不等同于 MV 的 `depends_on_mv_ids`。
4. `structural_flags`：复杂结构标签，例如 `cte`、`subquery`、`set_op_union_all`、`set_op_intersect`、`window`、`rollup`、`outer_join`、`correlated_subquery`、`non_decomposable_aggregate`。
5. `relation_instances`：当前 QueryBlock 内的物理表实例。`relation_instance_id` 与 SQL alias 无关；单次出现使用物理表名，多次出现使用稳定 role 或按出现顺序编号，例如 `date_dim__sold_date`、`date_dim__1`。
6. `join_edges`、`predicates`、`projections`、`group_by_exprs`、`aggregate_exprs`、`order_by_exprs`：结构化表达式事实，保存物理表名、物理列名和 `relation_instance_ids`，不保存 SELECT 输出 alias。

这些字段不改变“Query 是执行单位，QueryBlock 是分析单位”的原则，也不改变下游核心 Artifact：`query_blocks.json`、`query_to_qbs.json`、`qb_to_query.json`。它们只是让全量 SQL 下的 QueryBlock 更可解释、更可合并、更容易被安全门过滤。

对当前结构的结论：

```text
FeatureAgent 只抽取 alias-free SQL 事实。
FamilyCandidateBuilder / FamilyAgent 根据这些事实派生 family candidate 和最终 QueryFamily。
BatchClusterAgent 读取外部 batch source of truth，不再依赖 FeatureAgent 的复杂度标签。
```

代码实现阶段建议把 `QueryBlock` schema 补全为以下形态：

```text
qb_id                  required
query_id               required
scope_type             required  # outer / cte / subquery / setop_branch
block_name             optional  # stable analysis name, not SQL alias
parent_qb_id           optional
depends_on_qb_ids      optional list[str]
structural_flags       optional list[str]
relation_instances     required list[RelationInstance]
tables                 required list[str]
join_edges             required list[JoinEdge]
predicates             required list[PredicateExpression]
projections            required list[QueryExpression]
group_by_exprs         required list[QueryExpression]
aggregate_exprs        required list[AggregateExpression]
order_by_exprs         required list[OrderByExpression]
limit                  optional str | int
unsupported_reasons    required list[str]
```

Feature artifact 不保存 `complexity_type`、`block_shape`、`family_key`、SQL table alias 或 SELECT 输出 alias。Rewrite 阶段如需保持 original SQL 的输出契约，应从 original SQL 解析和校验，而不是依赖 Feature artifact 中持久化 alias。

### 0.5 CTE QueryBlock 的 MV 与局部 Rewrite 决策

全量 TPC-DS 中的 CTE 不应只被当成 outer Query 的附属信息。很多 SQL 的外层主 Query 直接依赖 CTE 名或 derived table alias，导致整条 SQL 的全局 rewrite 难度较高；但 CTE 内部本身可能是稳定的 join-filter-aggregate 结构，反而更适合作为 MV Candidate 来源和 rewrite 目标。

因此，第一版全量方案采用以下原则：

1. CTE QueryBlock 可以进入 `QueryFamily`。如果两个 CTE QueryBlock 具有相同或可兼容的 core fact table、join graph、predicate shape 和 measure，就可以归入同一 family。
2. CTE QueryBlock 可以生成 MV Candidate。MV Candidate 仍必须来自当前 batch 的 Query / QueryFamily，但其结构证据可以来自当前 batch SQL 内的 CTE QueryBlock。
3. MV Candidate 仍保留 SQL 级字段 `source_query_ids` 和 `target_queries`，用于说明候选来自哪些完整 SQL、服务哪些完整 SQL。
4. 对 CTE / subquery 场景，MV Candidate 还应增加可选的 `source_qb_ids` 和 `target_qb_ids`，用于说明候选来自哪些 QueryBlock、计划改写哪些 QueryBlock。它们是 Candidate 元数据字段，不是新的核心业务结构。
5. BatchClusterAgent 不因为 CTE QueryBlock 生成新的执行单位。一个 SQL 进入哪个 global batch，由外部分类 CSV 决定；unsupported QueryBlock 不参与 `family_groups`、MV Candidate 或 rewrite target，只保留为诊断和 fallback 证据。
6. RewriteAgent 可以做 QueryBlock-local rewrite：只替换 CTE / subquery body，使该 block 从 MV 读取，外层 Query 继续引用原 CTE 名或 subquery alias。
7. QueryBlock-local rewrite 必须保持原 block 的输出契约，包括输出列名、可被父 Query 引用的 alias、排序 / limit 语义边界以及父子依赖关系。
8. 如果 CTE 是递归 CTE、相关子查询、包含无法安全下推 / 保留的 window、rollup、set operation 或非可分解聚合，则可以提取结构特征，但 BatchMVAgent / RewriteAgent 应保守 skip 或 fallback。

示例：

```sql
WITH ss AS (
  SELECT
    ss_item_sk,
    ss_sold_date_sk,
    SUM(ss_ext_sales_price) AS revenue
  FROM store_sales
  GROUP BY ss_item_sk, ss_sold_date_sk
)
SELECT item.i_brand, SUM(ss.revenue)
FROM ss
JOIN item ON ss.ss_item_sk = item.i_item_sk
GROUP BY item.i_brand;
```

如果 `ss` 这个 CTE 的内部 QueryBlock 可被某个 MV 覆盖，rewrite 可以只改写 CTE body：

```sql
WITH ss AS (
  SELECT
    ss_item_sk,
    ss_sold_date_sk,
    revenue
  FROM mv_store_sales_item_date_revenue
)
SELECT item.i_brand, SUM(ss.revenue)
FROM ss
JOIN item ON ss.ss_item_sk = item.i_item_sk
GROUP BY item.i_brand;
```

这里外层 Query 仍然引用 `ss`，因此 rewritten CTE 必须继续输出 `ss_item_sk`、`ss_sold_date_sk`、`revenue`。如果 MV 只能输出 `sum_ss_ext_sales_price` 而无法保持 `revenue` 这个 CTE 输出名，就不能直接替换，除非 rewritten CTE 显式 `AS revenue`；否则必须 fallback。

## 1. 工作目录

除项目根目录 `.env` 外，Agent-only 原型相关文件均放在：

```text
llm_demo/
```

建议目录：

```text
llm_demo/
├── README.md
├── notebooks/
│   └── etl_agent_flow.ipynb
├── configs/
│   ├── default.yaml
│   └── paths.yaml
├── workflow/
│   └── tpcds-spark/
├── rules/
│   ├── _prompt_template.md
│   ├── feature_agent.md
│   ├── family_agent.md
│   ├── batch_cluster_agent.md
│   ├── batch_mv_agent.md
│   ├── rewrite_agent.md
│   ├── executor_agent.md
│   └── self_iteration_agent.md
├── src/
│   ├── core/
│   │   ├── agent_base.py
│   │   ├── llm_client.py
│   │   ├── artifact_store.py
│   │   ├── query_analysis_runner.py
│   │   ├── family_candidate_builder.py
│   │   ├── batch_workflow_runner.py
│   │   ├── coverage_summary_builder.py
│   │   ├── physical_schema.py
│   │   ├── sql_utils.py
│   │   └── schemas.py
│   └── agents/
│       ├── sql_loader_agent.py
│       ├── feature_agent.py
│       ├── family_agent.py
│       ├── batch_cluster_agent.py
│       ├── batch_mv_agent.py
│       ├── rewrite_agent.py
│       ├── executor_agent.py
│       └── self_iteration_agent.py
├── artifacts/
│   └── {run_id}/
│       ├── 00_raw_sql/
│       ├── 01_query_blocks/
│       ├── 02_families/
│       ├── 03_batches/
│       ├── 04_batch_mvs/
│       ├── 05_rewritten_sql/
│       ├── 06_execution_logs/
│       └── 07_feedback/
└── tests/
    └── test_agent_flow_q42_q52.py
```

说明：

1. `notebooks/etl_agent_flow.ipynb` 是第一版主入口，参考 `examples/notebooks/agent` 的试验方式，直接在 notebook 中实例化和调用各 Agent。
2. 第一版可以不实现 `src/main.py`。当 notebook 跑通后，再把稳定流程收敛成 `src/main.py`。
3. `workflow/tpcds-spark/` 可以复制少量 SQL，例如 `q42.sql`、`q52.sql`；也可以在 `configs/paths.yaml` 中引用项目根目录的 `../tpcds-spark/`。
4. Artifact 和日志必须写入 `llm_demo/artifacts/{run_id}/`，不要污染项目根目录。

## 2. 环境配置

项目根目录放置 `.env`，由本地运行时读取：

```dotenv
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com/
DEEPSEEK_MODEL=deepseek-v4-flash
LLM_TEMPERATURE=0.2
LLM_MAX_RETRIES=2
```

约束：

1. `.env` 不提交到版本库。
2. 不在 `llm_client.py`、notebook、rules、artifact 中硬编码 API key。
3. `llm_demo/src/core/llm_client.py` 只负责从环境变量读取配置，并提供 `infer(prompt, load_json=True)`。
4. 如果环境变量缺失，启动时直接报错，不进入 LLM 调用。

第一版最小依赖建议包含：

```text
pytest
pydantic
openai
python-dotenv
PyYAML
nbformat
sqlglot
```

其中 `sqlglot` 用于代码层 SQL AST 操作，例如 CTAS 解析、SELECT 输出列名识别和 rewrite 安全检查。

## 3. Agent 实现方式

| Agent | 实现方式 | 说明 |
|---|---|---|
| `SQLLoaderAgent` | 代码 | 读取 SQL 文件、生成 `query_id`、保存 SQL manifest artifact |
| `FeatureAgent` | LLM + rules | 从 SQL 中提取 QueryBlock JSON |
| `FamilyAgent` | 代码预筛 + LLM + rules | 代码先做 blocking / scoring，LLM 再做 family evaluate、merge、split 和 reason |
| `BatchClusterAgent` | 代码，CSV 驱动 | 读取外部 batch 分类 CSV 生成 SQL 级 ComplexityBatch，并按 QueryFamily 在 batch 内保留 family_groups |
| `BatchMVAgent` | LLM + rules | 在 batch 内生成、选择 MV candidate，并输出 CTAS SQL |
| `RewriteAgent` | LLM + rules | 基于全局已物化 MV 生成 rewritten SQL |
| `ExecutorAgent` | 代码 | 当前阶段固定 dry-run；通过 `materialize_mvs(...)` 更新可用 MV 状态，通过 `run_queries(...)` 输出 SQL 运行顺序，并维护 `materialized_mvs` |
| `SelfIterationAgent` | LLM + rules | 基于日志生成规则优化建议 |

辅助工程模块：

| Module | 实现方式 | 说明 |
|---|---|---|
| `QueryAnalysisRunner` | 代码 | 读取 SQL manifest，逐条调用 `FeatureAgent`，负责 QueryBlock artifact merge、失败隔离、统一 retry 和 `feature_extract_status.json` |
| `FamilyCandidateBuilder` | 代码 | 基于 QueryBlock 生成 family candidate groups，计算 core fact table、Jaccard、Containment、predicate column set、measure set 等可量化特征 |
| `BatchWorkflowRunner` | 代码 | 按 global batch 顺序调用 historical rewrite、batch-local MV generation、dry-run materialize、final rewrite 和 query order 输出，集中维护 batch 级 artifact 路径和 `materialized_mvs.json` 路径 |
| `CoverageSummaryBuilder` | 代码 | 汇总 feature、family、batch、MV、rewrite 和 execution order 覆盖情况，输出轻量 `coverage_summary.json` 诊断 artifact |

第一版 `FamilyAgent` 和 `BatchClusterAgent` 仍串行调用：先由 `FamilyAgent` 生成 QueryFamily，再由 `BatchClusterAgent` 读取外部 batch 分类 CSV、QueryBlock、`query_to_qbs` 和 QueryFamily 生成全局 ComplexityBatch。这样让 family 继续服务 batch 内 MV 生成边界，但不参与 batch 分类判断，也不引入额外的 FamilyBatch 数据结构。

当前可行性 demo 的 batch source of truth 是 `batch_classification.csv`。`BatchClusterAgent` 的职责是把 CSV 中的 `Batch-1` / `Batch-2` / `Batch-3` / `Batch-4` / `Batch-5` / `Other` 映射为 `ComplexityBatch.batch_type`，并基于 QueryFamily 组装 `family_groups`；旧的结构复杂度规则只作为历史方案保留，不再作为当前链路的判定依据。

`FamilyAgent` 内部不再采用纯 LLM 全局聚类。全量 SQL 下先由 `FamilyCandidateBuilder` 执行 deterministic blocking / scoring：

```text
QueryBlock artifact
  -> FamilyCandidateBuilder(code)
       计算 table set / core fact table / Jaccard / Containment
       计算 predicate column set / measure set / group-by set
       先按 core fact table 粗分，再按 table set signature 建 primary group
       为 containment 关系的邻近 group 记录 related groups
       生成 02_families/family_candidates.json
  -> FamilyAgent(llm+rules)
       读取 family_candidates.json 并 evaluate candidate groups
       merge / split / keep separate
       输出 query_families.json
```

这样把可量化、可测试、可复现的部分放在代码中，把 ETL 宽表覆盖、residual filter 安全性、roll-up safety 等难以完全算法化的判断留给 LLM + rules。当前可行性 demo 保持这个混合方案，不切换为纯 LLM 全局聚类，也不切换为纯代码 family grouping；后续正式实现阶段再根据 coverage、误合并和漏合并情况决定替换范围。

`family_candidates.json` 是轻量诊断 artifact，不是新的核心业务数据结构。它可以在后续分 family 时作为辅助证据，也可以被 SelfIterationAgent 引用来判断 family 误合并或漏合并的原因；但 BatchClusterAgent 的 batch 分配主输入是外部分类 CSV，BatchMVAgent 的 family 边界主输入仍然是 `query_families.json`。

当前可行性 demo 中，`BatchMVAgent` 继续由 `LLM + rules` 主导，不拆成“代码生成候选骨架 + LLM 补全”的形式。这样可以先观察 LLM 在 shared upstream superset MV、增量式 MV 扩充、dimension-only MV、partial_success SQL 等边界上的候选生成能力。代码层只负责必要的 schema、SQLGlot、物理列、batch-local、single-family 和依赖 DAG 校验；如果 LLM 输出不满足契约，则 fail、skip 或 fallback，而不是由代码自动猜测修复。

当前可行性 demo 中，`RewriteAgent` 也继续由 `LLM + rules` 主导，不提前切换为模板化 rewrite 或完整 SQLGlot AST rewrite。这样可以先观察 LLM 在 historical rewrite、final rewrite、QueryBlock-local rewrite、partial_success fallback、输出 alias 保持、`ORDER BY` / `LIMIT` 保持等边界上的表现。代码层只负责解析与校验：确认 rewritten SQL 可被 SQLGlot 解析、使用的 MV 存在且可见、引用的列是 MV 物理列、输出列名和排序 / limit 契约未被破坏；如果不满足契约，则触发重试或 fallback，不由代码自动修复 SQL。

当前可行性 demo 中，`SelfIterationAgent` 继续由 `LLM + rules` 主导，不改成纯代码指标报告，也不跳过自迭代反馈。它读取已有 artifact 和日志，把失败集中点、fallback 原因、family 误合并 / 漏合并迹象、MV Candidate 质量问题、rewrite 契约破坏和 execution order 依赖问题整理成 `feedback_rules_{run_id}.json`。该反馈只进入人工 review，不自动写回 `rules/*.md`，避免把单轮实验噪声直接固化为规则。

该文件采用 group-level 为主、pair evidence 为辅的结构：

```json
{
  "candidate_groups": [
    {
      "candidate_group_id": "cand_group_store_sales_date_dim_item_001",
      "blocking_key": "core_fact=store_sales|tables=date_dim,item,store_sales",
      "family_type": "fact_based",
      "core_fact_table_set": ["store_sales"],
      "core_fact_block": "store_sales",
      "table_set_signature": "date_dim,item,store_sales",
      "join_signature": "store_sales.ss_item_sk->item.i_item_sk|store_sales.ss_sold_date_sk->date_dim.d_date_sk",
      "normalized_join_edges": [
        "date_dim.d_date_sk = store_sales.ss_sold_date_sk",
        "item.i_item_sk = store_sales.ss_item_sk"
      ],
      "member_qb_ids": ["q42.outer", "q52.outer"],
      "related_groups": [
        {
          "related_group_id": "cand_group_store_sales_date_dim_item_promotion_001",
          "relation": "containment_candidate",
          "containment": 1.0,
          "join_signature_relation": "compatible",
          "reason": "current table set is contained by related wider table set"
        }
      ],
      "common_core_fact_tables": ["store_sales"],
      "common_tables": ["date_dim", "item", "store_sales"],
      "min_jaccard": 1.0,
      "min_containment": 1.0,
      "predicate_column_sets": {
        "q42.outer": ["date_dim.d_moy", "date_dim.d_year", "item.i_manager_id"],
        "q52.outer": ["date_dim.d_moy", "date_dim.d_year", "item.i_manager_id"]
      },
      "measure_sets": {
        "q42.outer": ["SUM(store_sales.ss_ext_sales_price)"],
        "q52.outer": ["SUM(store_sales.ss_ext_sales_price)"]
      },
      "pair_evidence": [
        {
          "left_qb_id": "q42.outer",
          "right_qb_id": "q52.outer",
          "jaccard": 1.0,
          "containment": 1.0,
          "core_fact_table_match": true,
          "join_signature_match": true,
          "predicate_column_set_relation": "equal",
          "measure_set_relation": "compatible"
        }
      ],
      "candidate_reason": "same core fact table, same table set, compatible predicates and measure"
    }
  ]
}
```

`pair_evidence` 不保存全量 pair 矩阵，只保留进入候选组的关键 pair，或用于说明边界情况的代表性 pair。这样既给 LLM group-level 输入，又保留调试和反馈所需的 pair-level 证据。

blocking 采用两级策略：

1. `core_fact_table_set` 由代码基于 TPC-DS fact table 白名单识别：`store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。LLM 不负责决定 fact table 身份。
2. 若 `core_fact_table_set` 为空，则标记 `family_type = "dimension_only"`，只进入 dimension-only candidate group；不能和 fact-based group 混合。
3. dimension-only family 可用于维表过滤、维表映射或 CTE 局部 MV，例如 `item` 过滤、`date_dim` 日历映射；但不能被当作 fact join-domain family。
4. 一级按 `family_type` + `core_fact_block` / `core_fact_table_set` 粗分，例如 `fact_based:store_sales`、`fact_based:store_sales,store_returns`、`dimension_only:item` 分开，避免不同事实语义混合。
5. 二级按 `table_set_signature` 建 primary group，例如 `date_dim,item,store_sales`。
6. 如果两个 primary group 共享 core fact table，且 table set 存在 containment 关系，例如 `{store_sales,date_dim,item}` 被 `{store_sales,date_dim,item,promotion}` 覆盖，则不直接合并，只在 `related_groups` 中记录 `containment_candidate`，交给 FamilyAgent evaluate 判断是否可形成宽表 shared superset family。
7. 如果 core fact table 不同，不建立 related group，即使 table set 相似也默认断开。
8. 第一版建立 `related_groups` 的阈值为 `Containment >= 0.8`，并同时要求 join graph 不出现明显多事实表混合、predicate column set 不能完全不相交。该条件只决定“是否交给 LLM evaluate”，不直接决定 family 合并。
9. predicate column set 的交集需要区分强弱：`date_dim.*`、`time_dim.*` 这类 date/time 维度列只算弱交集。如果两个 group 只在 date/time 维度列上相交，还必须满足 measure set 或 group-by set 兼容；如果存在非 date/time 维度 predicate 交集，则可以进入 related group evaluate。
10. measure set 第一版按严格等价判断兼容：`SUM(store_sales.ss_ext_sales_price)` 只和同一 aggregate function、同一 source column 的 measure 兼容；`COUNT(*)` 只和 `COUNT(*)` 兼容；`COUNT(DISTINCT x)` 暂不参与 measure 兼容合并。`AVG` 由 `SUM/COUNT` 推导这类规则留作后续 cost-based / SelfIteration 扩展。
11. group-by set 第一版按 roll-up compatible 判断兼容：不要求两个 QueryBlock 的 group-by set 完全相同；如果 measure set 兼容、join graph 兼容，且所有 group-by expr 都能作为 MV 维度列保留，则可以使用 `union_group_by_exprs` 构造更细粒度 MV，再由各 query roll-up 得到结果。表达式型 group-by、函数型 group-by 或无法稳定保留的派生列，应标记为需要 LLM evaluate 或 skip。
12. join graph 第一版采用基础 hard gate：`FamilyCandidateBuilder` 为每个 QueryBlock 生成规范化 `join_signature`。`join_signature` 使用 fact-centered edge signature，例如 `store_sales.ss_sold_date_sk->date_dim.d_date_sk|store_sales.ss_item_sk->item.i_item_sk`。同一 primary group 内先按 `join_signature` 拆分 sub-group；`join_signature` 不同的 group 不直接合并，只能作为 `related_groups` 交给 LLM evaluate。
13. 同时保留 `normalized_join_edges`，例如 `date_dim.d_date_sk = store_sales.ss_sold_date_sk`、`item.i_item_sk = store_sales.ss_item_sk`，作为 LLM evaluate、debug 和 SelfIterationAgent 的可读证据。
14. 如果 join key 涉及未知表达式、非等值条件、OR 条件或复杂函数条件，`join_signature` 设为 `unknown`。`unknown` 不参与代码层 hard merge，只能进入 LLM evaluate 或 skip。
15. 多 fact table QueryBlock 使用 `core_fact_table_set` 作为 hard gate：`{store_sales, store_returns}` 只能和同样 `core_fact_table_set = {store_sales, store_returns}` 的 QueryBlock 比较；不能和 `{store_sales}` 或 `{store_returns}` 单 fact QueryBlock 混合。
16. 多 fact table 候选还必须通过 join graph 行数膨胀检查；如果无法证明不会因事实表之间的 join 产生重复放大，保持独立 family 或标记为 `other`。

## 4. 最小框架设计

### 4.1 参考 `dataintel_client/agent`

可以借鉴以下形态：

```text
BaseAgent:
  async run(...)
  async _run(...)
```

但本项目不需要完全复刻。当前只需要：

1. 每个 Agent 都有统一 `run(...)` 入口。
2. 父流程在 notebook 中显式串行调用各 Agent。
3. Agent 之间通过 `ArtifactStore` 的命名路径方法和显式 artifact 路径传递结果，避免把大量 SQL 和 JSON 都塞进内存对象。
4. 执行过程只把关键输入、输出、错误和耗时追加到 run log artifact。

### 4.2 Artifact 契约

本系统不再设计额外的 Agent IO 对象。原因是各 Agent 的数据来源已经很明确：

```text
raw SQL
QueryBlock / query_to_qbs / qb_to_query
QueryFamily
ComplexityBatch
batch SQL
rewritten batch SQL
materialized_mvs
run_log
```

因此，Agent 之间不需要再传递额外中间对象。Notebook 编排层只负责保存这些 artifact 路径，并把路径作为参数传给下一个 Agent。为了避免全量 SQL 后路径字符串散落在 notebook、Runner 和 Agent 中，第一版要求 `ArtifactStore` 提供命名路径方法；调用方通过方法获取路径，而不是手写深层目录。

第一版建议的最小 artifact 契约：

```text
{run_id}/00_raw_sql/sql_manifest.json
{run_id}/01_query_blocks/query_blocks.json
{run_id}/01_query_blocks/query_to_qbs.json
{run_id}/01_query_blocks/qb_to_query.json
{run_id}/01_query_blocks/feature_extract_status.json
{run_id}/02_families/family_candidates.json
{run_id}/02_families/query_families.json
{run_id}/03_batches/complexity_batches.json
{run_id}/04_batch_mvs/batch_{batch_id}_mv_candidates.json
{run_id}/04_batch_mvs/batch_{batch_id}_mv_build.sql
{run_id}/04_batch_mvs/materialized_mvs.json
{run_id}/05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewritten.sql
{run_id}/05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewrite_meta.json
{run_id}/05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewritten.sql
{run_id}/05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewrite_meta.json
{run_id}/06_execution_logs/batch_{batch_id}_execution_order.json
{run_id}/06_execution_logs/coverage_summary.json
{run_id}/06_execution_logs/run_log.jsonl
{run_id}/07_feedback/feedback_rules_{run_id}.json
```

设计原则：

1. 业务结果以 artifact 为唯一事实来源。
2. Notebook 中可以用普通变量保存路径，但不形成新的系统级数据结构；这些路径应来自 `ArtifactStore` 命名方法。
3. `materialized_mvs.json` 是 Materialized View State，只保存已经成功物化且可被 RewriteAgent 使用的 Materialized View；它必须保留 `mv_predicates`、`generalized_predicates` 和 `residual_filters`，否则 RewriteAgent 无法判断查询 filter 是否已经被 MV predicate 覆盖。
4. `run_log.jsonl` 记录每个 Agent 的输入路径、输出路径、耗时、错误，不参与业务决策；每条记录必须有稳定的 `event_id`。
5. MV Candidate 中被跳过、物化失败或仅用于诊断的信息保留在 `batch_{batch_id}_mv_candidates.json` 和 `run_log.jsonl`，不写入 `materialized_mvs.json`。
6. `run_id` 由 notebook 在每次实验开始时生成，建议使用 `YYYYMMDD_HHMMSS` 或手动命名的实验 ID。
7. `family_candidates.json` 只作为 FamilyAgent evaluate、debug 和 SelfIterationAgent 证据来源；下游主流程不直接依赖它。
8. `coverage_summary.json` 只作为全量实验诊断和 SelfIterationAgent 证据来源；下游主流程不依赖它做业务决策。
9. 不做跨 run artifact 复用。历史 run 的 artifact 可用于人工对比和论文实验分析，但不能作为本轮 Agent 执行的缓存输入。
10. 同一 `run_id` 内只允许 Feature intake resume；`feature_extract_status.json` 可以作为跳过已成功 Query 的当前 run 检查点，但不能作为 Family、Batch、MV、Rewrite 或 Execution 的断点续跑依据。
11. 后续替换算法实现时，只要保持 artifact 契约不变，就不影响上游和下游 Agent。

`ArtifactStore` 的第一版命名方法保持克制，只覆盖已经确定的 artifact：

```text
sql_manifest_path()
query_blocks_path()
query_to_qbs_path()
qb_to_query_path()
feature_extract_status_path()
family_candidates_path()
query_families_path()
complexity_batches_path()
batch_mv_candidates_path(batch_id)
batch_mv_build_sql_path(batch_id)
materialized_mvs_path()
rewrite_dir(batch_id, stage)              # stage = historical | final
rewritten_sql_path(batch_id, stage, query_id)
rewrite_meta_path(batch_id, stage, query_id)
execution_order_path(batch_id)
coverage_summary_path()
run_log_path()
feedback_rules_path()
```

这些方法只负责生成路径、创建必要目录和配合 JSON / SQL / JSONL 读写；不负责判断 artifact 是否业务正确，也不维护额外 registry。Agent 和 Runner 的公共接口仍接收显式 artifact path，便于测试中传入手工构造的 artifact。

`materialized_mvs.json` 的最小结构：

```json
{
  "materialized_mvs": [
    {
      "mv_id": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "table_name": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "source_candidate_id": "cand_batch_2_family_fact_ss_dd_item_manager_moy_year_0001",
      "source_batch_id": 2,
      "available_from_batch": 2,
      "family_id": "family_fact_ss_dd_item_manager_moy_year",
      "family_type": "fact_based",
      "target_queries": ["q42", "q52"],
      "source_qb_ids": ["q42.outer", "q52.outer"],
      "target_qb_ids": ["q42.outer", "q52.outer"],
      "depends_on_mv_ids": ["mv_batch_1_example"],
      "mv_predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year IN (2000)"
      ],
      "generalized_predicates": [
        {
          "predicate_shape": "date_dim.d_year = <CONST>",
          "covered_values": [2000],
          "source_query_ids": ["q42", "q52"]
        }
      ],
      "residual_filters": [
        {
          "query_id": "q42",
          "predicates": []
        },
        {
          "query_id": "q52",
          "predicates": []
        }
      ],
      "group_by_exprs": ["date_dim.d_year", "date_dim.d_moy", "item.i_manager_id"],
      "measure_exprs": ["SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price"],
      "output_columns": [
        "d_year",
        "d_moy",
        "i_manager_id",
        "sum_ss_ext_sales_price"
      ],
      "column_mappings": [
        {
          "source_expr": "date_dim.d_year",
          "source_table": "date_dim",
          "source_column": "d_year",
          "mv_column": "d_year",
          "role": "dimension"
        },
        {
          "source_expr": "date_dim.d_moy",
          "source_table": "date_dim",
          "source_column": "d_moy",
          "mv_column": "d_moy",
          "role": "filter"
        },
        {
          "source_expr": "item.i_manager_id",
          "source_table": "item",
          "source_column": "i_manager_id",
          "mv_column": "i_manager_id",
          "role": "filter"
        },
        {
          "source_expr": "SUM(store_sales.ss_ext_sales_price)",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "mv_column": "sum_ss_ext_sales_price",
          "role": "measure"
        }
      ],
      "build_sql_path": "{run_id}/04_batch_mvs/batch_2_mv_build.sql"
    }
  ]
}
```

该文件不保存 `status = failed`、`decision = skip` 或未执行的 MV Candidate。成功物化的 MV 可以保留 `source_candidate_id`、`source_qb_ids` 和 `target_qb_ids`，用于回溯其来源候选和支持 QueryBlock-local rewrite。RewriteAgent 读取这个文件判断可用 Materialized View，并使用其中的 `mv_predicates` / `generalized_predicates` 判断 MV 已包含的过滤语义，使用 `residual_filters` 判断下游仍需补充的过滤条件，使用 `output_columns` / `column_mappings.mv_column` 作为可引用的 MV 物理列集合。

`run_log.jsonl` 的单行最小结构：

```json
{
  "run_id": "20260524_153000",
  "event_id": "20260524_153000:ExecutorAgent:batch_2:0001",
  "agent_name": "ExecutorAgent",
  "batch_id": 2,
  "candidate_id": "cand_batch_2_family_fact_ss_dd_item_manager_moy_year_0001",
  "input_artifact_paths": ["..."],
  "output_artifact_paths": ["..."],
  "elapsed_ms": 1200,
  "event": "mv_materialize_success",
  "error": null
}
```

`event_id` 建议由代码生成，保持同一 `run_id` 内唯一、稳定、可读，例如 `{run_id}:{agent_name}:batch_{batch_id}:{seq}`。如果事件不属于某个 batch，可用 `global` 替代 `batch_{batch_id}`。

普通 Agent 级别事件可以把 `candidate_id` 置为 `null`；涉及单个 MV Candidate 的事件，例如 `mv_materialize_success`、`mv_materialize_failed`、`mv_candidate_skipped`，必须填写对应 `candidate_id`。如果 MV Candidate 物化失败，`event` 使用 `mv_materialize_failed`，并在 `error` 中记录失败原因，例如依赖 MV 表不存在、依赖关系不满足或 SQL 不可生成。

### 4.3 通用 `LLMRulesAgent`

除 `SQLLoaderAgent` 和 `ExecutorAgent` 外，其余 Agent 可继承一个通用 `LLMRulesAgent`。

执行模板：

```text
1. 根据 agent_name 读取 rules/{agent_name}.md
2. 读取 rules/_prompt_template.md
3. 根据 `input_artifact_paths` 读取必要输入
4. 拼接 prompt
5. 调用 llm.infer(prompt, load_json=True)
6. 用 schemas.py 中的 Pydantic schema 校验
7. 写入目标 artifact
8. 追加 run log
9. 返回输出 artifact 路径
```

这样每个 LLM Agent 的代码差异只剩：

```text
agent_name
rules_path
examples_path
input_artifact_keys
output_schema
output_artifact_path
```

### 4.4 Agent 输入输出约定

第一版 Agent 之间只传显式路径，避免隐藏状态。

| Agent | 主要输入 | 主要输出 |
|---|---|---|
| `SQLLoaderAgent` | `sql_paths` | `{run_id}/00_raw_sql/sql_manifest.json` |
| `FeatureAgent` | SQL manifest | QueryBlock、`query_to_qbs`、`qb_to_query` |
| `FamilyAgent` | QueryBlock artifact | QueryFamily artifact |
| `BatchClusterAgent` | `batch_classification.csv`、SQL manifest、QueryBlock、`query_to_qbs`、QueryFamily | ComplexityBatch artifact |
| `RewriteAgent` | original batch SQL、QueryBlock、`materialized_mvs.json` | historical rewrite 或 final rewrite SQL |
| `BatchMVAgent` | historical rewrite SQL、原始 QueryBlock、原始 QueryFamily、当前 batch 的 `family_groups`、`materialized_mvs.json`、全局 `complexity_batches.json` 和 `query_families.json` 只读上下文 | 当前 batch 的 MV Candidate JSON、MV build SQL |
| `ExecutorAgent.materialize_mvs(...)` | MV Candidate JSON、MV build SQL | 成功物化的 MV 追加到 `materialized_mvs.json`；skip / failed Candidate 写入 run log |
| `ExecutorAgent.run_queries(...)` | final rewrite SQL、rewrite meta、ComplexityBatch | dry-run execution order JSON、run log |
| `CoverageSummaryBuilder` | SQL manifest、Feature status、QueryBlock、QueryFamily、ComplexityBatch、MV Candidate、rewrite meta、execution order、run log | `{run_id}/08_coverage/coverage_summary.json` |
| `SelfIterationAgent` | `run_log.jsonl`、`coverage_summary.json`、`family_candidates.json`、MV Candidate、rewrite meta、execution order | `{run_id}/07_feedback/feedback_rules_{run_id}.json` |

## 5. 通用 LLM + Rules Prompt 模板

建议保存为 `llm_demo/rules/_prompt_template.md`。它只保存通用 prompt 骨架，不直接保存任何具体 Agent 的示例。每个 Agent 的具体规则和示例写在对应的 `rules/{agent_name}.md` 中，运行时整体注入到模板。

````markdown
# 角色

你是 ETL 物化视图查询编排加速原型系统中的 `{agent_name}`。

# 任务

{task}

# Agent 专属规则与示例

{agent_rules_md}

# 全局约束

1. SQL/query_id 是执行单位。
2. QueryBlock/qb_id 只是分析单位。
3. 不要编造 query_id、qb_id、表名、字段名、MV 名称或 SQL 谓词。
4. QueryBlock / QueryFamily 只来自原始 workflow SQL，不随 rewrite 重新生成。
5. 如果输入信息不足，采用保守 fallback，并说明原因。
6. 必须保持 SQL 语义等价。如果无法确定语义等价，返回 fallback，不要进行不安全改写。
7. 输出必须是合法 JSON。不要输出 Markdown 代码块、注释或额外解释文本。
8. 各阶段的标识符必须保持稳定。
9. MV candidate 只能在当前 batch 内生成。
10. 已物化成功的 MV 对后续 batch 全局可用。
11. 可以只读参考完整的 `complexity_batches.json` 和 `query_families.json` 来判断当前 batch MV 的后续复用价值，但不能为未来 batch 生成 MV Candidate。
12. MV Candidate 必须有当前 batch 的 Query 或 QueryFamily 依据；下游信息只能影响 `decision` 和 `reason`，不能单独触发候选生成。
13. MV 的 `output_columns` 是 MV 表真实物理列名；源字段身份必须写入 `column_mappings`，不要把 `date_dim.d_year` 作为 MV 输出列名。
14. 使用 MV rewrite 时只能引用 MV 物理列名，并保持 original SQL 的输出列名；无 alias 表达式也要保留 Spark 输出名。

# 当前上下文

```json
{context_json}
```

# 输入 Artifact

```json
{input_artifacts_json}
```

# 必须遵守的输出 Schema

```json
{output_schema_json}
```

# 输出要求

只返回一个严格符合输出 Schema 的 JSON object。
````

这里的 `{agent_rules_md}` 是从 `rules/{agent_name}.md` 读取的完整内容。也就是说，`_prompt_template.md` 只提供“示例应该出现在 prompt 的哪个位置”，不维护具体示例内容。

各 Agent 的 `rules/*.md` 应同时包含本 Agent 的判断标准和少量示例，避免再拆出 `rules/examples/` 目录。建议每个文件保持以下结构：

```text
# 职责

# 规则

# 示例
```

其中“示例”只保留少量高质量输入输出样例，用于约束输出格式和关键判断边界，避免 prompt 过长。

各 Agent 的 `rules/*.md` 建议覆盖：

1. `feature_agent.md` 说明如何从 SQL 提取 QueryBlock。
2. `family_agent.md` 说明如何按结构化 QueryBlock 事实、family candidate 证据和 join skeleton 聚合 QueryFamily；`family_key` 只属于最终 QueryFamily 输出，不属于 Feature artifact。
3. `batch_cluster_agent.md` 说明如何以 SQL 为单位划分 batch。
4. `batch_mv_agent.md` 说明如何在当前 batch 内生成 MV。
5. `rewrite_agent.md` 说明何时使用 MV、何时 fallback。
6. `self_iteration_agent.md` 说明如何基于日志输出规则建议。

各 Agent 的“示例”小节建议包含，并随设计确认持续更新：

1. `feature_agent.md`：给出 q42/q52 片段到 QueryBlock JSON 的示例。
2. `family_agent.md`：给出 predicate 兼容、`predicate_shapes`、重复 family evaluate / merge 的示例。
3. `batch_cluster_agent.md`：给出按外部分类 CSV 生成 global batch，并在 batch 内组织 `family_groups` 的示例。
4. `batch_mv_agent.md`：给出 ETL superset MV、`mv_predicates`、`residual_filters`、`generalized_predicates` 的示例。
5. `rewrite_agent.md`：给出基于 superset MV 加 residual filter / roll-up 的 rewrite 示例，以及必须 fallback 的对照示例。
6. `executor_agent.md`：说明 dry-run 物化状态维护、execution order 输出和 query 依赖记录。
7. `self_iteration_agent.md`：给出 run log 到 rule suggestion 的反馈示例。

实现文档只保留这些示例的摘要；真正注入 prompt 的示例必须写在对应 `rules/{agent_name}.md` 中，避免 `_prompt_template.md` 或方案文档变成超长规则库。

## 6. 运行主线

### 6.1 执行前编排

执行前只做 workflow 顺序编排，不生成全局 MV candidate。

```text
sql_manifest_path = SQLLoaderAgent(code).run(sql_paths)
query_block_paths = QueryAnalysisRunner(code).run_all(sql_manifest_path)
family_path = FamilyAgent(llm+rules).run(query_block_paths)
batch_path = BatchClusterAgent(code).run_from_classification_csv(
  classification_csv_path,
  sql_manifest_path,
  query_block_paths,
  family_path
)
```

当前短期链路中，ComplexityBatch 的执行顺序来自外部分类 CSV 的 batch 标签顺序：

```text
Batch-1
Batch-2
Batch-3
Batch-4
Batch-5
Other
```

`BatchClusterAgent` 的当前判定过程分两层：

1. 代码层按 `batch_classification.csv` 建立顶层 `query_id -> batch_type`，CSV 中的分类结果是唯一 batch 判断来源。
2. 代码层再根据 QueryFamily、`query_to_qbs` 和 QueryBlock 可用性组装 batch 内 `family_groups`，供 `BatchMVAgent` 生成 MV Candidate 使用。

因此，最终执行顺序仍是全局 batch 顺序，不是按 family 分别执行。`ComplexityBatch` 内部用 `family_groups` 保存 batch 内的 family 组织关系；带 `unsupported_reasons` 的 QueryBlock 不参与 `family_groups` 和 MV candidate 生成，但完整 SQL 仍可以在 final rewrite 阶段以 fallback 方式输出。

输出：

| Artifact | 说明 |
|---|---|
| `artifacts/{run_id}/00_raw_sql/sql_manifest.json` | 原始 SQL 文件 manifest，记录 query_id、原始路径、相对路径和大小，不复制 SQL Text |
| `artifacts/{run_id}/01_query_blocks/query_blocks.json` | LLM 提取的 QueryBlock |
| `artifacts/{run_id}/01_query_blocks/query_to_qbs.json` | SQL 到 QB 索引 |
| `artifacts/{run_id}/01_query_blocks/qb_to_query.json` | QB 到 SQL 索引 |
| `artifacts/{run_id}/02_families/query_families.json` | LLM 生成的 QueryFamily |
| `artifacts/{run_id}/03_batches/complexity_batches.json` | 根据外部分类 CSV 生成的 SQL 级 batch，并包含 batch 内 family_groups |

### 6.2 Batch 编排

每个 batch 先使用历史已物化 MV rewrite 当前 batch 的 original SQL，得到 historical rewrite SQL；再基于 historical rewrite SQL 生成本 batch MV。由于 historical rewrite SQL 可能已经引用历史 MV，当前 batch 的新 MV 可以基于这些历史 MV 构建，形成增量式 MV 扩充。完成物化后，final rewrite 必须重新以当前 batch 的 original SQL 为输入，使用更新后的 Materialized View State 生成最终执行 SQL。

第一版把这段循环收敛到 code-level `BatchWorkflowRunner` 中，而不是长期写在 notebook 里。`BatchWorkflowRunner` 只做顺序编排和 artifact 路径管理，不判断 MV 是否应该生成、不判断 SQL 是否可以 rewrite，也不改变任何 Agent 的业务职责。

```text
BatchWorkflowRunner.run_all_batches(
  complexity_batches_path,
  sql_manifest_path,
  query_block_paths,
  family_path,
  materialized_mvs_path
)

内部固定执行：

for batch in global complexity_batches:
  1. RewriteAgent 使用 historical materialized_mvs.json 改写当前 batch 的 original SQL，生成 historical rewrite SQL
     每条 SQL 都必须输出 {query_id}_rewritten.sql 与 {query_id}_rewrite_meta.json
     即使没有可用历史 MV，也必须输出与 original SQL 等价的 rewritten SQL，并记录 used_mv_ids = [] 和 fallback_reason
  2. BatchMVAgent 分两次 LLM + rules 调用处理当前 batch：
     2.1 基于 historical rewrite SQL Text + 原始 QueryBlock / 原始 QueryFamily / 当前 batch family_groups 生成 candidate_mv_output
     2.2 基于相同输入 + candidate_mv_output 执行 evaluate，修正当前 batch 边界、family 边界、mv_type、predicate / residual_filter、depends_on_mv_ids、column_mappings、build_sql 和 decision
     可只读参考完整的 query_families.json 与 complexity_batches.json 判断后续复用价值
     如果 build SQL 依赖历史 MV，必须记录 depends_on_mv_ids
     如果 decision = materialize，output_columns 必须是 MV 物理列名；普通物理列默认不 AS，聚合表达式必须显式 AS measure mv_column
     使用 tpcds_simple.json 校验 column_mappings 中的 source_table.source_column 是否存在
     即使 used_mv_ids = []，也应继续尝试生成当前 batch 的 MV Candidate
     对外落盘的 batch_{batch_id}_mv_candidates.json 使用 evaluate 后的最终结果
  3. ExecutorAgent.materialize_mvs(...) 物化 decision = materialize 的 MV Candidate；成功则更新 materialized_mvs.json，失败只写 run_log.jsonl
  4. RewriteAgent 使用更新后的 materialized_mvs.json 重新改写当前 batch 的 original SQL，生成 final rewrite SQL；
     每条 SQL 都必须输出 {query_id}_rewritten.sql 与 {query_id}_rewrite_meta.json
     若无法证明语义等价，final rewrite 也必须 fallback 到与 original SQL 等价的 rewritten SQL
  5. ExecutorAgent.run_queries(...) 对 final rewrite SQL 做 dry-run 顺序规划，并根据 rewrite meta 的 used_mv_ids 写出 execution order 中的 query 依赖
```

因此 notebook 或后续 `main.py` 只需要调用 `BatchWorkflowRunner`，而不是手写每个 batch 的五步流程。这样可以保证全量 SQL 实验时每个 batch 的 artifact 命名、状态传递和日志行为一致。

对应 Artifact 写入：

```text
historical rewrite SQL -> {run_id}/05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewritten.sql
historical rewrite meta -> {run_id}/05_rewritten_sql/batch_{batch_id}/historical_rewrite/{query_id}_rewrite_meta.json
MV Candidate          -> {run_id}/04_batch_mvs/batch_{batch_id}_mv_candidates.json
MV build SQL          -> {run_id}/04_batch_mvs/batch_{batch_id}_mv_build.sql
Materialized View State -> {run_id}/04_batch_mvs/materialized_mvs.json
final rewrite SQL     -> {run_id}/05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewritten.sql
final rewrite meta    -> {run_id}/05_rewritten_sql/batch_{batch_id}/final_rewrite/{query_id}_rewrite_meta.json
execution order       -> {run_id}/06_execution_logs/batch_{batch_id}_execution_order.json
coverage summary      -> {run_id}/06_execution_logs/coverage_summary.json
run log               -> {run_id}/06_execution_logs/run_log.jsonl
```

第一版避免同一个 batch 内无限迭代：

```text
historical MV rewrite
  -> batch-local MV generation
  -> materialize selected MVs
  -> final rewrite
  -> execute
```

如果某个 batch 没有 SQL，则跳过该 batch，不额外拆分子 batch。当前 batch 是工作流编排层级，不是并发资源调度单位。

### 6.3 Coverage Diagnostics

全量 SQL 实验结束后，运行 code-level `CoverageSummaryBuilder` 生成：

```text
{run_id}/06_execution_logs/coverage_summary.json
```

它只读取已有 artifact，不调用 LLM，不修改任何业务结果。第一版输入包括：

```text
00_raw_sql/sql_manifest.json
01_query_blocks/feature_extract_status.json
01_query_blocks/query_blocks.json
02_families/query_families.json
03_batches/complexity_batches.json
04_batch_mvs/batch_{batch_id}_mv_candidates.json
04_batch_mvs/materialized_mvs.json
05_rewritten_sql/batch_{batch_id}/{stage}/{query_id}_rewrite_meta.json
06_execution_logs/batch_{batch_id}_execution_order.json
06_execution_logs/run_log.jsonl
```

`coverage_summary.json` 的职责是回答“这轮全量实验覆盖到了哪里、失败在哪里、fallback 在哪里”，而不是决定下一步该如何 rewrite 或物化 MV。

最小结构建议：

```json
{
  "run_id": "20260601_153000",
  "total_queries": 99,
  "feature": {
    "success_count": 78,
    "partial_success_count": 12,
    "failed_count": 6,
    "unsupported_count": 3,
    "partial_success_query_ids": ["q4", "q17"],
    "failed_query_ids": ["q14", "q23"],
    "unsupported_reasons_top": [
      {"reason": "correlated_subquery", "count": 2}
    ]
  },
  "family": {
    "family_count": 18,
    "covered_query_count": 86,
    "unassigned_query_ids": ["q14", "q23"]
  },
  "batch": {
    "Batch-1": 12,
    "Batch-2": 24,
    "Batch-3": 41,
    "Batch-4": 13,
    "Batch-5": 6,
    "Other": 3
  },
  "mv": {
    "candidate_count": 35,
    "materialized_count": 11,
    "skipped_count": 20,
    "failed_count": 4
  },
  "rewrite": {
    "historical_fallback_count": 90,
    "final_fallback_count": 62,
    "rewritten_with_mv_count": 28
  },
  "execution": {
    "planned_query_count": 90,
    "queries_with_mv_dependency_count": 28
  }
}
```

该 artifact 可以被 `SelfIterationAgent` 读取，用于提出规则优化建议。例如，如果 `final_fallback_count` 很高且 fallback 原因集中在输出 alias 缺失，则 SelfIterationAgent 可以建议收紧 `rewrite_agent.md` 中的输出列名规则。

## 7. 规则文件设计

每个 LLM Agent 都有一个对应规则文件，规则只写判断标准，不写算法实现。

### 7.1 `rules/feature_agent.md`

职责：

```text
给定 SQL 文本，提取 QueryBlock。
```

规则重点：

1. SQL 是执行单位，QueryBlock 是分析单位。
2. 每个 SQL 至少提取一个 outer QueryBlock。
3. CTE / subquery 可以提取为独立 QueryBlock；如果能提取清晰物理表 join domain，它们后续可以进入 QueryFamily 和 MV Candidate。
4. 必须输出 `query_id`、`qb_id`、`scope_type`、`relation_instances`、`tables`、结构化 `join_edges`、`predicates`、`projections`、`group_by_exprs`、`aggregate_exprs`、`order_by_exprs`、`limit` 和 `unsupported_reasons`。
5. 不输出 `complexity_type`、`block_shape`、`family_key` 或 batch 判断字段。
6. Feature artifact 不保留 SQL 局部 alias，包括 table alias、relation alias 和 SELECT 输出 alias。SQL alias 只允许在解析过程中使用，evaluate 后必须消失。
7. `tables` 和所有表达式字段必须使用物理表名、物理字段名和稳定 `relation_instance_id`；例如 `date_dim dt` 的 `dt.d_year` 应输出为 `date_dim.d_year`。
8. 同一物理表多次出现时，用物理实例 ID 消歧，例如 `date_dim__sold_date`、`date_dim__returned_date`；无法稳定判断 role 时用 `date_dim__1`、`date_dim__2`，但这些编号不是 SQL alias。
9. FeatureAgent 在 LLM 提取后增加 LLM + rules evaluate 环节，输入为原始 SQL 与 `candidate_feature_output`，输出仍为修正后的完整 FeatureOutput。
10. evaluate 阶段检查输出中是否仍有 SQL alias、未知表名前缀、未知字段或不一致的 `relation_instance_ids`；不确定信息写入 `unsupported_reasons`，不要编造字段。
11. 全量模式下 QueryBlock 应补充 `block_name`、`parent_qb_id`、`depends_on_qb_ids`、`structural_flags`；`block_name` 使用稳定分析名，不使用 CTE 名或 subquery alias。
12. 第一版 FeatureAgent 读取完整单条 SQL Text，不要求 LLM 处理全量 manifest，也不要求把单条 SQL 预拆成多个 prompt chunk。
13. TPC-DS Spark SQL 一般不是超长 SQL 上下文问题；如果单条 SQL 结构复杂，优先输出可用 QueryBlock 与 `partial_success`，而不是要求 SQLGlot 预拆分后再分别调用 LLM。
14. Feature retry 时不向 prompt 注入上一轮错误、schema validation 失败详情或 evaluate 失败详情；这些内容只作为日志和人工复盘证据。
15. 不要因为某个 CTE / subquery / set branch unsupported 就让整条 SQL 提取失败；应尽量输出其他可用 QueryBlock，并让 QueryAnalysisRunner 标记为 `partial_success`。
16. 带 `unsupported_reasons` 的 QueryBlock 可以保留在输出中，但必须明确原因，供下游 skip / fallback；不要把 unsupported block 伪装成可安全生成 MV 或 rewrite 的 QueryBlock。

输出 JSON：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "scope_type": "outer",
      "block_name": "outer",
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
        }
      ],
      "predicates": [
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

### 7.2 `rules/family_agent.md`

职责：

```text
给定 QueryBlock 列表，按 upstream join-domain MV / shared superset MV 覆盖关系聚合 QueryFamily。
```

规则重点：

1. 聚合单位是 `qb_id`。
2. `QueryFamily` 表示一组可由同一个 upstream join-domain MV 或 shared superset MV 覆盖的 QueryBlock，不是简单 SQL 相似簇。
3. CTE / subquery QueryBlock 与 outer QueryBlock 一样可以进入 family；是否合并只看 join domain、predicate、measure 和 rewrite 安全性，不因为 `scope_type = cte` 自动降级。
4. 带 `unsupported_reasons` 的 QueryBlock 默认不进入 QueryFamily；它可以作为 candidate 被拒绝的证据，但不能被当作可安全共享 MV 的成员。
5. `FamilyAgent` 的输入不应是无筛选的全量 QueryBlock 列表，而应是 `FamilyCandidateBuilder` 生成的 candidate family groups。
6. `FamilyCandidateBuilder` 用代码计算 table set、core fact table、Jaccard、Containment、predicate column set、measure set、group-by set 等可量化特征。
7. `core_fact_table_set` 由 `FamilyCandidateBuilder` 基于 TPC-DS fact table 白名单识别；FeatureAgent / LLM 不负责决定 fact table 身份。
7. 如果 `core_fact_table_set` 为空，QueryBlock 可进入 `family_type = "dimension_only"`；dimension-only family 只能和 dimension-only family 比较，不能并入 fact-based family。
8. dimension-only family 可服务维表过滤、维表映射或 CTE 局部 MV，但不代表事实表 join domain。
9. dimension-only MV Candidate 可以被 BatchMVAgent 物化；它可服务 QueryBlock-local rewrite，也可作为后续 fact-based MV 的 `depends_on_mv_ids`，但这不改变 family 不混合原则。
10. 如果 `FamilyCandidateBuilder` 需要从 SQL 表达式中识别谓词列、聚合函数、join key 或输出表达式，必须使用 SQLGlot AST；不得用正则表达式判断 SQL 结构。
11. `family_candidates.json` 以 candidate group 为主，每个 group 内保存必要 `pair_evidence`；不保存完整 O(n²) pair 矩阵。
12. blocking 使用两级策略：先按 family_type / core fact table 粗分，再按 table set signature 建 primary group。
13. 对同一 core fact table 内存在 table set containment 的 primary groups，只有满足 `Containment >= 0.8`、join graph 不出现明显多事实表混合、predicate column set 不完全不相交时，才记录 `related_groups`。
14. 如果 predicate column set 的交集只包含 `date_dim.*`、`time_dim.*` 这类 date/time 维度列，则必须额外满足 measure set 或 group-by set 兼容，才允许记录 `related_groups`。
15. 如果 predicate column set 存在非 date/time 维度交集，则允许进入 `related_groups` evaluate；最终是否合并仍由 LLM 安全门判断。
16. measure set 第一版按严格等价判断兼容：aggregate function 和 source column 必须相同；`COUNT(*)` 只和 `COUNT(*)` 兼容；`COUNT(DISTINCT x)` 暂不与其他 measure 兼容。
17. group-by set 第一版按 roll-up compatible 判断兼容：不要求完全相同；如果 measure、core fact table 和 join graph 兼容，且所有 group-by expr 都能作为 MV 维度列保留，则允许使用 `union_group_by_exprs` 支持后续 roll-up。
18. 表达式型 group-by、函数型 group-by 或无法稳定保留的派生列，不能仅凭 group-by 兼容放行，必须进入 LLM evaluate 或 skip。
19. join graph 第一版采用代码级基础 hard gate：`FamilyCandidateBuilder` 生成规范化 `join_signature`；同一 primary group 内先按 `join_signature` 拆分 sub-group。
20. `join_signature` 使用 fact-centered edge signature，例如 `store_sales.ss_sold_date_sk->date_dim.d_date_sk|store_sales.ss_item_sk->item.i_item_sk`；`normalized_join_edges` 保留原始等值连接的规范化证据。
21. `join_signature` 不同的 group 不直接合并；如果其他条件满足，只能作为 `related_groups` 交给 LLM evaluate。
22. 如果 join key 涉及未知表达式、非等值条件、OR 条件或复杂函数条件，`join_signature = "unknown"`，该候选必须进入 LLM evaluate 或 skip。
23. `related_groups` 只表示“值得交给 LLM evaluate 的宽表覆盖候选”，不表示代码层已经决定合并。
24. 第一版优先保守合并，不做激进模糊合并；错误合并会影响 MV rewrite 安全，漏合并只影响优化机会。
25. family 中记录 common tables、common join skeleton、common predicates、predicate shapes、group by 分叉和 measure 并集。
26. 如果合并依据不足，保持分离。
27. `FamilyAgent` 采用两次 LLM 调用：第一次基于 candidate family groups 生成 `candidate_query_families`，第二次 evaluate 并修正输出。
28. evaluate 阶段输入为 QueryBlock 列表、family candidate groups 和 `candidate_query_families`，输出仍是修正后的完整 QueryFamily。
29. evaluate 阶段重点检查是否存在重复 family、可合并 family、错误拆分、错误合并、成员 QueryBlock 归属错误、common 字段错误，以及最终 `family_key` 是否与 family 的 join domain / predicate / measure 边界一致。
30. `family_id` 是 BatchClusterAgent 与 BatchMVAgent 维护单 family 边界的键，必须全局唯一；它不只按表集合命名，而应按“可共享 MV 的语义边界”命名。
31. `family_id` 第一版采用三层格式：`family_{family_type}_{domain}_{distinguishing_feature}`。`family_type` 使用 `fact`、`dim`、`cte`、`mixed`；`domain` 使用核心 fact table 或核心 dimension / join domain；`distinguishing_feature` 使用关键 predicate、measure 或 derived semantics。
32. fact-based family 优先使用 `family_fact_{fact_table}_{main_dims}_{predicate_or_measure_feature}`，例如 q42/q52 可命名为 `family_fact_store_sales_date_dim_item_manager_month_year`，或使用稳定缩写 `family_fact_ss_dd_item_manager_moy_year`。
33. dimension-only family 不能只写表名，必须包含关键过滤或派生语义。例如 `q46.outer` 可命名为 `family_dim_customer_address_city_mismatch`，`q34.outer` 可命名为 `family_dim_customer_dn_count_range`；不应使用 `family_customer_customer_address_dn` 这类弱区分命名。
34. CTE / derived QueryBlock 应体现派生含义，例如 `customer_total_return` 可命名为 `family_cte_store_returns_customer_total_return`；如果 block 名是 `dn` 这类弱语义别名，应根据来源逻辑命名，而不是直接把 `dn` 当作核心语义。
35. 同一表集合但 predicate shape 不同，必须进入命名区分，例如 `family_fact_ss_dd_item_year_manager` 与 `family_fact_ss_dd_item_category`；同一表集合但 measure 不同也必须进入命名区分，例如 `family_fact_ss_dd_item_sum_sales_price`、`family_fact_ss_dd_item_sum_net_paid`、`family_fact_ss_dd_item_count`。
36. q42/q52 这种 measure 相同但 group by 不同的 QueryBlock 仍应进入同一 family，因为可以用更细粒度 MV roll-up 覆盖；不需要把 category / brand 拆成两个 family。
37. 完全重复的 family 应去重；同名但内容不同的 family 必须在 FamilyAgent evaluate 阶段生成语义可区分的不同 `family_id`。代码层追加 `__2` 只是避免流程中断的最后防线，不是设计命名规则，并且必须写入 run log 供 SelfIterationAgent 反馈。
38. family candidate 第一层使用表集合量化：`Jaccard(A,B)=|T(A)∩T(B)|/|T(A)∪T(B)|` 衡量相似度，`Containment(A,B)=|T(A)∩T(B)|/min(|T(A)|,|T(B)|)` 识别宽表覆盖关系。
39. core fact table 是 QueryBlock join domain 中承载事实记录或主要 measure 来源的中心表；TPC-DS 第一版可优先识别 `store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。
40. candidate 分层规则：`strong candidate` = `Jaccard = 1.0`，或 `Containment = 1.0` 且 core fact table 相同；`medium candidate` = `Jaccard >= 0.6` 且 core fact table 相同，或 `Containment >= 0.8` 且较小 QueryBlock 的表集合主要被较大 join domain 覆盖；低于该阈值默认保持分离。
41. core fact table 相同是 family 合并 hard gate；如果 core fact table 不同，即使 Jaccard / Containment 较高，也默认保持分离。
42. 多 fact table QueryBlock 只允许与具有完全相同 `core_fact_table_set`、且 join graph 可证明不会引入行数膨胀的 QueryBlock 合并；否则单独成 family。
43. 单 fact QueryBlock 和多 fact QueryBlock 不混合进同一个 family；例如 `{store_sales}` 不能和 `{store_sales, store_returns}` 合并。
44. dimension-only QueryBlock 不混入 fact-based family；如果后续 fact query 使用该维表 MV，应通过 RewriteAgent 的局部 rewrite 或后续 MV 依赖表达，而不是 family 合并表达。
45. Jaccard 和 Containment 只负责生成 candidate，不直接决定最终合并。
46. family 最终合并采用安全门：core fact table、join graph、predicate shape、measure compatibility 和 roll-up safety 必须可证明兼容。
47. family 合并采用 predicate 兼容标准，而不是要求 predicate 完全相同。
48. 第一版 predicate shape 兼容只允许两类情况：同一过滤列常量不同，例如 `date_dim.d_year = 2000` 与 `date_dim.d_year = 2001`；或者一方比另一方多出过滤条件，且多出的过滤列可在 MV 中保留并作为下游 residual filter。
49. 如果两个 QueryBlock 的过滤列集合完全不同，例如一个只过滤 `date_dim.d_year`，另一个只过滤 `item.i_manager_id`，即使 core fact table、join graph 和 measure 兼容，也必须拆成不同 family。
50. 如果两个 family 的 QueryBlock 具有相同或可证明等价的 join skeleton、同构过滤列结构但常量值不同、兼容 measure，并且后续可共享 MV，应在 evaluate 阶段合并为同一个 family。
51. `common_predicates` 只记录 family 内完全相同的完整谓词，例如所有成员都有 `item.i_manager_id = 1`。
52. `predicate_shapes` 记录同构过滤列但常量值可能不同的谓词形状，例如 `date_dim.d_year = <CONST>`、`date_dim.d_moy = <CONST>`。
53. `predicate_shapes` 中的字段名必须使用物理表名，不使用 SQL alias。
54. 如果过滤列集合差异过大、predicate 语义不清或 family 是否可合并无法证明，保持分离，并在后续 SelfIterationAgent 的反馈中记录规则改进建议，而不是在 BatchMVAgent 中跨 family 生成 MV。

以 q42 / q52 为例，二者的表集合均为 `{date_dim, store_sales, item}`，`Jaccard = 1.0`，`Containment = 1.0`，core fact table 均为 `store_sales`，join graph、predicate shape 和 measure 均兼容，因此可以归入同一 `QueryFamily`。如果另一个 QueryBlock 使用 `web_sales` 作为事实表，即使也连接 `date_dim` 和 `item`，第一版也不与 `store_sales` family 合并。

输出 JSON：

```json
{
  "query_families": [
    {
      "family_id": "family_fact_ss_dd_item_manager_moy_year",
      "family_key": "store_sales-date_dim-item|manager_moy_year|sum_ss_ext_sales_price",
      "family_type": "fact_based",
      "core_fact_table_set": ["store_sales"],
      "members": ["q42.outer", "q52.outer"],
      "common_tables": ["store_sales", "date_dim", "item"],
      "common_predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year = 2000"
      ],
      "predicate_shapes": [
        "date_dim.d_year = <CONST>",
        "date_dim.d_moy = <CONST>",
        "item.i_manager_id = <CONST>"
      ],
      "union_group_by_exprs": [
        "date_dim.d_year",
        "item.i_category_id",
        "item.i_category",
        "item.i_brand_id",
        "item.i_brand"
      ],
      "union_measure_exprs": ["sum(store_sales.ss_ext_sales_price)"]
    }
  ]
}
```

### 7.3 `rules/batch_cluster_agent.md`

职责：

```text
给定外部 batch 分类 CSV、SQL manifest、QueryBlock、query_to_qbs 和 QueryFamily，以 SQL/query_id 为单位生成全局 ComplexityBatch，并在每个 batch 内保留 family_groups。
```

规则重点：

1. batch 分配单位是 SQL/query_id，不是 qb_id。
2. 顶层 `query_id -> batch_type` 由外部分类 CSV 决定，不由 FeatureAgent、FamilyAgent 或 LLM 根据 QueryBlock 结构推断。
3. `batch_type` 使用 CSV 中的标签：`Batch-1`、`Batch-2`、`Batch-3`、`Batch-4`、`Batch-5` 或 `Other`。
4. CSV 中的每条 SQL 必须能映射到 SQL manifest 中的 `query_id`；缺失、重复或未知 SQL 必须在代码层报错或写入 run log。
5. 当前不处理并发上限、子 batch 拆分。
6. 先在每个 QueryFamily 内收集可用成员 QueryBlock，再按所属 SQL 的 CSV batch 组织到对应 global batch 的 `family_groups`。
7. 不输出独立的 `FamilyBatch` artifact；family 内部分层结果只体现在 `ComplexityBatch.family_groups` 中。
8. 顶层 `query_ids` 表示该 global batch 要处理的完整 SQL，必须去重。
9. 如果一个 SQL 的多个 QueryBlock 属于不同 family，该 `query_id` 可以出现在多个 `family_groups` 中；这是 MV 生成视角的重复，不代表该 SQL 会被执行多次。
10. 每个 `family_groups[].qb_ids` 只能包含该 family 的可用 QueryBlock；`family_groups[].query_ids` 必须由这些 QB 对应 query_id 去重得到，并属于同一个 batch 的顶层 `query_ids`。
11. `family_groups[].qb_ids` 默认只包含可用 QueryBlock，不包含带 `unsupported_reasons` 的 QueryBlock。
12. 如果 unsupported outer QueryBlock 导致完整 SQL 语义边界不清，但某个 CTE / subquery QueryBlock 可用，该 SQL 仍按 CSV 分类进入对应 batch；final rewrite 必须由 RewriteAgent 保守 fallback 或只做可证明安全的 QueryBlock-local rewrite。
13. 当前 CSV 驱动入口不需要两次 LLM 调用；evaluate 责任由代码层 schema 校验、CSV 覆盖校验、batch label 校验和 `family_groups` 合法性校验承担。

输出 JSON：

```json
{
  "complexity_batches": [
    {
      "batch_id": 1,
      "batch_type": "Batch-1",
      "query_ids": [],
      "family_groups": []
    },
    {
      "batch_id": 3,
      "batch_type": "Batch-3",
      "query_ids": ["q42", "q52"],
      "family_groups": [
        {
          "family_id": "family_fact_ss_dd_item_manager_moy_year",
          "query_ids": ["q42", "q52"],
          "qb_ids": ["q42.outer", "q52.outer"]
        }
      ]
    }
  ]
}
```

### 7.4 `rules/batch_mv_agent.md`

职责：

```text
给定当前 batch 的 historical rewrite SQL Text、原始 QueryBlock、原始 QueryFamily、当前 batch 的 `family_groups`、全局 materialized_mvs，以及完整的 complexity_batches / QueryFamily 只读上下文，生成当前 batch 的 MV spec 和 CTAS SQL。
```

实现方式：

```text
第一次 LLM 调用：生成 candidate_mv_output。
第二次 LLM 调用：evaluate candidate_mv_output，检查并修正是否满足 batch-local generation、family 边界、shared upstream superset MV、依赖 DAG 和 rewrite 安全约束，最终输出修正后的完整 MV Candidate JSON 与 build SQL。
```

当前 demo 中，MV Candidate、`build_sql`、`decision` 和 `reason` 都由 BatchMVAgent 的 LLM + rules 生成。代码层不主动生成候选骨架，也不自动补全 SQL；代码层只负责验证输出是否满足 schema、batch-local、single-family、column_mappings、SQLGlot 解析和依赖 DAG 等契约。

规则重点：

1. MV 只在 batch 内生成。
2. 执行前不生成全局 MV candidate。
3. Batch-k 的 MV 基于当前 batch 的 historical rewrite SQL Text。
4. 如果 historical rewrite 没有使用历史 MV，即 `used_mv_ids = []`，仍然必须尝试为当前 batch 生成 MV Candidate。
5. Batch-1 初始 Materialized View State 为空，正是从这个流程开始生产第一批 MV。
6. 第一版直接读取完整的 `complexity_batches.json` 和全局 `query_families.json`，不额外新增 `future_reuse_summary.json`。
7. 这些只读上下文只能用于判断当前 batch MV 是否可能服务后续 batch。
8. 只读上下文不能触发未来 batch 的 MV Candidate 生成；MV Candidate 的 `source_batch_id` 必须等于当前 batch。
9. MV Candidate 必须来自当前 batch 的 Query 或 QueryFamily，不能只因为后续 batch 可能有用而生成。
10. 下游 batch / 全局 QueryFamily 只读上下文只能影响 `decision` 和 `reason`，不能单独构成候选生成依据。
11. 原始 QueryBlock / QueryFamily 只作为结构提示和 family 归属依据，不因 SQL rewrite 重新生成或覆盖。
12. 允许 build SQL 基于已成功物化的历史 MV 构建新 MV，这是增量式 MV 扩充的核心路径。
13. 如果 build SQL 引用了历史 MV，必须在 `depends_on_mv_ids` 中记录直接依赖的 MV；如果不依赖历史 MV，则使用空数组。
14. `depends_on_mv_ids` 只能引用 `materialized_mvs.json` 中已存在且 `available_from_batch <= current_batch_id` 的 MV。
15. 一个 MV Candidate 可以依赖多个历史 MV，但依赖关系必须保持 DAG，不能依赖自身或形成循环。
16. 默认先构造 shared upstream superset MV，而不是先在 detail MV 和 aggregate MV 中二选一。
17. 只生成能够从当前 batch Query / QueryFamily 得到结构依据的 MV。
18. 后续 batch / 全局 QueryFamily 只读上下文只能影响 `decision` 和 `reason`，不能成为结构化 target，也不能生成 downstream-only Candidate。
19. 输出必须包含 source query、target query、group by、measure、output columns、column mappings、decision、reason 和 depends_on_mv_ids；如果 Candidate 来源或目标是 CTE / subquery QueryBlock，还必须包含对应的 `source_qb_ids` / `target_qb_ids`。
20. 每个 MV Candidate 必须给出稳定的 `candidate_id`，同一 `run_id` 内唯一。
21. 每个 MV Candidate 必须给出非空 `source_query_ids`，其中所有 Query 都必须属于当前 `source_batch_id`；如果候选由具体 QueryBlock 触发，`source_qb_ids` 必须非空并属于这些 Query。
22. 每个 MV Candidate 必须给出非空 `target_queries`，其中所有 Query 都必须属于当前 `source_batch_id`；如果计划做 QueryBlock-local rewrite，`target_qb_ids` 必须非空并属于这些 Query。
23. `source_qb_ids` / `target_qb_ids` 不能包含带 `unsupported_reasons` 的 QueryBlock；如果某个 Query 只有 unsupported QueryBlock，BatchMVAgent 必须对该 Query 的 MV Candidate 生成 skip，并写明原因。
24. 每个 MV Candidate 必须给出 `decision`，取值为 `materialize` 或 `skip`。
25. `decision = materialize` 的 MV Candidate 必须包含可执行的 `build_sql`、`mv_id`、`target_table_name` 和非空 `column_mappings`；物化成功后才允许将 `mv_id` 写入 `materialized_mvs.json`。
26. `decision = skip` 可以不包含 `build_sql` 和 `mv_id`，但仍需保留 `candidate_id`、`source_batch_id`、`source_query_ids`、`family_id`、`target_queries`、`decision` 和 `reason`，用于后续反馈分析。
27. 当前 batch 内优先按 `family_groups` 生成 MV Candidate；如果一个 SQL 出现在多个 family group 中，只能分别针对对应 family 的 QueryBlock 生成候选，不代表 SQL 被拆分执行。
28. 每个 MV Candidate 只能绑定一个 `family_id`，不允许把不同 family 的 QueryBlock 直接合并为一个 Candidate。
29. 如果 BatchMVAgent 发现不同 family 可能共享 MV，只能在 `reason` 或 run log 中记录 family 质量问题，不能跨 family 生成 MV；该问题应由 FamilyAgent evaluate 或 SelfIterationAgent 反馈规则处理。
30. 默认采用 ETL shared upstream superset MV 策略：MV 是当前 batch 多个 Query 的可复用上游中间结果，目标 Query 通过 residual filter / projection / aggregate / roll-up 从 MV 中重写得到。
31. MV 不要求是某条 workflow SQL 的子集；更准确的关系是 `workflow_sql_i = rewrite(MV, residual_filter_i, projection_i, rollup_i)`。

对于 `family_type = "dimension_only"` 的 QueryFamily，BatchMVAgent 允许生成并物化维表过滤 / 映射类 MV Candidate。dimension-only MV 的主要用途是 QueryBlock-local rewrite，以及作为后续 fact-based MV 的 `depends_on_mv_ids` 构建依赖；它不能把 dimension-only family 与 fact-based family 合并，也不能被解释为 fact query 的 shared upstream superset MV。

32. `BatchMVAgent` 先确定 shared upstream superset MV 的语义边界，再决定物理形态。
33. 如果所有 target Query 都是聚合查询，measure 可 roll-up，且 group-by 粒度能覆盖所有 target Query 的 roll-up 维度与 residual filter 列，优先生成 `mv_type = "fine_grain_aggregate"`。
34. 如果无法安全聚合，或者下游需要明细字段 / 非可加聚合 / 复杂表达式，则 fallback 生成 `mv_type = "detail_superset"`。
35. 如果 detail superset MV 也无法证明能安全支持 target Query rewrite，则 Candidate 必须 `decision = skip`。
36. 只有所有 `target_queries` 共同拥有的 predicate shape 才能进入共享 MV predicate。
37. 共同 predicate shape 可以做当前 batch 内有限泛化，例如当前 batch 已出现 `date_dim.d_year = 2000` 和 `date_dim.d_year = 2001` 时，可以生成 `date_dim.d_year IN (2000, 2001)`。
38. 非共同 predicate shape 不进入共享 MV predicate，必须按 query 记录到 `residual_filters`，由下游 rewrite 或执行 SQL 继续过滤。
39. MV 必须保留执行 `residual_filters`、projection 和 roll-up 所需的列或 group-by 粒度；如果无法保留，Candidate 必须 `decision = skip`。
40. 不允许为了覆盖未来 batch 直接去掉共同 predicate，或生成覆盖整列范围的宽 MV。
41. 如果共享 MV 使用了有限泛化 predicate，必须在 Candidate 中记录 `generalized_predicates`，说明每个 predicate shape 覆盖的当前 batch 常量范围。
42. 额外拆分更窄的专用 MV 可以作为后续 cost-based 扩展；第一版的默认主规则是 shared upstream superset MV，而不是为每个 predicate 差异都生成多个 Candidate。
43. `BatchMVAgent` 必须采用 generate + evaluate 两阶段；第一阶段生成 `candidate_mv_output`，第二阶段输入相同上下文和 `candidate_mv_output`，输出修正后的完整结果。
44. `output_columns` 必须是 MV 表的真实物理列名，例如 `d_year`、`i_brand_id`，不能写 `date_dim.d_year` 这类源表限定列名。
45. `column_mappings` 记录源字段到 MV 物理列的映射，最小字段包括 `source_expr`、`source_table`、`source_column`、`mv_column` 和 `role`。
46. 对直接来自物理表字段的 dimension / filter / projection 列，`mv_column` 默认使用 `source_column`；如果同一个 MV 中出现同名物理列冲突，才使用 `{source_table}_{source_column}` 消歧。
47. measure 列使用 `{agg_func}_{source_column}`，例如 `sum_ss_ext_sales_price`；`build_sql` 中普通物理列默认不写 `AS`，聚合表达式必须显式 `AS measure_mv_column`。
48. `column_mappings.source_table.source_column` 必须能在 `tpcds_simple.json` 中找到；代码层只校验，不自动修复 SQL。
49. `build_sql` 必须使用 `CREATE TABLE ... AS SELECT ...`，不要使用 `CREATE OR REPLACE TABLE`。
50. evaluate 阶段必须检查：`source_batch_id` 是否等于当前 batch，`source_query_ids` / `target_queries` 是否都属于当前 batch，`source_qb_ids` / `target_qb_ids` 是否属于对应 Query 且不跨 family，`family_id` 是否唯一且不跨 family，`mv_type` 是否与可证明 rewrite 关系一致，`mv_predicates` / `generalized_predicates` / `residual_filters` 是否满足 shared upstream superset 规则，`output_columns` / `column_mappings` / `group_by_exprs` / `measure_exprs` 是否保留 residual filter、projection 和 roll-up 所需信息，`depends_on_mv_ids` 是否只引用可见历史 MV 且不形成循环，`build_sql` 是否与 Candidate spec 对齐。
51. evaluate 阶段如果发现 Candidate 只由未来 batch 或 downstream reuse 触发，必须删除或改为 `decision = "skip"`；如果发现 Candidate 跨 family，必须拆回单 family Candidate，或记录 family 质量问题并 skip。
52. 对外落盘的 `batch_{batch_id}_mv_candidates.json` 使用 evaluate 后的最终结果；第一阶段草稿可作为 debug artifact 保留，但不作为 ExecutorAgent 的输入。

输出 JSON：

```json
{
  "batch_id": 2,
  "mv_candidates": [
    {
      "candidate_id": "cand_batch_2_family_fact_ss_dd_item_manager_moy_year_0001",
      "mv_id": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "source_batch_id": 2,
      "source_query_ids": ["q42", "q52"],
      "source_qb_ids": ["q42.outer", "q52.outer"],
      "family_id": "family_fact_ss_dd_item_manager_moy_year",
      "family_type": "fact_based",
      "mv_type": "fine_grain_aggregate",
      "target_table_name": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "target_queries": ["q42", "q52"],
      "target_qb_ids": ["q42.outer", "q52.outer"],
      "depends_on_mv_ids": ["mv_batch_1_example"],
      "mv_predicates": [
        "item.i_manager_id = 1",
        "date_dim.d_moy = 11",
        "date_dim.d_year IN (2000)"
      ],
      "generalized_predicates": [
        {
          "predicate_shape": "date_dim.d_year = <CONST>",
          "covered_values": [2000],
          "source_query_ids": ["q42", "q52"]
        },
        {
          "predicate_shape": "date_dim.d_moy = <CONST>",
          "covered_values": [11],
          "source_query_ids": ["q42", "q52"]
        }
      ],
      "residual_filters": [
        {
          "query_id": "q42",
          "predicates": []
        },
        {
          "query_id": "q52",
          "predicates": []
        }
      ],
      "output_columns": [
        "d_year",
        "i_brand_id",
        "i_brand",
        "i_category_id",
        "i_category",
        "sum_ss_ext_sales_price"
      ],
      "column_mappings": [
        {
          "source_expr": "date_dim.d_year",
          "source_table": "date_dim",
          "source_column": "d_year",
          "mv_column": "d_year",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_brand_id",
          "source_table": "item",
          "source_column": "i_brand_id",
          "mv_column": "i_brand_id",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_brand",
          "source_table": "item",
          "source_column": "i_brand",
          "mv_column": "i_brand",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_category_id",
          "source_table": "item",
          "source_column": "i_category_id",
          "mv_column": "i_category_id",
          "role": "dimension"
        },
        {
          "source_expr": "item.i_category",
          "source_table": "item",
          "source_column": "i_category",
          "mv_column": "i_category",
          "role": "dimension"
        },
        {
          "source_expr": "SUM(store_sales.ss_ext_sales_price)",
          "source_table": "store_sales",
          "source_column": "ss_ext_sales_price",
          "mv_column": "sum_ss_ext_sales_price",
          "role": "measure"
        }
      ],
      "group_by_exprs": [
        "date_dim.d_year",
        "item.i_brand_id",
        "item.i_brand",
        "item.i_category_id",
        "item.i_category"
      ],
      "measure_exprs": ["SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price"],
      "build_sql": "CREATE TABLE mv_ss_dd_item_mgr1_y2000_m11_fg AS SELECT date_dim.d_year, item.i_brand_id, item.i_brand, item.i_category_id, item.i_category, SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price FROM ...",
      "decision": "materialize",
      "reason": "same family and same filter, different group by branches"
    }
  ]
}
```

### 7.5 `rules/rewrite_agent.md`

职责：

```text
给定 original SQL、QueryBlock 和全局 materialized_mvs，输出每条 SQL 的 rewritten SQL 文件、rewrite meta 和 run log。
```

当前 demo 中，rewritten SQL 与 rewrite meta 由 RewriteAgent 的 `LLM + rules` 生成。代码层不主动模板化改写 SQL；只负责 SQLGlot 解析、可用 MV 校验、输出契约校验和 fallback 触发。

规则重点：

1. historical rewrite 和 final rewrite 的输入 SQL 都应是当前 batch 的 original SQL。
2. historical rewrite 只能使用进入当前 batch 前已经存在的 Materialized View State。
3. RewriteAgent 永远不覆盖 original SQL。
4. 每个 rewrite 阶段、每条输入 SQL 都必须产出 `{query_id}_rewritten.sql`。
5. 每个 rewrite 阶段、每条输入 SQL 都必须产出 `{query_id}_rewrite_meta.json`。
6. 每个 rewrite 阶段都必须向 `run_log.jsonl` 追加事件，记录输入、输出、rewrite 状态、使用 MV 和 fallback 原因。
7. historical rewrite 如果没有可用历史 MV，则输出与 original SQL 等价的 rewritten SQL，`used_mv_ids = []`，`fallback_reason = "no_available_historical_mv"`。
8. final rewrite 使用当前 batch 物化成功后更新的完整 Materialized View State。
9. 只能使用 `materialized_mvs.json` 中且 `available_from_batch <= current_batch_id` 的 Materialized View。
10. rewritten SQL 必须保持 original SQL 输出语义。
11. 只有当 MV 的 join key、filter 覆盖关系、group by 粒度、aggregate 表达式和 MV 物理列都能覆盖原 SQL 需求时，才允许 rewrite。
12. 如果不确定是否等价，必须 fallback 到与 original SQL 等价的 rewritten SQL，不能为了使用 MV 进行猜测式改写。
13. fallback 时 `status = "fallback"`，`used_mv_ids = []`，`fallback_reason` 必须非空。
14. 成功使用 MV 时 `status = "rewritten"`，`used_mv_ids` 必须非空，`fallback_reason = null`。
15. final rewrite 没有可安全使用 MV 时，也必须 fallback original-equivalent SQL，并按原因填写 `fallback_reason`。
16. 常见 fallback reason 包括：`no_available_historical_mv`、`no_matching_mv`、`mv_columns_not_covering_query`、`mv_column_mappings_missing`、`mv_uses_source_qualified_columns`、`mv_unknown_column`、`output_alias_missing`、`output_name_missing`、`order_by_missing`、`order_by_mismatch`、`limit_missing`、`limit_mismatch`、`filter_not_implied_by_mv`、`group_by_not_compatible`、`aggregate_not_supported`、`unsupported_sql_pattern`、`block_output_contract_uncertain`、`parent_dependency_not_preserved`、`semantic_equivalence_uncertain`。
17. 第一版支持 SUM / COUNT / AVG / MIN / MAX。
18. 对复杂 window、rollup、count distinct、stddev、相关子查询默认 fallback。
19. 对 `partial_success` SQL，RewriteAgent 可以改写可证明安全的 QueryBlock；遇到带 `unsupported_reasons` 的 QueryBlock，必须保持 original-equivalent 或 fallback，不能强行使用 MV。
20. QueryBlock-local rewrite 的 `target_qb_ids` 不能包含带 `unsupported_reasons` 的 QueryBlock。
21. 判断 filter 覆盖关系时，必须读取 `materialized_mvs.json` 中的 `mv_predicates`、`generalized_predicates` 和 `residual_filters`。
22. 如果 query filter 已经被 `mv_predicates` 或 `generalized_predicates` 覆盖，不要求该过滤列出现在 MV 的 `output_columns` 中；只有仍需在 rewritten SQL 中执行的 residual filter，才要求对应列存在于 `output_columns` 或 `column_mappings.mv_column`。
23. 使用 MV rewrite 时，只能引用 `materialized_mvs.json` 中的 `output_columns` 或 `column_mappings.mv_column`。
24. rewritten SQL 不能从 MV 表中引用 `date_dim.d_year`、`` `date_dim.d_year` ``、`item.i_brand_id` 这类源表限定列名；源字段身份只用于规则判断和日志解释。
25. rewritten SQL 必须保留 original SQL 的输出列名。显式或隐式 alias 必须保留，例如 q52 中的 `brand_id`、`brand`、`ext_price`。
26. original SQL 中没有 alias 的表达式也必须保留 Spark 输出名。例如 q42 的 `sum(ss_ext_sales_price)` 没有 alias，rewrite 时应输出 `AS \`sum(ss_ext_sales_price)\``，不能改成 `AS sum_ss_ext_sales_price`。
27. final rewrite 必须保留 original SQL 的 `ORDER BY` 和 `LIMIT`；如果 original SQL 有排序或 limit，rewritten SQL 也必须保留对应排序项数量、排序方向和 limit 值。
28. 如果 final rewrite 没有保留 `ORDER BY` / `LIMIT`，RewriteAgent 最多重试 2 次；仍无法修正时 fallback 到 original-equivalent SQL。
29. 如果无法证明输出列名、排序或 limit 一致，必须 fallback 到 original-equivalent SQL。
30. RewriteAgent 可以输出 `rewrite_mode = "query_block_local_rewrite"`，用于只改写 CTE / subquery body；此时 `target_qb_ids` 必须非空。
31. QueryBlock-local rewrite 不改变外层 Query 的 SELECT、JOIN、WHERE、GROUP BY、ORDER BY、LIMIT 等结构，只替换目标 QueryBlock 的内部 SQL。
32. CTE / subquery body 被 MV 改写后，必须继续提供父 Query 所引用的列名或 alias；如果 MV 物理列名不同，rewritten block 必须显式 `AS original_block_column_name`。
33. 如果不能证明 rewritten block 保持原 QueryBlock 输出契约，或不能证明父 Query 的依赖关系不变，必须 fallback。

输出 JSON：

```json
{
  "rewrites": [
    {
      "query_id": "q42",
      "rewrite_stage": "final",
      "status": "rewritten",
      "used_mv_ids": ["mv_ss_dd_item_mgr1_y2000_m11_fg"],
      "target_qb_ids": ["q42.outer"],
      "original_sql_path": ".../tpcds-spark/q42.sql",
      "rewritten_sql_path": "{run_id}/05_rewritten_sql/batch_3/final_rewrite/q42_rewritten.sql",
      "rewrite_meta_path": "{run_id}/05_rewritten_sql/batch_3/final_rewrite/q42_rewrite_meta.json",
      "rewrite_mode": "mv_filter_projection_rollup",
      "rewritten_sql": "SELECT d_year, i_category_id, i_category, SUM(sum_ss_ext_sales_price) AS `sum(ss_ext_sales_price)` FROM mv_ss_dd_item_mgr1_y2000_m11_fg GROUP BY d_year, i_category_id, i_category ORDER BY `sum(ss_ext_sales_price)` DESC, d_year, i_category_id, i_category LIMIT 100",
      "residual_filters": [],
      "rollup_exprs": ["GROUP BY d_year, i_category_id, i_category"],
      "semantic_check": {
        "status": "pass",
        "reason": "query can be derived from MV by projection and roll-up"
      },
      "fallback_reason": null
    },
    {
      "query_id": "q99",
      "rewrite_stage": "final",
      "status": "fallback",
      "used_mv_ids": [],
      "original_sql_path": ".../tpcds-spark/q99.sql",
      "rewritten_sql_path": "{run_id}/05_rewritten_sql/batch_3/final_rewrite/q99_rewritten.sql",
      "rewrite_meta_path": "{run_id}/05_rewritten_sql/batch_3/final_rewrite/q99_rewrite_meta.json",
      "rewrite_mode": "original_equivalent",
      "rewritten_sql": "SELECT ... FROM original_tables ...",
      "residual_filters": [],
      "rollup_exprs": [],
      "semantic_check": {
        "status": "fallback",
        "reason": "semantic equivalence uncertain"
      },
      "fallback_reason": "semantic_equivalence_uncertain"
    }
  ]
}
```

### 7.6 `rules/self_iteration_agent.md`

职责：

```text
读取 run log、coverage summary、family candidate、MV candidate、rewrite meta 和 execution order 等已有证据，输出下一轮 rules 或配置修改建议。
```

当前 demo 中，`SelfIterationAgent` 由 `LLM + rules` 主导生成 `feedback_rules_{run_id}.json`。它可以归纳失败模式和规则改进方向，但只输出建议，不自动修改 `rules/*.md`、代码或配置。

规则重点：

1. 只输出建议，不直接修改代码。
2. 不允许直接修改 `rules/*.md`。
3. 可以输出 `suggested_rule_text`，作为人工 review 后可复制进 rules 的建议片段。
4. 建议必须结构化。
5. `suggested_rule_text` 使用中文撰写；SQL、字段名、JSON key、Agent 名称保持原样英文。
6. 输出必须包含 `run_id`。
7. 反馈必须按 `target_agent` 分组。
8. 每条建议必须包含 `evidence_refs`，且至少引用一个已有 artifact 或 run log 事件。
9. 如果证据来自 `run_log.jsonl`，必须优先引用对应的 `event_id`。
10. `evidence_refs` 只做证据追踪，不引入新的主数据结构。
11. 反馈只影响人工 review 后的下一轮 run。
12. 可以基于 BatchMVAgent / RewriteAgent / ExecutorAgent 日志反馈 MV 列映射问题，例如缺少 `column_mappings`、`output_columns` 使用源表限定列名、rewrite 丢失输出列名、execution order 依赖与 rewrite meta 不一致。
13. 可以基于 CTE / subquery 相关日志反馈 QueryBlock-local rewrite 规则问题，例如 `source_qb_ids` / `target_qb_ids` 缺失、CTE 输出契约不清、父 Query 依赖无法证明、可进入 family 的 CTE 被错误 skip。
14. 可以引用 `family_candidates.json` 反馈 FamilyAgent 规则问题，例如高 Jaccard / Containment 候选被错误拆分、core fact table 不同却被错误合并、predicate column set 完全不同却进入同一 family。

输出 JSON：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {
    "BatchMVAgent": {
      "suggestions": [
        {
          "target_rule": "min_reuse_count",
          "suggestion": "increase",
          "suggested_rule_text": "当 MV build cost 连续高于预计节省时间时，提高最小复用次数阈值。",
          "reason": "MV build cost exceeded saved query time",
          "evidence_refs": [
            {
              "artifact": "{run_id}/06_execution_logs/run_log.jsonl",
              "event_id": "20260526_153000:BatchMVAgent:batch_2:0007",
              "batch_id": 2,
              "candidate_id": "cand_batch_2_family_fact_ss_dd_item_manager_moy_year_0001",
              "mv_id": "mv_ss_dd_item_mgr1_y2000_m11_fg",
              "query_ids": ["q42", "q52"],
              "event": "mv_benefit_lower_than_expected"
            }
          ]
        }
      ]
    }
  }
}
```

## 8. 代码职责边界

虽然多数 Agent 是 `LLM + rules`，代码仍负责基础设施。

### 8.1 代码必须负责

```text
Agent 编排
QueryAnalysisRunner 全量 intake 编排
FamilyCandidateBuilder blocking / scoring
BatchWorkflowRunner batch 闭环编排
CoverageSummaryBuilder 覆盖诊断汇总
ArtifactStore 命名 artifact 路径契约
run log 记录
文件读取 / 写入
rules 加载
LLM 调用
JSON schema 校验
artifact 落盘
SQL 文件保存
SQLGlot AST 解析与 SQL 安全校验
materialized_mvs 状态维护
Executor dry-run 顺序规划
错误日志记录
```

### 8.2 LLM 负责

```text
QueryBlock 抽取判断
Family 聚合判断
MV 生成方案
Rewrite 方案
反馈建议
解释 reason
```

### 8.3 必要安全检查

Agent-only 原型仍保留最低限度检查：

1. LLM 输出必须是合法 JSON。
2. 必填字段必须存在。
3. `query_id`、`qb_id` 必须能和输入对齐。
4. rewritten SQL 和 build SQL 至少能保存为文件。
5. Executor 执行失败时记录 fallback，不中断整个流程。
6. LLM 生成的 MV 名称必须稳定、可复现，不能包含随机后缀。
7. `materialized_mvs.json` 只能包含成功物化且可用于 rewrite 的 Materialized View。
8. `depends_on_mv_ids` 只能记录直接依赖；代码层需要校验依赖存在、不能依赖自身、不能形成循环。
9. 单个 MV Candidate 物化失败不阻断当前 batch；失败 Candidate 不写入 `materialized_mvs.json`，Final Rewrite 也不能使用它。
10. `run_log.jsonl` 的每条记录必须包含非空且同一 `run_id` 内唯一的 `event_id`。
11. 每个 MV Candidate 必须包含非空且同一 `run_id` 内唯一的 `candidate_id`。
12. 涉及单个 MV Candidate 的 run log 记录必须包含对应的 `candidate_id`。
13. `materialized_mvs.json` 中的 `mv_id` 只能来自成功物化的 Candidate。
14. MV Candidate 的 `source_query_ids` 必须非空，且都属于 `source_batch_id`。
15. MV Candidate 的 `target_queries` 必须非空，且都属于 `source_batch_id`。
16. 如果 MV Candidate 包含 `source_qb_ids` / `target_qb_ids`，代码层必须校验这些 `qb_id` 属于对应 `source_query_ids` / `target_queries`，且不跨越 Candidate 的 `family_id`。
17. MV Candidate 的 `source_qb_ids` / `target_qb_ids` 不能包含带 `unsupported_reasons` 的 QueryBlock。
18. MV Candidate 不设置 downstream target 字段；下游复用价值只能写入 `reason`，用于解释 `decision`。
19. RewriteAgent 输出 `status = "rewritten"` 时，`used_mv_ids` 必须非空且都存在于 `materialized_mvs.json`。
20. RewriteAgent 输出 `status = "fallback"` 时，`used_mv_ids` 必须为空，`fallback_reason` 必须非空，`rewritten_sql` 必须与 original SQL 等价。
21. `rewrite_mode = "query_block_local_rewrite"` 时，rewrite meta 必须包含非空 `target_qb_ids`，且 rewritten SQL 必须保持目标 CTE / subquery 的输出契约。
22. QueryBlock-local rewrite 的 `target_qb_ids` 不能包含带 `unsupported_reasons` 的 QueryBlock。
23. SelfIterationAgent 的每条 `suggestion` 必须包含非空 `evidence_refs`。
24. 如果 `evidence_refs` 引用 `run_log.jsonl`，必须包含对应的 `event_id`。
25. 如果 `evidence_refs` 引用 MV Candidate，必须包含对应的 `candidate_id`。
26. 所有落盘路径必须位于 `llm_demo/artifacts/{run_id}/`。
27. `decision = materialize` 的 MV Candidate 必须包含非空 `column_mappings`。
28. `output_columns` 不能包含 `.`；它只表示 MV 表真实物理列名。
29. `column_mappings.source_table.source_column` 必须存在于 `tpcds_simple.json`。
30. `build_sql` 中普通物理列默认不写 `AS`；聚合表达式必须显式包含 `AS measure_mv_column`。
31. `build_sql` 必须使用 `CREATE TABLE ... AS SELECT ...`，不能使用 `CREATE OR REPLACE TABLE`。
32. RewriteAgent 使用 MV 时不能引用源表限定列名；发现 `` `date_dim.d_year` `` 或 `date_dim.d_year` 这类引用时必须 fallback。
33. RewriteAgent 必须保持 original SQL 中显式或隐式 alias，以及无 alias 表达式的 Spark 输出名；如果无法证明一致，必须 fallback。
34. `ExecutorAgent.run_queries(...)` 必须从 rewrite meta 读取 `used_mv_ids` 并写入 execution order；如果 rewrite meta 缺少该字段，代码层直接失败。
35. 代码层 SQL 操作必须通过 SQLGlot AST 完成；不得用正则表达式解析 SELECT 子句、判断列引用、规范化 CTAS 或改写 SQL。
36. measure `mv_column` 必须符合 `{agg_func}_{source_column}`；例如 `sum_ss_ext_sales_price`，不能写成丢失源字段前缀的 `sum_ext_sales_price`。
37. final rewrite 必须保留 original SQL 的 `ORDER BY` 和 `LIMIT`；代码层最多触发 2 次 LLM 修正，仍不满足则 fallback。

## 9. MVP 流程

### 9.1 MVP-A：q42 / q52 闭环测试

输入：

```text
llm_demo/workflow/tpcds-spark/q42.sql
llm_demo/workflow/tpcds-spark/q52.sql
```

目标：

1. 验证 `SQLLoaderAgent` 能读取 SQL。
2. 验证 `FeatureAgent` 能提取 `q42.outer` / `q52.outer`。
3. 验证 `FamilyAgent` 能识别 `store_sales-date_dim-item` family。
4. 验证 `BatchClusterAgent` 能按 `batch_classification.csv` 把 q42/q52 放入对应 global batch，并在该 batch 的 `family_groups` 中归入同一个 family。
5. 验证 `BatchMVAgent` 能基于同 family 生成候选 MV。
6. 验证 `RewriteAgent` 和 `ExecutorAgent` 能 dry-run 落盘 rewritten SQL 和日志。
7. 验证 MV Candidate 使用 `column_mappings` 和稳定 MV 物理列名，final rewrite 不再引用源表限定列名。
8. 验证 execution order 中 query step 的 `depends_on_mv_ids` 与 rewrite meta 的 `used_mv_ids` 一致。

注意：q42/q52 主要验证同 family 和 batch 内 MV 链路，不足以验证跨 batch 复用。

### 9.2 MVP-B：跨 batch rewrite 测试

为了验证“Batch-1 物化 MV，Batch-2/3 使用历史 MV rewrite”，需要额外加入一种测试方式：

1. 选择一个低复杂度 SQL，放入更早 batch。
2. 或者在 dry-run 中预置一个 historical MV artifact。

第一版可以优先使用第二种方式：

```text
artifacts/{run_id}/04_batch_mvs/materialized_mvs.json
```

其中写入一个已成功物化、`available_from_batch = 1` 的 mock MV，再运行 q42/q52 所在 batch，测试 `RewriteAgent` 是否会使用该 MV。

### 9.3 MVP-C：完整 `tpcds-spark/` coverage run

第一轮全量 SQL 实验直接读取项目根目录完整 `tpcds-spark/`，不先筛选代表性子集。

目标：

1. 验证 `SQLLoaderAgent` 能为完整 corpus 生成 manifest。
2. 验证 `QueryAnalysisRunner` 能按 one query per call 逐条调用 FeatureAgent，并在单条 SQL 失败时继续处理。
3. 验证失败 Query 在全量首轮结束后统一 retry 1 次。
4. 验证 `feature_extract_status.json`、`run_log.jsonl` 和 `coverage_summary.json` 能解释每个 Query 的状态。
5. 验证后续 Family、Batch、MV、Rewrite 和 dry-run Execution 阶段能消费成功或 partial_success 的 QueryBlock，并对 unsupported / failed Query 保守 skip 或 fallback。
6. 验证 `SelfIterationAgent` 能基于全量失败分布和 fallback 原因输出规则反馈。
7. 验证同一 `run_id` 内的 Feature-only resume：中断后只跳过已成功或 partial_success Query，下游 Family / Batch / MV / Rewrite / Execution 仍重新生成。

该测试允许大量 SQL 进入 fallback 或 unsupported。第一轮关注的是覆盖率曲线和失败归因，而不是端到端加速收益。

### 9.4 dry-run 模式

当前阶段固定使用模拟执行，不连接 Spark，也不设计 `ExecutionAdapter`：

```yaml
execution:
  mode: dry_run
```

`dry_run` 下：

1. Executor 不连接 Spark。
2. `ExecutorAgent.materialize_mvs(...)` 将 `decision = materialize` 的 MV Candidate 作为成功可用的 Materialized View 追加到 `materialized_mvs.json`。
3. rewritten SQL 落盘。
4. 被 skip 或物化失败的 MV Candidate 保留在 `batch_{batch_id}_mv_candidates.json` 和 `run_log.jsonl`。
5. 依赖历史 MV 不可用的 MV Candidate 视为物化失败，不写入 `materialized_mvs.json`，不阻断后续 query dry-run。
6. `ExecutorAgent.run_queries(...)` 按 `ComplexityBatch.query_ids` 顺序生成 `batch_{batch_id}_execution_order.json`。
7. execution order 中的 `run_query.depends_on_mv_ids` 必须与对应 `{query_id}_rewrite_meta.json` 的 `used_mv_ids` 一致。
8. 生成 run log。

真实 Spark 执行暂不进入本方案；后续需要真实执行时，再单独评估是否引入执行后端隔离层。

## 10. Notebook 编排建议

`llm_demo/notebooks/etl_agent_flow.ipynb` 第一版建议拆成以下 cell：

```text
1. 加载 .env、configs、初始化 LLMClient
2. 初始化 ArtifactStore 和 artifact 根目录
3. SQLLoaderAgent 读取 q42/q52
4. FeatureAgent 提取 QueryBlock
5. FamilyAgent 生成 QueryFamily
6. BatchClusterAgent 读取 batch_classification.csv 生成 ComplexityBatch
7. 查看 artifacts，人工确认 JSON 是否合理
8. for batch in batches 执行 batch 编排
9. 查看 materialized_mvs、rewritten SQL、run_log
10. CoverageSummaryBuilder 生成 coverage_summary.json
11. SelfIterationAgent 读取 run_log、coverage_summary、family candidates、MV candidates、rewrite meta 和 execution order，生成 feedback_rules_{run_id}.json
```

Notebook 的价值是方便你在每一步人工检查 LLM 输出，及时调整 rules。等 rules 稳定后，再把相同调用顺序迁移成脚本。

## 11. 后续替换策略

Agent-only 原型跑通后，可以逐步替换。

| 当前模块 | 后续替换方向 |
|---|---|
| `FeatureAgent` | 后续可替换为 SQLGlot 确定性 AST 提取；当前 demo 不做单条 SQL 预拆分或 chunk prompt |
| `BatchClusterAgent` | 当前已采用 CSV 驱动代码路径；后续可把外部分类 CSV 的生成过程沉淀为可复现规则或人工审核流程 |
| `RewriteAgent` | 后续可替换为模板化 / AST rewrite；当前 demo 保持 LLM + rules 主导 |
| `BatchMVAgent` 部分能力 | 后续可引入成本模型 + 规则生成；当前 demo 保持 LLM + rules 主导 |
| `FamilyAgent` 部分能力 | 后续可替换为 canonical join graph 分组；当前 demo 保持代码预筛 + LLM 判定 |
| `SelfIterationAgent` | 当前 demo 保持 LLM + rules 主导；后续可补充代码指标聚合，但仍不自动写回 rules |

保留 LLM 的位置：

```text
SelfIterationAgent
报告生成
失败归因
复杂 SQL fallback
规则解释
```

替换原则：

1. 不改变 artifact 契约。
2. 不改变 Agent 调用顺序。
3. 不改变 Agent 的显式输入输出路径约定。
4. 算法 Agent 可以继续使用同名 artifact 输出，便于和 LLM Agent A/B 对比。

## 12. 最小交付清单

第一轮只需要在 `llm_demo/` 中实现：

```text
notebooks/etl_agent_flow.ipynb
rules/_prompt_template.md
rules/*.md
src/core/agent_base.py
src/core/llm_client.py
src/core/artifact_store.py
src/core/query_analysis_runner.py
src/core/family_candidate_builder.py
src/core/batch_workflow_runner.py
src/core/coverage_summary_builder.py
src/core/physical_schema.py
src/core/sql_utils.py
src/core/schemas.py
src/agents/sql_loader_agent.py
src/agents/feature_agent.py
src/agents/family_agent.py
src/agents/batch_cluster_agent.py
src/agents/batch_mv_agent.py
src/agents/rewrite_agent.py
src/agents/executor_agent.py
src/agents/self_iteration_agent.py
```

项目根目录需要：

```text
.env
tpcds_simple.json
tpcds-spark/
```

跑通后应能生成：

```text
artifacts/{run_id}/01_query_blocks/query_blocks.json
artifacts/{run_id}/01_query_blocks/feature_extract_status.json
artifacts/{run_id}/02_families/query_families.json
artifacts/{run_id}/03_batches/complexity_batches.json
artifacts/{run_id}/04_batch_mvs/*.sql
artifacts/{run_id}/04_batch_mvs/materialized_mvs.json
artifacts/{run_id}/05_rewritten_sql/*.sql
artifacts/{run_id}/06_execution_logs/batch_{batch_id}_execution_order.json
artifacts/{run_id}/06_execution_logs/run_log.jsonl
artifacts/{run_id}/07_feedback/feedback_rules_{run_id}.json
```

这份方案的重点是先完成 Agent 化流程闭环，而不是一开始追求每个模块的算法正确性。后续可以在不改变 artifact 契约的前提下，把 LLM+rules Agent 逐个替换为确定性算法 Agent。
