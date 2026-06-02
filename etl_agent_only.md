# ETL MV Agent-Only 原型实现方案

> 版本：v1.51
> 日期：2026-05-30
> 目标：在 `llm_demo/` 中先用轻量 Agent 串通 ETL MV 编排流程。除 `SQLLoaderAgent` 和 `ExecutorAgent` 外，其余 Agent 均采用 `LLM + rules` 实现。第一版优先验证流程闭环，后续再逐步替换为确定性算法实现。

## 0. 定位

本方案参考 `dataintel_client/agent` 的实现风格，但不要求完全贴合。第一版重点不是做一个完整 Agent 框架，而是用最小的自研框架验证：

```text
SQLLoaderAgent(code)
  -> FeatureAgent(llm+rules)
  -> FamilyAgent(llm+rules)
  -> BatchClusterAgent(llm+rules)
  -> for each batch:
       RewriteAgent(llm+rules)
       BatchMVAgent(llm+rules)
       ExecutorAgent(code)
       RewriteAgent(llm+rules)
       ExecutorAgent(code)
  -> SelfIterationAgent(llm+rules)
```

核心约束：

1. `SQLLoaderAgent` 使用代码实现，负责 SQL 文件读取校验和 SQL manifest artifact 落盘，不复制原始 SQL 文件。
2. `ExecutorAgent` 使用代码实现，负责 dry-run / Spark 执行、MV 状态维护和日志落盘。
3. 其他 Agent 均使用 `LLM + rules` 实现。
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
28. `BatchClusterAgent` 先在 `QueryFamily` 内分析 QueryBlock 复杂度，再把不同 family 的同复杂度 SQL 汇总到全局 `ComplexityBatch`。
29. batch 的执行单位始终是完整 SQL/query_id；QueryBlock 只作为 family 归属、复杂度判断、MV 生成和 rewrite 辅助结构。
30. 一个 SQL 的全局 batch 由其所有 QueryBlock 中最高复杂度决定。
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
53. `BatchMVAgent` 的 `build_sql` 统一使用 `CREATE TABLE ... AS SELECT ...`；不生成 `CREATE OR REPLACE TABLE`，避免原型 dry-run / 后续 Spark 执行阶段引入覆盖已有表的语义。
54. 代码层凡涉及 SQL 解析、SELECT 输出列名提取、CTAS 规范化、源表限定列检测等 SQL 操作，统一使用 SQLGlot；不使用正则表达式解析、判断或改写 SQL。
55. measure 类型的 MV 物理列名统一使用 `{agg_func}_{source_column}`，例如 `SUM(store_sales.ss_ext_sales_price)` 输出 `sum_ss_ext_sales_price`，保留源字段 `ss_ext_sales_price` 的业务前缀。
56. final rewrite 必须保留 original SQL 的 `ORDER BY` 和 `LIMIT`；如果 LLM 输出遗漏，RewriteAgent 最多重试 2 次，仍无法修正则 fallback。

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
| `FamilyAgent` | LLM + rules | 基于 QueryBlock 聚合 QueryFamily |
| `BatchClusterAgent` | LLM + rules | 基于 QueryBlock 和 QueryFamily 生成 SQL 级 ComplexityBatch，并在 batch 内保留 family_groups |
| `BatchMVAgent` | LLM + rules | 在 batch 内生成、选择 MV candidate，并输出 CTAS SQL |
| `RewriteAgent` | LLM + rules | 基于全局已物化 MV 生成 rewritten SQL |
| `ExecutorAgent` | 代码 | 通过 `materialize_mvs(...)` 物化 MV，通过 `run_queries(...)` 执行或模拟执行 SQL，并维护 `materialized_mvs` |
| `SelfIterationAgent` | LLM + rules | 基于日志生成规则优化建议 |

第一版 `FamilyAgent` 和 `BatchClusterAgent` 串行调用：先由 `FamilyAgent` 生成 QueryFamily，再由 `BatchClusterAgent` 读取 QueryBlock、`query_to_qbs` 和 QueryFamily 生成全局 ComplexityBatch。这样让 family 参与复杂度分层判断，但不引入额外的 FamilyBatch 数据结构。

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
3. Agent 之间通过 ArtifactStore 和显式 artifact 路径传递结果，避免把大量 SQL 和 JSON 都塞进内存对象。
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

因此，Agent 之间不需要再传递额外中间对象。Notebook 编排层只负责保存这些 artifact 路径，并把路径作为参数传给下一个 Agent。

第一版建议的最小 artifact 契约：

```text
{run_id}/00_raw_sql/sql_manifest.json
{run_id}/01_query_blocks/query_blocks.json
{run_id}/01_query_blocks/query_to_qbs.json
{run_id}/01_query_blocks/qb_to_query.json
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
{run_id}/06_execution_logs/run_log.jsonl
{run_id}/07_feedback/feedback_rules_{run_id}.json
```

设计原则：

1. 业务结果以 artifact 为唯一事实来源。
2. Notebook 中可以用普通变量保存路径，但不形成新的系统级数据结构。
3. `materialized_mvs.json` 是 Materialized View State，只保存已经成功物化且可被 RewriteAgent 使用的 Materialized View。
4. `run_log.jsonl` 记录每个 Agent 的输入路径、输出路径、耗时、错误，不参与业务决策；每条记录必须有稳定的 `event_id`。
5. MV Candidate 中被跳过、物化失败或仅用于诊断的信息保留在 `batch_{batch_id}_mv_candidates.json` 和 `run_log.jsonl`，不写入 `materialized_mvs.json`。
6. `run_id` 由 notebook 在每次实验开始时生成，建议使用 `YYYYMMDD_HHMMSS` 或手动命名的实验 ID。
7. 后续替换算法实现时，只要保持 artifact 契约不变，就不影响上游和下游 Agent。

`materialized_mvs.json` 的最小结构：

```json
{
  "materialized_mvs": [
    {
      "mv_id": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "table_name": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "source_candidate_id": "cand_batch_2_family_ss_dd_item_0001",
      "source_batch_id": 2,
      "available_from_batch": 2,
      "family_id": "family_ss_dd_item",
      "target_queries": ["q42", "q52"],
      "depends_on_mv_ids": ["mv_batch_1_example"],
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

该文件不保存 `status = failed`、`decision = skip` 或未执行的 MV Candidate。成功物化的 MV 可以保留 `source_candidate_id`，用于回溯其来源候选。RewriteAgent 读取这个文件判断可用 Materialized View，并使用其中的 `output_columns` / `column_mappings.mv_column` 作为可引用的 MV 物理列集合。

`run_log.jsonl` 的单行最小结构：

```json
{
  "run_id": "20260524_153000",
  "event_id": "20260524_153000:ExecutorAgent:batch_2:0001",
  "agent_name": "ExecutorAgent",
  "batch_id": 2,
  "candidate_id": "cand_batch_2_family_ss_dd_item_0001",
  "input_artifact_paths": ["..."],
  "output_artifact_paths": ["..."],
  "elapsed_ms": 1200,
  "event": "mv_materialize_success",
  "error": null
}
```

`event_id` 建议由代码生成，保持同一 `run_id` 内唯一、稳定、可读，例如 `{run_id}:{agent_name}:batch_{batch_id}:{seq}`。如果事件不属于某个 batch，可用 `global` 替代 `batch_{batch_id}`。

普通 Agent 级别事件可以把 `candidate_id` 置为 `null`；涉及单个 MV Candidate 的事件，例如 `mv_materialize_success`、`mv_materialize_failed`、`mv_candidate_skipped`，必须填写对应 `candidate_id`。如果 MV Candidate 物化失败，`event` 使用 `mv_materialize_failed`，并在 `error` 中记录失败原因，例如依赖 MV 表不存在、Spark 执行失败或 SQL 不可执行。

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
| `BatchClusterAgent` | QueryBlock、`query_to_qbs`、QueryFamily | ComplexityBatch artifact |
| `RewriteAgent` | original batch SQL、QueryBlock、`materialized_mvs.json` | historical rewrite 或 final rewrite SQL |
| `BatchMVAgent` | historical rewrite SQL、原始 QueryBlock、原始 QueryFamily、当前 batch 的 `family_groups`、`materialized_mvs.json`、全局 `complexity_batches.json` 和 `query_families.json` 只读上下文 | 当前 batch 的 MV Candidate JSON、MV build SQL |
| `ExecutorAgent.materialize_mvs(...)` | MV Candidate JSON、MV build SQL | 成功物化的 MV 追加到 `materialized_mvs.json`；skip / failed Candidate 写入 run log |
| `ExecutorAgent.run_queries(...)` | final rewrite SQL、rewrite meta、ComplexityBatch | dry-run execution order JSON、run log |
| `SelfIterationAgent` | `run_log.jsonl` | `{run_id}/07_feedback/feedback_rules_{run_id}.json` |

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
2. `family_agent.md` 说明如何按 join skeleton / family_key 聚合 QueryFamily。
3. `batch_cluster_agent.md` 说明如何以 SQL 为单位划分 batch。
4. `batch_mv_agent.md` 说明如何在当前 batch 内生成 MV。
5. `rewrite_agent.md` 说明何时使用 MV、何时 fallback。
6. `self_iteration_agent.md` 说明如何基于日志输出规则建议。

各 Agent 的“示例”小节建议包含，并随设计确认持续更新：

1. `feature_agent.md`：给出 q42/q52 片段到 QueryBlock JSON 的示例。
2. `family_agent.md`：给出 predicate 兼容、`predicate_shapes`、重复 family evaluate / merge 的示例。
3. `batch_cluster_agent.md`：给出 SQL 级复杂度取最高 QB complexity，以及 global batch 内 `family_groups` 的示例。
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
query_block_paths = FeatureAgent(llm+rules).run(sql_manifest_path)
family_path = FamilyAgent(llm+rules).run(query_block_paths)
batch_path = BatchClusterAgent(llm+rules).run(query_block_paths, family_path)
```

ComplexityBatch 的执行顺序固定为从低复杂度到高复杂度：

```text
Batch-1 = join
Batch-2 = join_filter
Batch-3 = join_filter_groupby
Batch-4 = other
```

`BatchClusterAgent` 的判定过程分两层：

1. 在每个 QueryFamily 内，根据成员 QueryBlock 的复杂度为所属 SQL 标注候选 batch。
2. 把不同 family 中同一复杂度层级的 SQL 合并到全局 batch 中。

因此，最终执行顺序仍是全局 `Batch-1 -> Batch-2 -> Batch-3 -> Batch-4`，不是按 family 分别执行。`ComplexityBatch` 内部用 `family_groups` 保存 batch 内的 family 组织关系，供 `BatchMVAgent` 生成 MV Candidate 使用。

输出：

| Artifact | 说明 |
|---|---|
| `artifacts/{run_id}/00_raw_sql/sql_manifest.json` | 原始 SQL 文件 manifest，记录 query_id、原始路径、相对路径和大小，不复制 SQL Text |
| `artifacts/{run_id}/01_query_blocks/query_blocks.json` | LLM 提取的 QueryBlock |
| `artifacts/{run_id}/01_query_blocks/query_to_qbs.json` | SQL 到 QB 索引 |
| `artifacts/{run_id}/01_query_blocks/qb_to_query.json` | QB 到 SQL 索引 |
| `artifacts/{run_id}/02_families/query_families.json` | LLM 生成的 QueryFamily |
| `artifacts/{run_id}/03_batches/complexity_batches.json` | LLM 生成的 SQL 级 batch |

### 6.2 Batch 编排

每个 batch 先使用历史已物化 MV rewrite 当前 batch 的 original SQL，得到 historical rewrite SQL；再基于 historical rewrite SQL 生成本 batch MV。由于 historical rewrite SQL 可能已经引用历史 MV，当前 batch 的新 MV 可以基于这些历史 MV 构建，形成增量式 MV 扩充。完成物化后，final rewrite 必须重新以当前 batch 的 original SQL 为输入，使用更新后的 Materialized View State 生成最终执行 SQL。

```text
for batch in complexity_batches:
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
  5. ExecutorAgent.run_queries(...) 执行或 dry-run final rewrite SQL，并根据 rewrite meta 的 used_mv_ids 写出 execution order 中的 query 依赖
```

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
3. CTE / subquery 可以提取为独立 QueryBlock。
4. 必须输出 `query_id`、`qb_id`、tables、join_edges、predicates、group_by、aggregate、complexity_type。
5. `tables`、`join_edges`、`predicates`、`group_by_exprs`、`aggregate_exprs` 和 `family_key` 中的表名必须使用物理表名，不使用 SQL alias；例如 `date_dim dt` 的 `dt.d_year` 应输出为 `date_dim.d_year`。
6. FeatureAgent 在 LLM 提取后增加 LLM + rules evaluate 环节，输入为原始 SQL 与 `candidate_feature_output`，输出仍为修正后的完整 FeatureOutput。
7. evaluate 阶段检查输出中是否仍有 SQL alias 或未知表名前缀；SQL alias 应修正为物理表名，未知表名前缀写入 `unsupported_reasons`，不做复杂代码兜底。
8. 不确定时标记 `unsupported_reasons`，不要编造字段。

示例 `build_sql`：

```sql
CREATE TABLE mv_ss_dd_item_q42_q52_fg AS
SELECT
  date_dim.d_year,
  item.i_category_id,
  item.i_category,
  item.i_brand,
  item.i_brand_id,
  SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price
FROM date_dim
JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND date_dim.d_moy = 11
  AND date_dim.d_year IN (2000)
GROUP BY
  date_dim.d_year,
  item.i_category_id,
  item.i_category,
  item.i_brand,
  item.i_brand_id
```

输出 JSON：

```json
{
  "query_blocks": [
    {
      "qb_id": "q42.outer",
      "query_id": "q42",
      "scope_type": "outer",
      "tables": ["date_dim", "store_sales", "item"],
      "join_edges": [],
      "predicates": [],
      "group_by_exprs": ["date_dim.d_year", "item.i_category_id"],
      "aggregate_exprs": [],
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

### 7.2 `rules/family_agent.md`

职责：

```text
给定 QueryBlock 列表，按 upstream join-domain MV / shared superset MV 覆盖关系聚合 QueryFamily。
```

规则重点：

1. 聚合单位是 `qb_id`。
2. `QueryFamily` 表示一组可由同一个 upstream join-domain MV 或 shared superset MV 覆盖的 QueryBlock，不是简单 SQL 相似簇。
3. 第一版优先保守合并，不做激进模糊合并；错误合并会影响 MV rewrite 安全，漏合并只影响优化机会。
4. family 中记录 common tables、common join skeleton、common predicates、predicate shapes、group by 分叉和 measure 并集。
5. 如果合并依据不足，保持分离。
6. `FamilyAgent` 采用两次 LLM 调用：第一次生成 `candidate_query_families`，第二次 evaluate 并修正输出。
7. evaluate 阶段输入为 QueryBlock 列表和 `candidate_query_families`，输出仍是修正后的完整 QueryFamily。
8. evaluate 阶段重点检查是否存在重复 family、可合并 family、family_key 与 join skeleton 不一致、成员 QueryBlock 归属错误、common 字段错误等问题。
9. family candidate 第一层使用表集合量化：`Jaccard(A,B)=|T(A)∩T(B)|/|T(A)∪T(B)|` 衡量相似度，`Containment(A,B)=|T(A)∩T(B)|/min(|T(A)|,|T(B)|)` 识别宽表覆盖关系。
10. core fact table 是 QueryBlock join domain 中承载事实记录或主要 measure 来源的中心表；TPC-DS 第一版可优先识别 `store_sales`、`catalog_sales`、`web_sales`、`store_returns`、`catalog_returns`、`web_returns`、`inventory`。
11. candidate 分层规则：`strong candidate` = `Jaccard = 1.0`，或 `Containment = 1.0` 且 core fact table 相同；`medium candidate` = `Jaccard >= 0.6` 且 core fact table 相同，或 `Containment >= 0.8` 且较小 QueryBlock 的表集合主要被较大 join domain 覆盖；低于该阈值默认保持分离。
12. core fact table 相同是 family 合并 hard gate；如果 core fact table 不同，即使 Jaccard / Containment 较高，也默认保持分离。
13. 多 fact table QueryBlock 只允许与具有完全相同 core fact table set、且 join graph 可证明不会引入行数膨胀的 QueryBlock 合并；否则单独成 family。
14. Jaccard 和 Containment 只负责生成 candidate，不直接决定最终合并。
15. family 最终合并采用安全门：core fact table、join graph、predicate shape、measure compatibility 和 roll-up safety 必须可证明兼容。
16. family 合并采用 predicate 兼容标准，而不是要求 predicate 完全相同。
17. 第一版 predicate shape 兼容只允许两类情况：同一过滤列常量不同，例如 `date_dim.d_year = 2000` 与 `date_dim.d_year = 2001`；或者一方比另一方多出过滤条件，且多出的过滤列可在 MV 中保留并作为下游 residual filter。
18. 如果两个 QueryBlock 的过滤列集合完全不同，例如一个只过滤 `date_dim.d_year`，另一个只过滤 `item.i_manager_id`，即使 core fact table、join graph 和 measure 兼容，也必须拆成不同 family。
19. 如果两个 family 的 QueryBlock 具有相同或可证明等价的 join skeleton、同构过滤列结构但常量值不同、兼容 measure，并且后续可共享 MV，应在 evaluate 阶段合并为同一个 family。
20. `common_predicates` 只记录 family 内完全相同的完整谓词，例如所有成员都有 `item.i_manager_id = 1`。
21. `predicate_shapes` 记录同构过滤列但常量值可能不同的谓词形状，例如 `date_dim.d_year = <CONST>`、`date_dim.d_moy = <CONST>`。
22. `predicate_shapes` 中的字段名必须使用物理表名，不使用 SQL alias。
23. 如果过滤列集合差异过大、predicate 语义不清或 family 是否可合并无法证明，保持分离，并在后续 SelfIterationAgent 的反馈中记录规则改进建议，而不是在 BatchMVAgent 中跨 family 生成 MV。

以 q42 / q52 为例，二者的表集合均为 `{date_dim, store_sales, item}`，`Jaccard = 1.0`，`Containment = 1.0`，core fact table 均为 `store_sales`，join graph、predicate shape 和 measure 均兼容，因此可以归入同一 `QueryFamily`。如果另一个 QueryBlock 使用 `web_sales` 作为事实表，即使也连接 `date_dim` 和 `item`，第一版也不与 `store_sales` family 合并。

输出 JSON：

```json
{
  "query_families": [
    {
      "family_id": "family_ss_dd_item",
      "family_key": "store_sales-date_dim-item",
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
给定 QueryBlock、query_to_qbs 和 QueryFamily，以 SQL/query_id 为单位生成全局 ComplexityBatch，并在每个 batch 内保留 family_groups。
```

规则重点：

1. batch 分配单位是 SQL/query_id，不是 qb_id。
2. 一个 SQL 的复杂度取其所有 QueryBlock 的最高复杂度。
3. batch 映射固定为 `join -> Batch-1`、`join_filter -> Batch-2`、`join_filter_groupby -> Batch-3`、`other -> Batch-4`。
4. 当前不处理并发上限、子 batch 拆分。
5. 先在每个 QueryFamily 内进行复杂度分层，再把同一复杂度层级的 SQL 合并到对应的 global batch。
6. 不输出独立的 `FamilyBatch` artifact；family 内部分层结果只体现在 `ComplexityBatch.family_groups` 中。
7. 顶层 `query_ids` 表示该 global batch 要处理的完整 SQL，必须去重。
8. 如果一个 SQL 的多个 QueryBlock 属于不同 family，该 `query_id` 可以出现在多个 `family_groups` 中；这是 MV 生成视角的重复，不代表该 SQL 会被执行多次。
9. 每个 `family_groups[].qb_ids` 只能包含该 family 的 QueryBlock；`family_groups[].query_ids` 必须由这些 QB 对应的 query_id 去重得到。
10. `BatchClusterAgent` 采用两次 LLM 调用：第一次生成 `candidate_complexity_batches`，第二次 evaluate 并修正输出。
11. evaluate 阶段输入为 QueryBlock、`query_to_qbs`、QueryFamily 和 `candidate_complexity_batches`，输出仍是修正后的完整 ComplexityBatch。
12. evaluate 阶段重点检查：每个 SQL 是否按最高 QueryBlock 复杂度进入唯一 global batch、顶层 `query_ids` 是否去重、`family_groups` 是否只作为 batch 内组织信息、是否遗漏或误分 QueryBlock。

输出 JSON：

```json
{
  "complexity_batches": [
    {
      "batch_id": 1,
      "batch_type": "join",
      "query_ids": [],
      "family_groups": []
    },
    {
      "batch_id": 3,
      "batch_type": "join_filter_groupby",
      "query_ids": ["q42", "q52"],
      "family_groups": [
        {
          "family_id": "family_ss_dd_item",
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
19. 输出必须包含 source query、target query、group by、measure、output columns、column mappings、decision、reason 和 depends_on_mv_ids。
20. 每个 MV Candidate 必须给出稳定的 `candidate_id`，同一 `run_id` 内唯一。
21. 每个 MV Candidate 必须给出非空 `source_query_ids`，其中所有 Query 都必须属于当前 `source_batch_id`。
22. 每个 MV Candidate 必须给出非空 `target_queries`，其中所有 Query 都必须属于当前 `source_batch_id`。
23. 每个 MV Candidate 必须给出 `decision`，取值为 `materialize` 或 `skip`。
24. `decision = materialize` 的 MV Candidate 必须包含可执行的 `build_sql`、`mv_id`、`target_table_name` 和非空 `column_mappings`；物化成功后才允许将 `mv_id` 写入 `materialized_mvs.json`。
25. `decision = skip` 可以不包含 `build_sql` 和 `mv_id`，但仍需保留 `candidate_id`、`source_batch_id`、`source_query_ids`、`family_id`、`target_queries`、`decision` 和 `reason`，用于后续反馈分析。
26. 当前 batch 内优先按 `family_groups` 生成 MV Candidate；如果一个 SQL 出现在多个 family group 中，只能分别针对对应 family 的 QueryBlock 生成候选，不代表 SQL 被拆分执行。
27. 每个 MV Candidate 只能绑定一个 `family_id`，不允许把不同 family 的 QueryBlock 直接合并为一个 Candidate。
28. 如果 BatchMVAgent 发现不同 family 可能共享 MV，只能在 `reason` 或 run log 中记录 family 质量问题，不能跨 family 生成 MV；该问题应由 FamilyAgent evaluate 或 SelfIterationAgent 反馈规则处理。
29. 默认采用 ETL shared upstream superset MV 策略：MV 是当前 batch 多个 Query 的可复用上游中间结果，目标 Query 通过 residual filter / projection / aggregate / roll-up 从 MV 中重写得到。
30. MV 不要求是某条 workflow SQL 的子集；更准确的关系是 `workflow_sql_i = rewrite(MV, residual_filter_i, projection_i, rollup_i)`。
31. `BatchMVAgent` 先确定 shared upstream superset MV 的语义边界，再决定物理形态。
32. 如果所有 target Query 都是聚合查询，measure 可 roll-up，且 group-by 粒度能覆盖所有 target Query 的 roll-up 维度与 residual filter 列，优先生成 `mv_type = "fine_grain_aggregate"`。
33. 如果无法安全聚合，或者下游需要明细字段 / 非可加聚合 / 复杂表达式，则 fallback 生成 `mv_type = "detail_superset"`。
34. 如果 detail superset MV 也无法证明能安全支持 target Query rewrite，则 Candidate 必须 `decision = skip`。
35. 只有所有 `target_queries` 共同拥有的 predicate shape 才能进入共享 MV predicate。
36. 共同 predicate shape 可以做当前 batch 内有限泛化，例如当前 batch 已出现 `date_dim.d_year = 2000` 和 `date_dim.d_year = 2001` 时，可以生成 `date_dim.d_year IN (2000, 2001)`。
37. 非共同 predicate shape 不进入共享 MV predicate，必须按 query 记录到 `residual_filters`，由下游 rewrite 或执行 SQL 继续过滤。
38. MV 必须保留执行 `residual_filters`、projection 和 roll-up 所需的列或 group-by 粒度；如果无法保留，Candidate 必须 `decision = skip`。
39. 不允许为了覆盖未来 batch 直接去掉共同 predicate，或生成覆盖整列范围的宽 MV。
40. 如果共享 MV 使用了有限泛化 predicate，必须在 Candidate 中记录 `generalized_predicates`，说明每个 predicate shape 覆盖的当前 batch 常量范围。
41. 额外拆分更窄的专用 MV 可以作为后续 cost-based 扩展；第一版的默认主规则是 shared upstream superset MV，而不是为每个 predicate 差异都生成多个 Candidate。
42. `BatchMVAgent` 必须采用 generate + evaluate 两阶段；第一阶段生成 `candidate_mv_output`，第二阶段输入相同上下文和 `candidate_mv_output`，输出修正后的完整结果。
43. `output_columns` 必须是 MV 表的真实物理列名，例如 `d_year`、`i_brand_id`，不能写 `date_dim.d_year` 这类源表限定列名。
44. `column_mappings` 记录源字段到 MV 物理列的映射，最小字段包括 `source_expr`、`source_table`、`source_column`、`mv_column` 和 `role`。
45. 对直接来自物理表字段的 dimension / filter / projection 列，`mv_column` 默认使用 `source_column`；如果同一个 MV 中出现同名物理列冲突，才使用 `{source_table}_{source_column}` 消歧。
46. measure 列使用 `{agg_func}_{source_column}`，例如 `sum_ss_ext_sales_price`；`build_sql` 中普通物理列默认不写 `AS`，聚合表达式必须显式 `AS measure_mv_column`。
47. `column_mappings.source_table.source_column` 必须能在 `tpcds_simple.json` 中找到；代码层只校验，不自动修复 SQL。
48. `build_sql` 必须使用 `CREATE TABLE ... AS SELECT ...`，不要使用 `CREATE OR REPLACE TABLE`。
49. evaluate 阶段必须检查：`source_batch_id` 是否等于当前 batch，`source_query_ids` / `target_queries` 是否都属于当前 batch，`family_id` 是否唯一且不跨 family，`mv_type` 是否与可证明 rewrite 关系一致，`mv_predicates` / `generalized_predicates` / `residual_filters` 是否满足 shared upstream superset 规则，`output_columns` / `column_mappings` / `group_by_exprs` / `measure_exprs` 是否保留 residual filter、projection 和 roll-up 所需信息，`depends_on_mv_ids` 是否只引用可见历史 MV 且不形成循环，`build_sql` 是否与 Candidate spec 对齐。
50. evaluate 阶段如果发现 Candidate 只由未来 batch 或 downstream reuse 触发，必须删除或改为 `decision = "skip"`；如果发现 Candidate 跨 family，必须拆回单 family Candidate，或记录 family 质量问题并 skip。
51. 对外落盘的 `batch_{batch_id}_mv_candidates.json` 使用 evaluate 后的最终结果；第一阶段草稿可作为 debug artifact 保留，但不作为 ExecutorAgent 的输入。

输出 JSON：

```json
{
  "batch_id": 2,
  "mv_candidates": [
    {
      "candidate_id": "cand_batch_2_family_ss_dd_item_0001",
      "mv_id": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "source_batch_id": 2,
      "source_query_ids": ["q42", "q52"],
      "family_id": "family_ss_dd_item",
      "mv_type": "fine_grain_aggregate",
      "target_table_name": "mv_ss_dd_item_mgr1_y2000_m11_fg",
      "target_queries": ["q42", "q52"],
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
16. 常见 fallback reason 包括：`no_available_historical_mv`、`no_matching_mv`、`mv_columns_not_covering_query`、`mv_column_mappings_missing`、`mv_uses_source_qualified_columns`、`mv_unknown_column`、`output_alias_missing`、`output_name_missing`、`order_by_missing`、`order_by_mismatch`、`limit_missing`、`limit_mismatch`、`filter_not_implied_by_mv`、`group_by_not_compatible`、`aggregate_not_supported`、`unsupported_sql_pattern`、`semantic_equivalence_uncertain`。
17. 第一版支持 SUM / COUNT / AVG / MIN / MAX。
18. 对复杂 window、rollup、count distinct、stddev、相关子查询默认 fallback。
19. 使用 MV rewrite 时，只能引用 `materialized_mvs.json` 中的 `output_columns` 或 `column_mappings.mv_column`。
20. rewritten SQL 不能从 MV 表中引用 `date_dim.d_year`、`` `date_dim.d_year` ``、`item.i_brand_id` 这类源表限定列名；源字段身份只用于规则判断和日志解释。
21. rewritten SQL 必须保留 original SQL 的输出列名。显式或隐式 alias 必须保留，例如 q52 中的 `brand_id`、`brand`、`ext_price`。
22. original SQL 中没有 alias 的表达式也必须保留 Spark 输出名。例如 q42 的 `sum(ss_ext_sales_price)` 没有 alias，rewrite 时应输出 `AS \`sum(ss_ext_sales_price)\``，不能改成 `AS sum_ss_ext_sales_price`。
23. final rewrite 必须保留 original SQL 的 `ORDER BY` 和 `LIMIT`；如果 original SQL 有排序或 limit，rewritten SQL 也必须保留对应排序项数量、排序方向和 limit 值。
24. 如果 final rewrite 没有保留 `ORDER BY` / `LIMIT`，RewriteAgent 最多重试 2 次；仍无法修正时 fallback 到 original-equivalent SQL。
25. 如果无法证明输出列名、排序或 limit 一致，必须 fallback 到 original-equivalent SQL。

输出 JSON：

```json
{
  "rewrites": [
    {
      "query_id": "q42",
      "rewrite_stage": "final",
      "status": "rewritten",
      "used_mv_ids": ["mv_ss_dd_item_mgr1_y2000_m11_fg"],
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
读取 run log，输出下一轮 rules 或配置修改建议。
```

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
              "candidate_id": "cand_batch_2_family_ss_dd_item_0001",
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
run log 记录
文件读取 / 写入
rules 加载
LLM 调用
JSON schema 校验
artifact 落盘
SQL 文件保存
SQLGlot AST 解析与 SQL 安全校验
materialized_mvs 状态维护
Executor dry-run / Spark 执行
错误日志记录
```

### 8.2 LLM 负责

```text
QueryBlock 抽取判断
Family 聚合判断
Batch 复杂度判断
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
16. MV Candidate 不设置 downstream target 字段；下游复用价值只能写入 `reason`，用于解释 `decision`。
17. RewriteAgent 输出 `status = "rewritten"` 时，`used_mv_ids` 必须非空且都存在于 `materialized_mvs.json`。
18. RewriteAgent 输出 `status = "fallback"` 时，`used_mv_ids` 必须为空，`fallback_reason` 必须非空，`rewritten_sql` 必须与 original SQL 等价。
19. SelfIterationAgent 的每条 `suggestion` 必须包含非空 `evidence_refs`。
20. 如果 `evidence_refs` 引用 `run_log.jsonl`，必须包含对应的 `event_id`。
21. 如果 `evidence_refs` 引用 MV Candidate，必须包含对应的 `candidate_id`。
22. 所有落盘路径必须位于 `llm_demo/artifacts/{run_id}/`。
23. `decision = materialize` 的 MV Candidate 必须包含非空 `column_mappings`。
24. `output_columns` 不能包含 `.`；它只表示 MV 表真实物理列名。
25. `column_mappings.source_table.source_column` 必须存在于 `tpcds_simple.json`。
26. `build_sql` 中普通物理列默认不写 `AS`；聚合表达式必须显式包含 `AS measure_mv_column`。
27. `build_sql` 必须使用 `CREATE TABLE ... AS SELECT ...`，不能使用 `CREATE OR REPLACE TABLE`。
28. RewriteAgent 使用 MV 时不能引用源表限定列名；发现 `` `date_dim.d_year` `` 或 `date_dim.d_year` 这类引用时必须 fallback。
29. RewriteAgent 必须保持 original SQL 中显式或隐式 alias，以及无 alias 表达式的 Spark 输出名；如果无法证明一致，必须 fallback。
30. `ExecutorAgent.run_queries(...)` 必须从 rewrite meta 读取 `used_mv_ids` 并写入 execution order；如果 rewrite meta 缺少该字段，代码层直接失败。
31. 代码层 SQL 操作必须通过 SQLGlot AST 完成；不得用正则表达式解析 SELECT 子句、判断列引用、规范化 CTAS 或改写 SQL。
32. measure `mv_column` 必须符合 `{agg_func}_{source_column}`；例如 `sum_ss_ext_sales_price`，不能写成丢失源字段前缀的 `sum_ext_sales_price`。
33. final rewrite 必须保留 original SQL 的 `ORDER BY` 和 `LIMIT`；代码层最多触发 2 次 LLM 修正，仍不满足则 fallback。

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
4. 验证 `BatchClusterAgent` 能把 q42/q52 作为 SQL 放入 `join_filter_groupby` global batch，并在该 batch 的 `family_groups` 中归入同一个 family。
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

### 9.3 dry-run 模式

第一轮默认使用模拟执行：

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

后续切换：

```yaml
execution:
  mode: spark
```

## 10. Notebook 编排建议

`llm_demo/notebooks/etl_agent_flow.ipynb` 第一版建议拆成以下 cell：

```text
1. 加载 .env、configs、初始化 LLMClient
2. 初始化 ArtifactStore 和 artifact 根目录
3. SQLLoaderAgent 读取 q42/q52
4. FeatureAgent 提取 QueryBlock
5. FamilyAgent 生成 QueryFamily
6. BatchClusterAgent 生成 ComplexityBatch
7. 查看 artifacts，人工确认 JSON 是否合理
8. for batch in batches 执行 batch 编排
9. 查看 materialized_mvs、rewritten SQL、run_log
10. SelfIterationAgent 生成 feedback_rules_{run_id}.json
```

Notebook 的价值是方便你在每一步人工检查 LLM 输出，及时调整 rules。等 rules 稳定后，再把相同调用顺序迁移成脚本。

## 11. 后续替换策略

Agent-only 原型跑通后，可以逐步替换。

| 当前 LLM+rules Agent | 后续替换方向 |
|---|---|
| `FeatureAgent` | SQLGlot 确定性 AST 提取 |
| `BatchClusterAgent` | 规则代码分类 |
| `RewriteAgent` | 模板化 / AST rewrite |
| `BatchMVAgent` 部分能力 | 成本模型 + 规则生成 |
| `FamilyAgent` 部分能力 | canonical join graph 分组 |

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
```

跑通后应能生成：

```text
artifacts/{run_id}/01_query_blocks/query_blocks.json
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
