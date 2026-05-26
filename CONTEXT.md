# ETL MV Agent Prototype

This context defines the domain language for a research prototype that uses agent-guided materialized view planning to accelerate ETL-style analytical query workflows.

## Language

### Query Analysis

**Query**:
SQL query (SQL 查询): an executable TPC-DS SQL unit identified by a stable `query_id`. A **Query** is the unit that enters a **ComplexityBatch** and is ultimately executed or dry-run as a whole.
_Avoid_: SQL file, statement, task, QueryBlock

**SQL Text**:
The textual representation of a **Query** or rewritten **Query**. SQL text may be raw input, rewritten output, or materialized view build SQL.
_Avoid_: Query when referring only to the string contents

**QueryBlock**:
Query block (查询块): the smallest analysis unit extracted from a **Query**. A single **Query** has one or more **QueryBlocks**, such as an outer block, CTE block, or subquery block; a **QueryBlock** is never the execution unit.
_Avoid_: Query, SQL, execution unit, executing a QueryBlock

**QueryFamily**:
A shared query structure family (共享查询结构族): a group of **QueryBlocks** that share the same family basis, usually a compatible join skeleton or `family_key`. A **QueryFamily** is used to reason about shared materialization opportunities across related query blocks.
_Avoid_: Query group, query cluster, batch, generic clustering

**Family Key**:
Structure family key (结构族键): the canonical identifier used to decide whether **QueryBlocks** belong to the same **QueryFamily** in the first prototype. It should be conservative and stable rather than aggressively merging partially similar query blocks.
_Avoid_: fuzzy signature, similarity score, loose similarity key

**Complexity Type**:
Structural complexity type (结构复杂度类型): the complexity class assigned to a **QueryBlock** from the ordered set `join`, `join_filter`, `join_filter_groupby`, and `other`. A **Query** takes the highest complexity type among its **QueryBlocks**.
_Avoid_: cost, runtime, priority, cost-model class

**ComplexityBatch**:
A complexity orchestration batch (复杂度编排批次): a workflow planning group containing **Queries** with the same effective **Complexity Type**. A **ComplexityBatch** expresses materialized view generation and reuse order, not resource scheduling or intra-batch concurrency.
_Avoid_: execution batch, parallel batch, Spark batch, resource scheduling batch, QueryBlock batch

### Materialized View Planning

**Materialized View**:
A persisted relational result that can be used by later rewrite decisions when it has been successfully built and is available for the current batch. In this project, materialized views are the primary reuse mechanism for query acceleration.
_Avoid_: cache, temp table, summary table

**MV Candidate**:
A candidate materialized view (候选物化视图): a proposed **Materialized View** generated for the current **ComplexityBatch** before it is successfully materialized. An **MV Candidate** should be tied to target queries, grouping expressions, measures, and a clear reuse reason, but it is not part of **Materialized View State** until successfully materialized.
_Avoid_: global MV candidate, preselected MV, available materialized view

**Batch-local MV Generation**:
Batch-local materialized view generation (批内物化视图生成): the rule that **MV Candidates** are generated and used within the current **ComplexityBatch** under that batch's structural complexity level, rather than from a global pre-execution search. The first batch starts with an empty **Materialized View State**, generates its own materialized views, and then rewrites its own SQL with those views; later batches first use historical views, then generate additional views, then rewrite again with the expanded state.
_Avoid_: global MV selection, upfront MV enumeration, current-batch-only MV, one-shot global MV generation

**Materialized View State**:
Materialized view state (物化视图状态): the global set of successfully materialized views available to rewrite decisions. It is empty before Batch-1, then grows incrementally as each **ComplexityBatch** materializes new views; later batches can use both historical views and views produced by immediately preceding batches. `materialized_mvs` may remain as an implementation artifact name.
_Avoid_: Materialized MV State, MV registry, MV catalog

**Historical MV Rewrite**:
Historical materialized view rewrite (历史物化视图重写): the rewrite step that uses materialized views produced by earlier batches before generating new **MV Candidates** for the current batch. It is distinct from the final rewrite after current-batch materialization.
_Avoid_: initial rewrite, pre-rewrite, 历史物化视图改写

**Final Rewrite**:
Final rewrite (最终重写): the rewrite step that uses the updated **Materialized View State** after current-batch materialization. It uses historical views plus newly materialized current-batch views, and produces the SQL text that will be executed or dry-run for the current batch.
_Avoid_: historical rewrite, 最终改写

**Fallback**:
Conservative fallback (保守回退): a decision to keep original SQL semantics when a rewrite or extraction decision is unsupported or unsafe. Fallback is preferred over a speculative rewrite when semantic equivalence is uncertain.
_Avoid_: failure, skipped query, failed rewrite

### Agent Workflow

**ETL MV Agent Prototype**:
The project-level name for the research prototype. It refers to the overall system for agent-guided materialized view planning in ETL analytical workflows, not only to the first implementation version.
_Avoid_: MV_gen, ETL agent, Agent-only Prototype when naming the overall system

**Agent-only Prototype**:
Agent-only prototype (Agent-only 原型): the first research prototype in which most planning judgments are made by LLM agents guided by markdown rules, while loading and execution remain code-driven. Its purpose is to validate the workflow loop before replacing selected judgments with deterministic algorithms.
_Avoid_: full optimizer, production framework, pure LLM system, 纯 Agent 原型

**Rules**:
Rules (规则文件): markdown guidance read by an LLM agent to constrain its planning judgment for a specific role. **Rules** define decision criteria and output expectations, not executable algorithms or prompt templates alone.
_Avoid_: code, schema, prompt only, algorithm implementation

**Artifact**:
A durable project output that serves as the source of truth between workflow stages. Use the English term **Artifact** in project writing; artifacts represent query analysis, families, batches, materialized view candidates, rewritten SQL, execution logs, or feedback.
_Avoid_: in-memory state, temporary variable, 工件, 中间产物 as the canonical term

**Execution Log**:
A durable record of agent inputs, outputs, timing, and errors for a workflow run. It supports feedback and diagnosis but is not itself a query-planning artifact.
_Avoid_: business result, materialized state

**Self-Iteration Feedback**:
Self-iteration feedback (自迭代反馈): structured recommendations derived from execution logs to improve future rules or configuration. Feedback does not directly change query semantics, mutate prior artifacts, or automatically edit **Rules**.
_Avoid_: automatic rule rewrite, online learning, adaptive optimizer

## Example Dialogue

Developer: "Can q42.outer and q52.outer be put into the same batch?"

Domain expert: "Only the **Queries** q42 and q52 enter a **ComplexityBatch**. Their **QueryBlocks** can belong to the same **QueryFamily**, and that family can justify an **MV Candidate** inside the batch."

Developer: "So the system can generate all reusable materialized views before batching?"

Domain expert: "No. The prototype uses **Batch-local MV Generation**. The **Materialized View State** is empty at the beginning. Batch-1 generates and materializes its own views, then performs **Final Rewrite** for Batch-1. Batch-2 first performs **Historical MV Rewrite** using Batch-1's views, then generates and materializes additional views, expands the **Materialized View State**, and performs **Final Rewrite** for Batch-2."

Developer: "What if the agent is unsure that a rewritten query is equivalent?"

Domain expert: "Use **Fallback**. Keeping the original SQL semantics is preferred over a speculative rewrite."

## Flagged Ambiguities

**Query vs SQL Text**:
Use **Query** for the executable unit identified by `query_id`. Use **SQL Text** when referring only to textual SQL contents, including raw, rewritten, or build SQL.

**ComplexityBatch vs execution scheduling**:
A **ComplexityBatch** is a planning and reuse-order concept. It does not imply parallel execution limits, resource allocation, or sub-batch scheduling.

**Materialized View State expansion vs MV replacement**:
When moving from one batch to the next, the system expands **Materialized View State** by adding newly materialized views to historical views. It does not replace an earlier materialized view with a later one unless that replacement is explicitly described as a separate decision.
