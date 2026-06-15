# Batch-local Candidate Group 拆分方案

## 背景

当前 `BatchLocalCandidateBuilder` 会对同一 batch 内的 supported QueryBlock 做两两相似度计算，并把通过 Jaccard / Containment 阈值的 pair 连成图。随后使用 eligible pair 图的连通分量生成 `candidate_group`。

这个方式保留了较高召回率，但会产生过大的 group。例如最新运行中，`batch_local_group_0001` 覆盖了 19 个 query、47 个 QueryBlock，并包含 915 条 pair evidence。这里的 915 不是 915 个 MV candidate，而是 47 个 QueryBlock 两两比较后，通过召回条件的 pair 证据。

问题在于：连通分量具有传递膨胀效应。

```text
A 和 B 相似
B 和 C 相似
C 和 D 相似

=> A、B、C、D 全部进入同一个 group
```

但 MV 生成需要的是“这一组 QueryBlock 能共同形成一个可证明的公共计算结构”，而不是“它们通过若干 pair 间接相似”。因此，完整 pairwise evidence 适合做审计和召回证据，不适合直接作为 LLM 的 candidate group。

## 目标

新的拆分目标是：

1. 保留完整 pairwise similarity artifact，方便后续审计。
2. 不再把 broad pair graph 的连通分量直接交给 LLM。
3. 用更严格的 MV 兼容规则生成小而明确的 candidate group。
4. 每个 candidate group 都应能表达一个清晰的公共计算结构。
5. 避免 LLM 一次接收几十个 QueryBlock 和数百条 pair evidence。

## 总体流程

推荐把当前流程：

```text
broad pair recall
-> connected components
-> LLM
```

改为：

```text
broad pair recall artifact
-> hard partition by MVCompatibilityKey
-> exact / anchor / rollup / dimension candidate groups
-> oversize recursive split
-> compact group summary + top evidence
-> LLM
```

## 第一步：保留完整 pairwise evidence

`pairwise_similarity` 仍然全量落盘。它记录每两个 QueryBlock 的相似度和召回原因，例如：

```json
{
  "left_qb_id": "q42.outer",
  "right_qb_id": "q52.outer",
  "table_jaccard": 1.0,
  "table_containment": 1.0,
  "join_jaccard": 1.0,
  "shared_tables": ["date_dim", "item", "store_sales"],
  "shared_fact_tables": ["store_sales"],
  "shared_join_edges": ["..."],
  "recall_reasons": ["exact_table_set", "same_core_fact_table_set"]
}
```

这些 evidence 只用于召回、审计和解释，不直接决定 LLM 的输入 group。

## 第二步：用 MVCompatibilityKey 做硬分桶

对每个 QueryBlock 先提取一个用于分桶的兼容 key：

```text
scope_class
family_type
core_fact_table_set
measure_key
```

### scope_class

区分外层查询、CTE 和子查询：

```text
outer
cte
subquery
```

默认不要把 `outer`、`cte`、`subquery` 混在同一个 LLM group。外层查询代表完整 SQL 语义，子查询通常只是局部过滤或局部聚合，二者的 MV 边界不同。

### family_type

区分事实表结构和纯维表结构：

```text
fact_based
dimension_only
```

事实表查询和维表过滤/维表投影不应直接混合。

### core_fact_table_set

核心事实表不同不混：

```text
store_sales
catalog_sales
web_sales
store_returns
catalog_returns
web_returns
inventory
```

例如 `store_sales + date_dim + item` 和 `web_sales + date_dim + item` 表集合很像，但事实表不同，不应直接合成一个 MV group。

### measure_key

聚合类型和来源字段不同不混：

```text
SUM(store_sales.ss_ext_sales_price)
AVG(store_sales.ss_quantity)
COUNT(*)
detail_no_measure
window_or_complex
```

例如 `SUM(ss_ext_sales_price)`、`AVG(ss_quantity)`、`COUNT(*)`、window ratio 不应混入同一个 fine-grain aggregate group。

第一层分桶建议使用：

```text
(scope_class, family_type, core_fact_table_set, measure_key)
```

## 第三步：在桶内生成小 candidate group

每个桶内部再按照 MV 语义生成小 group。建议支持四类 group。

### exact_shape_candidate

最安全的候选类型。

要求：

```text
table_set 一致
join_signature 一致
measure_key 一致
```

典型例子：

```text
q42.outer: store_sales + date_dim + item, SUM(ss_ext_sales_price)
q52.outer: store_sales + date_dim + item, SUM(ss_ext_sales_price)
```

这类适合交给 LLM 判断是否生成 fine-grain aggregate MV。

### superset_anchor_candidate

用于一个更宽的结构覆盖多个较窄结构。

做法是选择一个 anchor QueryBlock，其他成员必须是 anchor 的直接 subset：

```text
Anchor: store_sales + date_dim + item + store
Member: store_sales + date_dim + item
```

关键约束：

```text
每个 member 必须直接兼容 anchor
不能靠中间节点传递合并
```

不允许：

```text
A 兼容 B
B 兼容 C
所以 A/B/C 合并
```

必须是：

```text
A 兼容 anchor
B 兼容 anchor
C 兼容 anchor
```

### measure_rollup_candidate

用于聚合 roll-up。

要求：

```text
same fact table
same additive measure
group_by 可 roll-up
predicate 可 residual filter
```

典型例子是 q42/q52：两者共享 `SUM(store_sales.ss_ext_sales_price)`，但 group by 维度不同，可以生成更细粒度 MV，再由各 query 做 projection 或 roll-up。

不应混入：

```text
AVG
COUNT DISTINCT
window ratio
复杂 CASE measure
无法映射到单一物理列的派生 measure
```

### dimension_filter_candidate

用于维表过滤或维表子查询。

维表候选应更严格，通常要求：

```text
dimension_only
table_set 一致
predicate_shape 高度一致
```

例如多个 query 都包含相同的 `date_dim` 年/月过滤子查询，可以形成候选组。但不能把所有 `date_dim` 子查询因为表名相同就合并。

## 第四步：用 complete-linkage 防止传递膨胀

当前 connected component 的问题是：

```text
只要和组里任意一个点相似，就可能被拉进来
```

新的规则应改为：

```text
一个 QueryBlock 要加入 group，必须和 group 内所有已有成员兼容
```

也就是 complete-linkage：

```text
当前 group = [A, B]

C 想加入时：
C 必须兼容 A
C 也必须兼容 B
否则不能加入
```

这样可以避免：

```text
A-B 相似，B-C 相似，但 A-C 不相似
```

最后仍被合成一个大 group。

## 第五步：设置上下文上限

即使语义拆分更严格，也需要工程保护。

建议初始上限：

```text
max_qbs_per_group = 10 或 12
max_query_ids_per_group = 8
max_pair_evidence_sent_to_llm = 20
```

如果 group 超过上限，不应直接截断，而应继续按稳定规则递归拆分：

```text
先按 exact table_set 拆
再按 join_signature 拆
再按 measure_key 拆
再按 group_by_set 拆
再按 predicate_shape_set 拆
```

如果最后仍然超过上限，再按稳定排序 chunk，并记录：

```json
{
  "split_reason": "context_cap"
}
```

这样后续看 artifact 时可以区分：这个 group 是因为上下文过大被工程拆分，不是语义上天然分离。

## 第六步：给 LLM compact group summary

完整 pair evidence 可以落盘，但不要全部传给 LLM。LLM 输入应是 group 摘要加 top evidence：

```json
{
  "candidate_group_id": "batch_local_group_0001",
  "group_type": "measure_rollup_candidate",
  "query_ids": ["q42", "q52", "q55"],
  "qb_ids": ["q42.outer", "q52.outer", "q55.outer"],
  "common_tables": ["store_sales", "date_dim", "item"],
  "common_join_signature": ["..."],
  "measure_key": "SUM(store_sales.ss_ext_sales_price)",
  "union_group_by": ["..."],
  "predicate_summary": {
    "common_predicate_shapes": ["..."],
    "residual_predicate_shapes": ["..."]
  },
  "top_pair_evidence": ["最多 20 条"],
  "pair_evidence_total_count": 915,
  "pair_evidence_truncated": true
}
```

LLM 的任务应变成：

```text
这是一个 SUM(store_sales.ss_ext_sales_price) 的 roll-up group。
判断是否能生成 fine-grain aggregate MV。
如果不能，输出 skip 并说明原因。
```

而不是：

```text
这里有 47 个 QueryBlock 和 915 条 pair evidence，请自行理解。
```

## 对 batch2 的预期影响

当前 `batch_local_group_0001` 混合了：

```text
19 个 query
47 个 QueryBlock
outer + subquery
不同事实表/维表组合
不同 measure
915 条 pair evidence
```

按新方案，它应被拆成更小的组，例如：

```text
Group A:
outer, store_sales, SUM(ss_ext_sales_price), table_set = store_sales/date_dim/item

Group B:
outer, store_sales, AVG(ss_quantity/ss_list_price/...), ROLLUP

Group C:
outer, store_sales, COUNT(*), correlated/scalar subquery risk

Group D:
dimension-only date_dim subquery

Group E:
dimension-only item subquery
```

这样每个 group 的语义边界更清晰，LLM 不容易输出 `source_qb_ids: null` 这类半结构化结果，也更容易判断是否能生成安全 MV。

## 方案总结

核心原则：

```text
pairwise evidence 负责召回和审计
candidate group 负责给 LLM 一个明确的 MV 生成任务
```

当前问题不是 Jaccard / Containment 本身，而是把相似度图的连通分量直接作为 LLM group。更合适的方式是：

```text
保留完整 pairwise evidence
先做 MVCompatibilityKey 硬分桶
再生成 exact / anchor / rollup / dimension 小 group
使用 complete-linkage 防止传递膨胀
使用上下文上限兜底
最后只把 compact summary 和 top evidence 交给 LLM
```

这能同时保留召回率、可解释性和可运行性。
