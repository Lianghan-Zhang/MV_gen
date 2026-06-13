# Batch Classification Difference Review

对照文件:

- 当前结果: `batch_classification.md`
- 另一个 AI 结果: `another_result.md`
- 判定规则: `batch_rules.md`

本次只审查两个结果中 batch 不一致的 SQL。两个文件均覆盖 105 条 SQL，差异 SQL 共 11 条。
下表里的“当前原分类”指复核前 `batch_classification.md` 中的分类；复核完成后，已将需要调整的结论同步回 `batch_classification.md`。

## 最终审查结论

| SQL | 当前原分类 | 另一个 AI 分类 | 最终分类 | 最终原因 |
| --- | --- | --- | --- | --- |
| q4.sql | Batch-4 | Batch-5 | Batch-4 | 查询用 CTE + `UNION ALL` 统一 `store_sales/catalog_sales/web_sales` 年度销售并做渠道增长对比；没有 returns、inventory、outer join、anti join、set 语义。按规则中“跨渠道 sales 统一或对比”优先归 Batch-4，复杂年度 ratio 不足以压过 Batch-4 边界。 |
| q8.sql | Batch-5 | Batch-2 | Batch-5 | SQL 中明确出现 `INTERSECT`。`batch_rules.md` 的整体优先级第 2 条规定 SQL 存在 `INTERSECT` 等强高风险语义时进入 Batch-5；这里不应因为 `INTERSECT` 出现在地址维度过滤子查询中而降为 Batch-2。 |
| q13.sql | Batch-2 | Batch-1 | Batch-2 | 主体是单一 `store_sales`，但 where 条件包含两组多分支 OR 布尔过滤，规则中 Batch-2 明确允许并覆盖“复杂布尔过滤”。它比基础 join-filter 更复杂，因此不放 Batch-1。 |
| q17.sql | Batch-5 | Batch-3 | Batch-3 | 查询组合 `store_sales + store_returns + catalog_sales`，核心是 sales/returns 关联后再扩展到 catalog_sales 统计；没有 outer join、anti join、INTERSECT/EXCEPT 或空值保留语义。按规则更接近 Batch-3 的 sales + returns / returns 扩展，而不是 Batch-5。 |
| q21.sql | Batch-2 | Batch-1 | Batch-2 | 单一 `inventory` 事实域，但先在派生表里计算 before/after 库存，再外层做 ratio 过滤。规则中派生 wrapper、单域 ratio/统计增强进入 Batch-2；不是最基础库存快照。 |
| q22.sql | Batch-2 | Batch-1 | Batch-1 | 单一 `inventory` 事实域，只有稳定维表 join、平均库存聚合和 item 层级轻量 `ROLLUP`。规则说明单事实域轻量 rollup 可放 Batch-1 或 Batch-2；本查询没有 CTE、阈值、window 或复杂后处理，最终归 Batch-1。 |
| q48.sql | Batch-2 | Batch-1 | Batch-2 | 单一 `store_sales`，但包含复杂人口属性和地址条件的多分支 OR 过滤。Batch-2 规则明确覆盖复杂布尔过滤，因此比基础 Batch-1 更合适。 |
| q61.sql | Batch-2 | Batch-5 | Batch-2 | 单一 `store_sales`，两个派生聚合分别算促销销售和总销售，再做比例。没有 outer/set/anti、多事实混合或复杂 rank/window；规则中派生 wrapper 和单域 ratio 属于 Batch-2，不需要提升到 Batch-5。 |
| q62.sql | Batch-2 | Batch-1 | Batch-1 | 单一 `web_sales`，稳定维表 join 后用多个 `CASE` 聚合统计 shipping delay 桶。Batch-1 允许简单 case aggregate；这里没有派生 wrapper、CTE、window 或阈值过滤，最终归 Batch-1。 |
| q83.sql | Batch-3 | Batch-5 | Batch-3 | 查询只涉及 `store_returns/catalog_returns/web_returns` 三类 returns 事实，CTE 后按 item 做 returns 数量对齐和比例；没有 sales 混入、outer/set/anti。规则中 returns 事实域扩展归 Batch-3，不能仅因三类 returns 分支就升为 Batch-5。 |
| q99.sql | Batch-2 | Batch-1 | Batch-1 | 单一 `catalog_sales`，稳定维表 join 后用多个 `CASE` 聚合统计 shipping delay 桶。与 q62 同类，属于 Batch-1 允许的简单 case aggregate。 |

## 为什么会不一样

| 差异类型 | 涉及 SQL | 审查口径 |
| --- | --- | --- |
| 强高风险语义优先级理解不同 | q8.sql | 只要 SQL 中存在 `INTERSECT`，按整体优先级直接进 Batch-5，即使它位于维度过滤子查询中。 |
| 跨渠道复杂度是否压过 Batch-4 | q4.sql | 只要核心仍是 store/catalog/web sales 统一、合并、对比，且没有 returns/inventory/outer/set/anti，就保留 Batch-4。 |
| 复杂布尔过滤是否算增强 | q13.sql, q48.sql | 复杂多分支 OR 条件属于 Batch-2 的“复杂布尔过滤”，不是基础 Batch-1。 |
| returns 扩展与复杂多事实的边界 | q17.sql, q83.sql | 没有 outer/set/anti/空值保留时，sales + returns 或 returns 多分支扩展优先按 Batch-3 处理。 |
| 单域派生 ratio 与 Batch-5 的边界 | q21.sql, q61.sql | 单事实域派生 wrapper 或 ratio 属于 Batch-2；只有叠加高风险语义、复杂 rank/window/rollup 或复杂多事实时才升 Batch-5。 |
| 轻量 rollup/case aggregate 是否留在 Batch-1 | q22.sql, q62.sql, q99.sql | Batch-1 明确允许单域轻量 rollup 和简单 case aggregate；无 CTE/阈值/window/派生复杂后处理时保留 Batch-1。 |

## 对当前结果的同步修正

已同步修正 `batch_classification.md` 中 4 条边界分类:

| SQL | 修正前 | 修正后 |
| --- | --- | --- |
| q17.sql | Batch-5 | Batch-3 |
| q22.sql | Batch-2 | Batch-1 |
| q62.sql | Batch-2 | Batch-1 |
| q99.sql | Batch-2 | Batch-1 |

修正后 `batch_classification.md` 汇总:

| Batch | 数量 |
| --- | ---: |
| Batch-1 | 15 |
| Batch-2 | 29 |
| Batch-3 | 16 |
| Batch-4 | 17 |
| Batch-5 | 27 |
| Other | 1 |
| Total | 105 |

## 二次复查记录

复查结论: 上述最终审查结论保持不变，本次未再修改 `batch_classification.md` 的 batch 归属。

重点复查点:

| SQL | 二次复查结论 | 复查原因 |
| --- | --- | --- |
| q4.sql | 保持 Batch-4 | 虽然 CTE 后有多别名年度对比和 ratio，但事实域只是在 `store_sales/catalog_sales/web_sales` 三个销售渠道之间统一和比较；没有 returns、inventory、outer join、anti join、INTERSECT/EXCEPT。按整体优先级第 3 条仍归跨渠道 sales 统一批。 |
| q8.sql | 保持 Batch-5 | `INTERSECT` 是 `batch_rules.md` 明确列出的强高风险语义；规则没有限定必须发生在事实表子查询中，因此不能降为 Batch-2。 |
| q13.sql | 保持 Batch-2 | 单 `store_sales`，但两大组 OR 条件把人口属性、价格、地址、利润区间组合在一起，符合 Batch-2 的复杂布尔过滤。 |
| q17.sql | 保持 Batch-3 | 与 q25/q29 同形，都是 `store_sales + store_returns + catalog_sales` 的内连接链路，核心是 sales/returns 关联后扩展到 catalog sales 指标；没有 outer/set/anti/空值保留。这里不是 Batch-5 里说的“多渠道 + returns”高风险混合。 |
| q21.sql | 保持 Batch-2 | 单 `inventory`，但有派生 wrapper 和 before/after ratio 外层过滤，不是最基础库存快照。 |
| q22.sql | 保持 Batch-1 | 单 `inventory`，稳定 join + 平均库存 + 轻量 `ROLLUP`，没有 CTE、window、阈值或派生后处理；Batch-1 允许单域轻量 rollup。 |
| q48.sql | 保持 Batch-2 | 与 q13 同类，复杂多分支 OR 布尔过滤是主要复杂点。 |
| q61.sql | 保持 Batch-2 | 单 `store_sales` 的两个派生聚合做促销/总销售比例，属于单域 ratio 和派生 wrapper；没有 Batch-5 强高风险语义。 |
| q62.sql | 保持 Batch-1 | 单 `web_sales` 的 shipping delay 简单 case aggregate，与 q43 的轻量 case 聚合口径一致，不需要升到 Batch-2。 |
| q83.sql | 保持 Batch-3 | 只有 `store_returns/catalog_returns/web_returns` 三类 returns 事实，属于 returns 事实域扩展；没有 sales 混入、outer、anti、INTERSECT/EXCEPT。三个 returns CTE 和比例计算不足以压过 Batch-3 的 returns 扩展边界。 |
| q99.sql | 保持 Batch-1 | 单 `catalog_sales` 的 shipping delay 简单 case aggregate，与 q62 同口径。 |
