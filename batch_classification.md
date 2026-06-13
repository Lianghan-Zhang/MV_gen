# TPC-DS Spark SQL Batch Classification

依据: `batch_rules.md` 的判定优先级与各 batch 规则。

| SQL | Batch | 理由 |
| --- | --- | --- |
| q01.sql | Batch-1 | 单一 `store_sales` 事实表，只有基础过滤，事实主干清晰且结构稳定。 |
| q02.sql | Batch-1 | 单一 `store_sales` 事实域连接 `item` 维表并做简单过滤，属于基础 join-filter。 |
| q1.sql | Batch-3 | `store_returns` returns 事实域查询，带 CTE 与同店平均阈值，但没有 outer/set/anti 语义；returns 单域按规则放入 Batch-3。 |
| q10.sql | Batch-4 | 通过 `EXISTS` 同时判断 `store_sales`、`web_sales`、`catalog_sales` 渠道购买存在性，核心是跨渠道 sales 存在性对比。 |
| q11.sql | Batch-4 | CTE 中以 `UNION ALL` 合并 `store_sales` 与 `web_sales` 年度销售并做渠道对比，属于跨渠道 sales 统一。 |
| q12.sql | Batch-2 | 单一 `web_sales` 事实域，带分组后的 revenue ratio/window 统计扩展，未跨事实组合。 |
| q13.sql | Batch-2 | 单一 `store_sales` 事实域，但包含多组复杂人口与地址布尔过滤，复杂度高于基础查询。 |
| q14a.sql | Batch-5 | 跨三销售渠道并包含 `INTERSECT`、多层 CTE、`HAVING` 与 `ROLLUP`，命中强高风险语义优先级。 |
| q14b.sql | Batch-5 | 跨三销售渠道并包含 `INTERSECT`、多层 CTE 与 `HAVING`，按集合高风险语义优先进入 Batch-5。 |
| q15.sql | Batch-1 | 单一 `catalog_sales` 事实域，维表 join、基础过滤和简单分组聚合，未出现复杂子查询或跨事实组合。 |
| q16.sql | Batch-5 | `catalog_sales` 与 `catalog_returns` 组合，并含 `NOT EXISTS` 反连接语义，强高风险优先。 |
| q17.sql | Batch-3 | `store_sales + store_returns + catalog_sales` 组合，核心是 sales/returns 后再关联 catalog_sales 的 returns 扩展；未含 outer/set/anti，按成对事实/returns 扩展处理。 |
| q18.sql | Batch-2 | 单一 `catalog_sales` 事实域，使用 `ROLLUP` 与多项统计聚合，属于单域统计扩展。 |
| q19.sql | Batch-1 | 单一 `store_sales` 事实域，维表 join、过滤、分组汇总，结构稳定。 |
| q2.sql | Batch-4 | CTE 中 `UNION ALL` 合并 `web_sales` 与 `catalog_sales`，并进行跨渠道周销售对比。 |
| q20.sql | Batch-2 | 单一 `catalog_sales` 事实域，带分组后的 revenue ratio/window 统计，属于同事实域增强。 |
| q21.sql | Batch-2 | 单一 `inventory` 事实域，派生表比较库存前后 ratio，属于库存单域增强统计。 |
| q22.sql | Batch-1 | 单一 `inventory` 事实域，只有稳定维表 join、平均库存聚合和轻量层级 `ROLLUP`，符合 Batch-1 允许的单域轻量 rollup。 |
| q23a.sql | Batch-4 | 多层 CTE 中跨 `store_sales`、`catalog_sales`、`web_sales` 计算客户销售，核心是跨渠道 sales 对比。 |
| q23b.sql | Batch-4 | 与 q23a 类似，跨三销售渠道构造客户年度销售并比较，未混入 returns/outer/set。 |
| q24a.sql | Batch-3 | `store_sales + store_returns` 成对事实组合，并用 CTE/HAVING 分析退货相关客户。 |
| q24b.sql | Batch-3 | `store_sales + store_returns` 成对事实组合，结构与 q24a 同类，未含 Batch-5 强风险语义。 |
| q25.sql | Batch-3 | `store_sales`、`store_returns`、`catalog_sales` 组合，核心是 sales 与 returns 关联后的成对事实扩展。 |
| q26.sql | Batch-1 | 单一 `catalog_sales` 事实域，维表 join、过滤和基础分组统计。 |
| q27.sql | Batch-2 | 单一 `store_sales` 事实域，使用 `ROLLUP`/`grouping` 做层级统计，属于单域统计扩展。 |
| q28.sql | Batch-2 | 单一 `store_sales` 事实域，但由多个派生 bucket 子查询组成，属于多桶统计增强。 |
| q29.sql | Batch-3 | `store_sales + store_returns + catalog_sales` 组合，主要是 sales/returns 事实关联扩展，未见 outer/set/anti。 |
| q3.sql | Batch-1 | 单一 `store_sales` 事实域，基础维表 join、过滤、分组求和。 |
| q30.sql | Batch-3 | 单一 `web_returns` returns 事实域，带 CTE/阈值比较；returns 单域扩展按规则进入 Batch-3。 |
| q31.sql | Batch-4 | CTE 中对 `store_sales` 与 `web_sales` 做年度销售对比，核心是跨销售渠道对齐。 |
| q32.sql | Batch-2 | 单一 `catalog_sales` 事实域，包含相关标量子查询阈值过滤，属于同事实域增强。 |
| q33.sql | Batch-4 | `UNION ALL` 合并 store/catalog/web 三渠道销售并统一汇总，未混入 returns 或 outer/set 语义。 |
| q34.sql | Batch-2 | 单一 `store_sales` 事实域，派生 ticket 聚合后再与 customer 过滤，属于派生 wrapper 增强。 |
| q35.sql | Batch-4 | 通过 `EXISTS` 判断 store/web/catalog 多渠道购买存在性，属于跨渠道 sales 存在性分析。 |
| q36.sql | Batch-5 | 单一 `store_sales` 但叠加 `ROLLUP`、`grouping`、`rank()` window 与层级排序，属于复杂 window/rank 高风险。 |
| q37.sql | Batch-3 | `catalog_sales + inventory` 成对事实组合，通过库存与销售关联做过滤。 |
| q38.sql | Batch-5 | 跨 store/catalog/web 三渠道并使用 `INTERSECT`，集合语义优先进入 Batch-5。 |
| q39a.sql | Batch-2 | 单一 `inventory` 事实域，CTE 中计算库存均值/方差等统计指标，属于库存单域增强。 |
| q39b.sql | Batch-2 | 单一 `inventory` 事实域，CTE 统计库存波动并后续过滤，属于库存单域增强。 |
| q4.sql | Batch-4 | CTE 以 `UNION ALL` 统一 `store_sales`、`catalog_sales`、`web_sales`，核心是多渠道 sales 汇总对比。 |
| q40.sql | Batch-5 | `catalog_sales + catalog_returns` 并使用 `LEFT OUTER JOIN`，空值保留语义优先进入 Batch-5。 |
| q41.sql | Other | 仅查询 `item` 维表并使用维表内相关子查询，无明确事实表主干。 |
| q42.sql | Batch-1 | 单一 `store_sales` 事实域，基础维表 join、过滤和分组求和。 |
| q43.sql | Batch-1 | 单一 `store_sales` 事实域，按店铺做简单 case aggregate，属于 Batch-1 允许的轻量 case 聚合。 |
| q44.sql | Batch-5 | 单一 `store_sales` 但包含多层派生、`HAVING` 标量阈值和双向 `rank()`，属于复杂 window/rank 高风险。 |
| q45.sql | Batch-2 | 单一 `web_sales` 事实域，带 `IN (SELECT ...)` 半子查询过滤，属于同事实域增强。 |
| q46.sql | Batch-2 | 单一 `store_sales` 事实域，先派生 ticket 粒度聚合再外层过滤展示，属于派生 wrapper。 |
| q47.sql | Batch-5 | 单一 `store_sales` 但 CTE 中包含 window 平均、`rank()`、自连接前后月比较，属于复杂 window/rank。 |
| q48.sql | Batch-2 | 单一 `store_sales` 事实域，包含多组复杂人口与地址布尔过滤，复杂度高于基础查询。 |
| q49.sql | Batch-5 | 跨三渠道 sales/returns，并使用 `LEFT OUTER JOIN`、`UNION` 与 `rank()`，命中多项高风险语义。 |
| q5.sql | Batch-5 | 跨三销售渠道且混入三类 returns，使用 `LEFT OUTER JOIN`、`UNION ALL` 与 `ROLLUP`，属于复杂多事实混合。 |
| q50.sql | Batch-3 | `store_sales + store_returns` 成对事实组合，围绕 ticket/item/customer 关联分析退货销售。 |
| q51.sql | Batch-5 | `store_sales` 与 `web_sales` 对齐时使用 `FULL OUTER JOIN` 并做 window 计算，空值保留语义优先。 |
| q52.sql | Batch-1 | 单一 `store_sales` 事实域，基础 item/date 过滤与分组聚合。 |
| q53.sql | Batch-2 | 单一 `store_sales` 事实域，派生表中使用 window 平均并外层 ratio 过滤，属于单域统计增强。 |
| q54.sql | Batch-4 | CTE 中跨 store/catalog/web 三渠道识别客户购买行为，核心是多渠道 sales 统一与对比。 |
| q55.sql | Batch-1 | 单一 `store_sales` 事实域，简单 join-filter 与分组销售额聚合。 |
| q56.sql | Batch-4 | `UNION ALL` 合并 store/catalog/web 三渠道 item 销售并统一汇总，未混入 returns。 |
| q57.sql | Batch-5 | 单一 `catalog_sales` 但 CTE 中包含 window 平均、`rank()` 和自连接前后月比较，属于复杂 window/rank。 |
| q58.sql | Batch-4 | 跨 store/catalog/web 三渠道 CTE 对齐并比较销售 ratio，核心是多渠道 sales 对比。 |
| q59.sql | Batch-2 | 单一 `store_sales` 事实域，CTE 构造周内多日 sales 后做跨年 ratio，对应单域增强。 |
| q6.sql | Batch-2 | 单一 `store_sales` 事实域，包含 `HAVING` 和标量平均阈值子查询，属于同事实域增强。 |
| q60.sql | Batch-4 | `UNION ALL` 合并 store/catalog/web 三渠道销售额并统一分组，属于跨渠道 sales 统一。 |
| q61.sql | Batch-2 | 单一 `store_sales` 事实域，两个派生聚合计算促销占比，属于单域 ratio/派生增强。 |
| q62.sql | Batch-1 | 单一 `web_sales` 事实域，稳定维表 join 后做 shipping delay 的简单 case aggregate，符合 Batch-1 允许的轻量 case 聚合。 |
| q63.sql | Batch-2 | 单一 `store_sales` 事实域，window 平均与外层偏差 ratio 过滤，属于单域统计增强。 |
| q64.sql | Batch-5 | 混合 `catalog_sales/catalog_returns` 与 `store_sales/store_returns`，多层 CTE、HAVING 与跨 returns/sales 混合，属于复杂多事实。 |
| q65.sql | Batch-2 | 单一 `store_sales` 事实域，嵌套派生计算店铺平均并用阈值筛选，属于派生统计增强。 |
| q66.sql | Batch-4 | `UNION ALL` 合并 `web_sales` 与 `catalog_sales` 仓储配送统计，属于跨销售渠道统一。 |
| q67.sql | Batch-5 | 单一 `store_sales` 但包含 `ROLLUP`、`rank()` window 与多层外层过滤，属于复杂 window/rank。 |
| q68.sql | Batch-2 | 单一 `store_sales` 事实域，派生 ticket 聚合后再外层 join 与过滤，属于派生 wrapper。 |
| q69.sql | Batch-5 | 跨 store/web/catalog 渠道并使用 `NOT EXISTS` 反连接语义，强高风险优先。 |
| q7.sql | Batch-1 | 单一 `store_sales` 事实域，维表 join、过滤、分组平均，结构稳定。 |
| q70.sql | Batch-5 | 单一 `store_sales` 但叠加 `ROLLUP`、`grouping`、`rank()` 和派生外层排序，属于复杂 window/rank。 |
| q71.sql | Batch-4 | `UNION ALL` 合并 store/catalog/web 三渠道销售事件，核心是跨渠道 sales 统一。 |
| q72.sql | Batch-5 | `catalog_sales + catalog_returns + inventory` 组合，并含 `LEFT OUTER JOIN`，属于复杂多事实且空值保留。 |
| q73.sql | Batch-2 | 单一 `store_sales` 事实域，派生 ticket 聚合与外层 customer 过滤，属于派生 wrapper。 |
| q74.sql | Batch-4 | CTE 中以 `UNION ALL` 合并 store/web 销售并比较年度客户指标，属于跨渠道 sales 对比。 |
| q75.sql | Batch-5 | 跨三销售渠道并混入三类 returns，虽然无 outer/set，但属于多渠道 + returns 复杂混合。 |
| q76.sql | Batch-4 | `UNION ALL` 合并 store/catalog/web 三渠道销售记录并统一输出，未混入 returns/outer/set。 |
| q77.sql | Batch-5 | 跨三销售渠道并混入 returns，使用 `LEFT JOIN` 与 `ROLLUP`，命中空值保留和复杂多事实。 |
| q78.sql | Batch-5 | 跨三销售渠道、三类 returns，并使用多处 `LEFT JOIN` 进行空值保留，属于复杂多事实混合。 |
| q79.sql | Batch-2 | 单一 `store_sales` 事实域，派生 ticket 聚合再外层 customer 展示，属于派生 wrapper。 |
| q8.sql | Batch-5 | 单一 `store_sales` 主体但包含 `INTERSECT` 与 `HAVING` 子查询，集合语义优先进入 Batch-5。 |
| q80.sql | Batch-5 | 跨三销售渠道并混入 returns，使用 `LEFT OUTER JOIN`、`UNION ALL` 与 `ROLLUP`，属于复杂多事实。 |
| q81.sql | Batch-3 | 单一 `catalog_returns` returns 事实域，带 CTE 和同 call center 平均阈值；returns 扩展放入 Batch-3。 |
| q82.sql | Batch-3 | `store_sales + inventory` 成对事实组合，通过库存存在性/商品维度过滤销售。 |
| q83.sql | Batch-3 | 跨 `store_returns/catalog_returns/web_returns` 三类 returns 做统一统计，属于 returns 事实域扩展且无 outer/set/anti。 |
| q84.sql | Batch-3 | 单一 `store_returns` returns 事实域，结合维表过滤退货客户，按 returns 单域规则进入 Batch-3。 |
| q85.sql | Batch-3 | `web_sales + web_returns` 成对事实组合，围绕 order/item 与退货原因做统计。 |
| q86.sql | Batch-5 | 单一 `web_sales` 但包含 `ROLLUP`、`grouping` 与 `rank()` window，属于复杂层级 rank。 |
| q87.sql | Batch-5 | 跨 store/catalog/web 三渠道并使用 `EXCEPT`，集合差集语义优先进入 Batch-5。 |
| q88.sql | Batch-2 | 单一 `store_sales` 事实域，由多个时间段派生 count 子查询组成，属于多桶统计增强。 |
| q89.sql | Batch-2 | 单一 `store_sales` 事实域，派生月销售与 window 平均偏差过滤，属于单域统计增强。 |
| q9.sql | Batch-2 | 单一 `store_sales` 事实域，多个标量子查询 bucket 阈值与 CASE 选择，属于多桶/标量增强。 |
| q90.sql | Batch-2 | 单一 `web_sales` 事实域，两个派生 count 子查询计算 AM/PM ratio，属于单域 ratio 增强。 |
| q91.sql | Batch-3 | 单一 `catalog_returns` returns 事实域，维表过滤并聚合退货损失，按 returns 单域放入 Batch-3。 |
| q92.sql | Batch-2 | 单一 `web_sales` 事实域，含相关标量子查询阈值过滤折扣，属于同事实域增强。 |
| q93.sql | Batch-5 | `store_sales + store_returns` 并使用 `LEFT OUTER JOIN`，空值保留语义优先进入 Batch-5。 |
| q94.sql | Batch-5 | `web_sales + web_returns` 并含 `NOT EXISTS` 反连接语义，强高风险优先。 |
| q95.sql | Batch-3 | `web_sales + web_returns` 成对事实组合，使用 CTE/IN 半子查询定位退货订单，未含 NOT EXISTS/outer。 |
| q96.sql | Batch-1 | 单一 `store_sales` 事实域，基础 join-filter 与 count 聚合，结构稳定。 |
| q97.sql | Batch-5 | `store_sales` 与 `catalog_sales` 通过 `FULL OUTER JOIN` 对齐，空值保留语义优先进入 Batch-5。 |
| q98.sql | Batch-2 | 单一 `store_sales` 事实域，分组后计算 window revenue ratio，属于单域统计增强。 |
| q99.sql | Batch-1 | 单一 `catalog_sales` 事实域，稳定维表 join 后做 shipping delay 的简单 case aggregate，符合 Batch-1 允许的轻量 case 聚合。 |

## 汇总

| Batch | 数量 |
| --- | ---: |
| Batch-1 | 15 |
| Batch-2 | 29 |
| Batch-3 | 16 |
| Batch-4 | 17 |
| Batch-5 | 27 |
| Other | 1 |
| Total | 105 |
