# Batch Choosen rules

## 一、整体判定优先级

| 判定顺序 | 规则                                                                                                            | 归入      |
| ---: | ------------------------------------------------------------------------------------------------------------- | ------- |
|    1 | SQL 无明确事实表主干，或主要是纯维表、纯标量、低业务事实语义查询                                                                            | Other   |
|    2 | SQL 存在强高风险语义：`INTERSECT`、`EXCEPT`、`NOT EXISTS`、anti join、`LEFT/FULL OUTER JOIN`、复杂集合分支、多事实混合且带空值保留语义          | Batch-5 |
|    3 | SQL 是跨 `store_sales / catalog_sales / web_sales` 的多销售渠道统一或对比，但不混入复杂 returns / inventory / outer join / set 语义 | Batch-4 |
|    4 | SQL 涉及两个事实域组合，例如 `sales + returns`、`sales + inventory`、returns 事实域扩展，但不含 Batch-5 的强高风险语义                      | Batch-3 |
|    5 | SQL 是单事实域或单渠道事实域，但带 CTE、HAVING、semi-subquery、标量阈值、派生 wrapper、复杂过滤、统计扩展等                                       | Batch-2 |
|    6 | SQL 是单事实域基础查询，结构稳定，主要是简单 join-filter 或简单 group aggregate                                                      | Batch-1 |

---

## 二、各 batch 的分类规则

|   Batch | 中文名                                  | SQL 主体特征                                                                              | 允许出现的 SQL 结构                                                                                                                                                                           | 不应包含的 SQL 特征                                                                                                  |
| ------: | ------------------------------------ | ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Batch-1 | 基础事实域种子批                             | 单事实域为主；事实表主干清晰；join 图谱稳定；主要围绕 `store_sales`、`catalog_sales`、`web_sales` 或 `inventory` | 简单 inner join、简单 where filter、简单 group by、简单聚合、简单 case aggregate、单域轻量 ratio、单域轻量 rollup                                                                                                | 跨销售渠道组合、sales + returns、sales + inventory、多事实混合、复杂 CTE、复杂 window/rank、outer join、anti join、intersect/except   |
| Batch-2 | 同事实域扩展批                              | 仍然是单事实域或单渠道事实域；没有跨事实组合；查询复杂度高于 Batch-1                                                | CTE、HAVING、semi-subquery、标量子查询阈值、派生 wrapper、多桶统计、复杂布尔过滤、单域统计指标、单域 window/ratio/rollup 外层分析                                                                                             | returns 与 sales 组合、inventory 与 sales 组合、store/catalog/web 跨渠道统一、outer join、anti join、intersect/except、复杂多事实混合 |
| Batch-3 | 成对事实 / returns / inventory-sales 组合批 | 涉及两个事实域之间的组合；主要是 `sales + returns`、`sales + inventory`，或 returns 事实域相关查询              | sales 与 returns 基于 order/ticket/item/customer/date 的组合；sales 与 inventory 的商品/日期/仓库存在性或过滤组合；returns 单域聚合或过滤                                                                             | store/catalog/web 三渠道统一型查询、复杂跨渠道 + returns 混合、outer join、anti join、intersect/except、多层集合分支                    |
| Batch-4 | 跨渠道 sales 统一批                        | 涉及两个或三个销售渠道事实表：`store_sales`、`catalog_sales`、`web_sales`；核心语义是多渠道 sales 对齐、合并、对比      | `UNION ALL` 多渠道分支、跨渠道 customer/item/date 聚合、渠道对比、渠道存在性判断、跨渠道销售指标统一                                                                                                                     | returns、inventory、sales + returns、sales + inventory、outer join、anti join、intersect/except、复杂多事实混合             |
| Batch-5 | 复杂消费 / 高风险语义批                        | 查询语义复杂，不能仅按单事实域或简单多事实组合归类；通常涉及集合、空值保留、反连接、复杂后聚合或复杂多事实混合                               | `INTERSECT`、`EXCEPT`、`NOT EXISTS`、anti join、`LEFT OUTER JOIN`、`FULL OUTER JOIN`、复杂 window/rank、复杂 rollup/cube/grouping、多层 CTE、复杂 ratio、sales + returns + inventory 混合、多渠道 + returns 混合 | 无特殊排除；只要满足强高风险语义，优先进入 Batch-5                                                                                 |
|   Other | fallback                             | 无明确事实表主干，或主要是纯维表/纯标量查询                                                                | 单表维表查询、常量/标量查询、无稳定事实域的查询                                                                                                                                                               | 有清晰事实表主干的查询通常不放 Other                                                                                         |

---

## 三、关键边界规则

| 边界问题                         | 归类规则                                                                                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 单事实表是否一定进 Batch-1？           | 不一定。简单单事实域查询进 Batch-1；如果有复杂 CTE、HAVING、统计扩展、semi-subquery 等，则进 Batch-2；如果有强高风险语义，则进 Batch-5。                                                                             |
| `inventory` 是否属于 Batch-1？    | `inventory` 是事实表。单域、基础库存快照类 SQL 可以进 Batch-1；复杂库存统计 SQL 进 Batch-2；`sales + inventory` 组合进 Batch-3；`inventory + returns + outer join` 等复杂混合进 Batch-5。                      |
| 有 `ROLLUP` 是否一定进 Batch-5？    | 不一定。单事实域、外层轻量 rollup 可放 Batch-1 或 Batch-2；如果 rollup 与复杂多层 CTE、rank、ratio、多事实混合叠加，则进 Batch-5。                                                                             |
| 有 window/rank 是否一定进 Batch-5？ | 通常倾向 Batch-5；但如果只是单域外层分析，且不改变事实域组合关系，可放 Batch-2。复杂 rank/window 与多事实、set、outer join 叠加时进 Batch-5。                                                                         |
| 有 CTE 是否一定进 Batch-5？         | 不一定。单事实域 CTE 通常进 Batch-2；跨事实、多分支、集合语义或复杂后处理 CTE 进 Batch-5。                                                                                                               |
| 跨多个事实表是否一定进 Batch-3？         | 不一定。`sales + returns`、`sales + inventory` 这类成对事实组合进 Batch-3；如果是 `store_sales/catalog_sales/web_sales` 的多渠道 sales 统一，进 Batch-4；如果还叠加 outer join、set、anti join，则进 Batch-5。 |
| 跨渠道 sales 是否一定进 Batch-4？     | 只有主要语义是 sales 多渠道统一、对比、合并时进 Batch-4；如果同时混入 returns、inventory、outer join、anti join 或复杂集合语义，则进 Batch-5。                                                                    |
| returns 单域查询放哪里？             | 简单 returns 单域或 returns 与维表过滤通常放 Batch-3，因为 returns 事实域在 sales 基础域之后引入；复杂 returns + set/outer/anti 语义放 Batch-5。                                                           |

---

## 四、压缩版规则

|   Batch | 一句话规则                                                                      |
| ------: | -------------------------------------------------------------------------- |
| Batch-1 | 单事实域、结构稳定、基础 join-filter 或基础聚合查询。                                          |
| Batch-2 | 单事实域增强查询，带 CTE、HAVING、阈值、派生、统计、复杂过滤，但不跨事实组合。                               |
| Batch-3 | 两个事实域组合查询，主要是 sales + returns、sales + inventory 或 returns 事实域扩展。           |
| Batch-4 | store/catalog/web 多销售渠道统一、合并、对比查询。                                         |
| Batch-5 | outer join、anti join、intersect/except、复杂 window/rank/rollup、复杂多事实混合等高风险查询。 |
|   Other | 无清晰事实表主干或纯维表/纯标量查询。                                                        |
