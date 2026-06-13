下面按你当前认可的最终 batch list，逐条说明每条 SQL 为什么被放在当前 batch。
事实表/维表识别基于你上传的表字段对照 JSON，例如 `store_sales`、`catalog_sales`、`web_sales`、`inventory`、`store_returns`、`catalog_returns`、`web_returns` 等表及字段定义。

缩写说明：

| 缩写  | 表                 |
| --- | ----------------- |
| SS  | `store_sales`     |
| CS  | `catalog_sales`   |
| WS  | `web_sales`       |
| SR  | `store_returns`   |
| CR  | `catalog_returns` |
| WR  | `web_returns`     |
| INV | `inventory`       |

---

# Batch-1：基础事实域种子批

分类规则：**单事实域为主，结构稳定，主要是简单 join-filter、简单 group aggregate、简单 case aggregate；允许轻量外层 ratio/rollup，但不涉及跨事实组合、returns、outer join、anti join、复杂 set。**

| SQL   | 主要事实域 | 关键 SQL 特征                                                | 分类原因                                                             |
| ----- | ----- | -------------------------------------------------------- | ---------------------------------------------------------------- |
| `q01` | SS    | 单表 `store_sales` 过滤                                      | 只有 SS 单事实表和简单日期键过滤，属于最基础的单事实域查询。                                 |
| `q02` | SS    | SS + `item` inner join                                   | 单销售事实表加商品维表过滤，无聚合、无 CTE、无复杂算子，属于基础 join-filter。                  |
| `q3`  | SS    | SS + `date_dim` + `item`，按 year/brand 聚合                 | 单 SS 星型聚合，join 图谱稳定，只有简单 `SUM + GROUP BY`。                       |
| `q7`  | SS    | SS + demo/date/item/promotion，多个 `AVG`                   | 单 SS 事实域，虽然维表较多，但仍是稳定星型 join + 简单聚合。                             |
| `q13` | SS    | SS + store/demo/household/address/date，`AVG/SUM`         | 单 SS 宽维度星型聚合，无 CTE、无 set、无 outer join，属于基础事实域查询。                 |
| `q15` | CS    | CS + customer/address/date，按 ZIP 聚合                      | 单 CS 事实域，维表过滤和简单 `SUM` 聚合，结构稳定。                                  |
| `q19` | SS    | SS + date/item/customer/address/store，按 item/manufact 聚合 | 单 SS 事实域，典型商品维度销售聚合，无复杂控制流。                                      |
| `q21` | INV   | INV + warehouse/item/date，before/after 条件聚合              | 单库存事实域；ratio 是外层过滤，底层仍是稳定的库存快照聚合，因此放入基础事实域批。                     |
| `q22` | INV   | INV + date/item/warehouse，`AVG` + `ROLLUP`               | 单库存事实域；`ROLLUP` 只作用在 item 层级上，没有多事实组合或 outer/set 语义，因此作为库存基础域查询。 |
| `q26` | CS    | CS + demo/date/item/promotion，多个 `AVG`                   | 单 CS 事实域，结构与 `q7` 类似，是 catalog sales 的基础星型聚合。                    |
| `q42` | SS    | SS + date/item，按 item category/year 聚合                   | 单 SS 事实域，简单商品维度销售汇总。                                             |
| `q43` | SS    | SS + date/store，按星期做 `CASE SUM`                          | 单 SS 事实域，`CASE` 只是指标分桶，不改变基础星型结构。                                |
| `q48` | SS    | SS + store/demo/address/date，复杂布尔过滤                      | 虽然过滤条件较复杂，但仍是单 SS 事实域，没有 CTE、set、outer join 或跨事实组合。              |
| `q52` | SS    | SS + date/item，按 brand/year 聚合                           | 单 SS 事实域，简单商品品牌销售汇总。                                             |
| `q55` | SS    | SS + date/item，按 brand 聚合                                | 单 SS 事实域，固定月份、固定 manager 的简单品牌销售聚合。                              |
| `q62` | WS    | WS + date/ship_mode/warehouse/web_site，配送时延分桶            | 单 WS 事实域，`CASE` 只是 shipping delay 分桶，属于基础 case aggregate。        |
| `q96` | SS    | SS + household/time/store，`COUNT`                        | 单 SS 事实域，时间和门店过滤下的简单计数。                                          |
| `q99` | CS    | CS + date/ship_mode/warehouse/call_center，配送时延分桶         | 单 CS 事实域，结构与 `q62` 类似，属于基础 case aggregate。                       |

---

# Batch-2：同事实域残差扩展批

分类规则：**仍然是单事实域或单渠道事实域，但比 Batch-1 更复杂；常见特征是 CTE、HAVING、semi-subquery、标量阈值、派生 wrapper、多桶统计、单域 window/ratio/rollup、复杂过滤。**

| SQL    | 主要事实域 | 关键 SQL 特征                                        | 分类原因                                                                       |
| ------ | ----- | ------------------------------------------------ | -------------------------------------------------------------------------- |
| `q6`   | SS    | 标量子查询、`HAVING`、价格阈值                              | 主体仍是 SS 单事实域，但有月份标量子查询、同类商品均价阈值和 `HAVING`，属于同域扩展。                          |
| `q8`   | SS    | SS + 维度子查询 `INTERSECT` + `HAVING`                | 主查询事实域仍是 SS；`INTERSECT` 发生在地址 ZIP 维度过滤子查询中，不是多事实集合组合，因此放同域扩展。              |
| `q9`   | SS    | 多个 SS 标量子查询、`CASE` 选择指标                          | 全部围绕 SS，不跨事实域；复杂点是多个 quantity bucket 的标量分支判断。                              |
| `q12`  | WS    | WS + item/date，收入占比 window                       | 单 WS 事实域；`OVER(PARTITION BY i_class)` 是单域外层 ratio 分析，因此放同域扩展。              |
| `q18`  | CS    | CS 宽维度聚合 + `ROLLUP`                              | 单 CS 事实域，`ROLLUP` 作用于 item/address 层级，不涉及多事实组合，属于同域扩展。                     |
| `q20`  | CS    | CS + item/date，收入占比 window                       | 与 `q12` 类似，但事实域是 CS；单域 window ratio，放 Batch-2。                             |
| `q27`  | SS    | SS + store/item/demo，`ROLLUP/GROUPING`           | 单 SS 事实域，复杂点是外层层级聚合，不涉及多事实或 set。                                           |
| `q28`  | SS    | 多个 SS bucket 子查询并列                               | 全部子查询都来自 SS，只是按 quantity/list_price/coupon/wholesale_cost 做多桶统计，属于单域多指标扩展。 |
| `q32`  | CS    | CS + item/date，折扣大于同 item 平均阈值                   | 单 CS 事实域，复杂点是相关标量子查询阈值。                                                    |
| `q34`  | SS    | SS 先按 ticket/customer 聚合，再关联 customer            | 单 SS 事实域，包含派生表 wrapper 和 ticket 级过滤，属于同域扩展。                                |
| `q39a` | INV   | INV + item/warehouse/date，CTE + `stddev/avg/cov` | 单库存事实域，但有 CTE、统计指标和月度自比较，复杂度高于基础库存查询。                                      |
| `q39b` | INV   | INV 统计 CTE + cov 过滤                              | 与 `q39a` 同属库存统计扩展，只是异常过滤条件更强。                                              |
| `q45`  | WS    | WS + customer/address/date/item，维度子查询过滤          | 单 WS 事实域，复杂点是 ZIP/item 维度过滤子查询，不跨事实域。                                      |
| `q46`  | SS    | SS 先按 ticket/customer/city 聚合，再外层过滤              | 单 SS 事实域，派生表 wrapper + 当前地址/购买地址比较，属于同域扩展。                                 |
| `q53`  | SS    | SS 月/季销售 + window 平均偏离度                          | 单 SS 事实域，复杂点是 `AVG(SUM(...)) OVER` 和偏离度过滤。                                 |
| `q59`  | SS    | CTE 周销售，当前年与去年周粒度比较                              | 单 SS 事实域，CTE 和自连接用于时间对比，属于同域扩展。                                            |
| `q63`  | SS    | SS 月销售 + window 平均偏离度                            | 与 `q53` 类似，单 SS 事实域的窗口平均与偏离度过滤。                                            |
| `q65`  | SS    | store-item revenue 子查询 + 店铺平均对比                  | 单 SS 事实域，复杂点是嵌套聚合和 store-item revenue 与店铺均值比较。                             |
| `q68`  | SS    | ticket/customer 聚合 wrapper + 地址比较                | 单 SS 事实域，派生表和复杂维度过滤，但无跨事实组合。                                               |
| `q73`  | SS    | ticket/customer count wrapper + household 条件     | 单 SS 事实域，先聚合 ticket，再按 count 区间筛选。                                         |
| `q79`  | SS    | ticket/customer/store city 聚合 wrapper            | 单 SS 事实域，派生表聚合后关联 customer，属于同域扩展。                                         |
| `q88`  | SS    | 多个时间段 count 子查询并列                                | 全部子查询都来自 SS + time/store/household，是单事实域多时间桶统计。                            |
| `q89`  | SS    | SS 月销售 + window 平均偏离度                            | 单 SS 事实域，复杂点是按 category/brand/store 的月销售偏离分析。                              |
| `q90`  | WS    | WS AM/PM 两个计数子查询做 ratio                          | 单 WS 事实域，复杂点是两个时间窗口指标的外层比例。                                                |
| `q92`  | WS    | WS 折扣大于同 item 平均阈值                               | 单 WS 事实域，相关子查询阈值，结构与 `q32` 对应。                                             |
| `q98`  | SS    | SS + item/date，收入占比 window                       | 单 SS 事实域，window ratio 是外层分析，不涉及跨事实或高风险集合。                                  |

---

# Batch-3：成对事实 / returns / inventory-sales 组合批

分类规则：**涉及两个事实域组合，或 returns 事实域相关查询；主要包括 sales + returns、sales + inventory、returns 单域扩展。没有明显 outer join、anti join、INTERSECT/EXCEPT 等高风险语义。**

| SQL    | 主要事实域        | 关键 SQL 特征                                                    | 分类原因                                                             |
| ------ | ------------ | ------------------------------------------------------------ | ---------------------------------------------------------------- |
| `q1`   | SR           | SR + date，CTE 统计退货额，再按 store/customer 过滤                     | 退货事实域查询；虽然只有 SR，但 returns 域在基础 sales/inventory 之后处理，因此归 Batch-3。 |
| `q17`  | SS + SR + CS | store sales、store returns、catalog sales 组合统计                 | 多事实组合，核心是销售、退货、后续 catalog sales 的关联统计，属于成对/多事实扩展。                |
| `q24a` | SS + SR      | store sales 与 store returns 通过 ticket/item 关联，CTE + HAVING   | 典型 sales + returns 组合，且没有 outer/anti/set，高度符合 Batch-3。           |
| `q24b` | SS + SR      | 与 `q24a` 同结构，不同 item color 条件                                | 同样是 store sales + store returns 的成对事实组合。                         |
| `q25`  | SS + SR + CS | store sales、store returns、catalog sales 利润/损失组合              | 多事实组合，以 sales-return 后续销售关联为核心，无 outer/set。                      |
| `q29`  | SS + SR + CS | store sales、store returns、catalog sales 数量组合                 | 与 `q25` 类似，但指标是 quantity，仍是成对/多事实组合。                             |
| `q30`  | WR           | web returns CTE，按 customer/state 计算退货额阈值                     | returns 单域扩展查询；复杂点是 CTE + 平均阈值，但事实域是 WR。                         |
| `q37`  | INV + CS     | inventory 与 catalog_sales 按 item 存在性/过滤组合                    | 典型 sales + inventory 组合，不是库存单域查询，因此放 Batch-3。                    |
| `q50`  | SS + SR      | store_sales 与 store_returns 按 ticket/item/customer 关联，退货时延分桶 | 成对事实组合，核心是销售后退货时延分析。                                             |
| `q81`  | CR           | catalog returns CTE，按 customer/state 计算退货额阈值                 | returns 单域扩展，结构与 `q30` 对应，但事实域是 CR。                              |
| `q82`  | INV + SS     | inventory 与 store_sales 按 item 存在性/过滤组合                      | 典型 sales + inventory 组合，放 Batch-3。                               |
| `q84`  | SR           | store_returns + customer/demo/addr/income 过滤                 | 单 returns 事实域查询，用退货事实连接客户画像，属于 returns 域扩展。                      |
| `q85`  | WS + WR      | web_sales 与 web_returns 按 order/item 关联                      | sales + returns 成对事实组合，且没有 outer/anti/set。                       |
| `q91`  | CR           | catalog_returns + call_center/customer/demo/address/date 聚合  | 单 returns 事实域聚合，按 call center 统计 return loss。                    |
| `q95`  | WS + WR      | web_sales 自关联找多仓订单，再要求存在 web_returns                         | web sales + web returns 组合，主要是订单存在性和退货关联，无 outer/anti/set。       |

---

# Batch-4：跨渠道 sales 统一残差批

分类规则：**涉及两个或三个销售渠道事实表 `store_sales`、`catalog_sales`、`web_sales`，核心语义是多渠道 sales 的统一、合并、对比、存在性判断；不混入 returns/inventory/outer join/anti join。**

| SQL    | 主要事实域                            | 关键 SQL 特征                                                | 分类原因                                                             |
| ------ | -------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------- |
| `q2`   | WS + CS                          | web/catalog sales `UNION ALL`，按周做日别销售对比                  | 跨两个销售渠道，核心是渠道 sales 合并后的时间对比。                                    |
| `q10`  | SS + WS + CS                     | customer 画像 + 多渠道 `EXISTS`                               | 通过 exists 判断客户在 store/web/catalog 渠道的销售存在性，属于跨渠道 sales presence。 |
| `q11`  | SS + WS                          | store/web year_total `UNION ALL`，客户年度对比                  | 跨两个销售渠道，核心是同一客户跨渠道年度销售对比。                                        |
| `q23a` | CS + WS，依赖 SS 选 frequent/best 子集 | 用 SS 定义高频商品/客户，再比较 CS/WS sales                           | 多销售渠道组合，核心仍是 sales 渠道对比，不涉及 returns/inventory。                   |
| `q23b` | CS + WS，依赖 SS 子集                 | 与 `q23a` 类似，输出 customer 维度聚合                             | 跨渠道 sales 对比，结构比 `q23a` 稍变但仍属于 Batch-4。                          |
| `q31`  | SS + WS                          | store/web 按 county/year/quarter 聚合对比                     | 跨两个销售渠道的地理和时间维度销售对比。                                             |
| `q33`  | SS + CS + WS                     | 三渠道分别聚合后 `UNION ALL`                                     | 典型三渠道 sales 统一汇总。                                                |
| `q35`  | SS + WS + CS                     | customer 画像 + 多渠道 `EXISTS`                               | 与 `q10` 类似，多渠道销售存在性判断，没有 anti/outer。                             |
| `q54`  | CS + WS 选客户，再用 SS 统计 revenue     | 三渠道共同参与 customer segmentation                            | 核心是跨渠道 sales 驱动的客户分层，不涉及 returns/inventory。                      |
| `q56`  | SS + CS + WS                     | 三渠道 item sales 聚合后 `UNION ALL`                           | 典型三渠道商品销售统一。                                                     |
| `q58`  | SS + CS + WS                     | 三渠道 item revenue CTE 互相比值/比较                             | 跨渠道 item-level sales 对齐和对比。                                      |
| `q60`  | SS + CS + WS                     | 三渠道 item/category sales 聚合后 `UNION ALL`                  | 三渠道销售统一汇总，过滤维度不同但事实语义一致。                                         |
| `q66`  | WS + CS                          | web/catalog shipping/warehouse/monthly sales `UNION ALL` | 跨两个非门店销售渠道，对齐仓库、ship_mode、月份指标。                                  |
| `q71`  | SS + CS + WS                     | 三渠道 sales `UNION ALL` 后按 brand/time 聚合                   | 三渠道统一后做商品和时间维度聚合。                                                |
| `q74`  | SS + WS                          | store/web year_total `UNION ALL`，客户年度增长对比                | 跨渠道 sales 的年度趋势对比。                                               |
| `q76`  | SS + WS + CS                     | 三渠道统一为 `channel/col_name` 后聚合                            | 明确的 all-channel sales schema 对齐查询，典型 Batch-4。                    |

---

# Batch-5：复杂消费 / 高风险语义批

分类规则：**包含 outer join、anti join、INTERSECT/EXCEPT、复杂 window/rank、复杂 rollup/cube/grouping、复杂 ratio、复杂多层 CTE、sales + returns + inventory 混合、多渠道 + returns 混合等高风险语义。只要这些高风险语义成为主查询重写难点，就优先归 Batch-5。**

| SQL    | 主要事实域                 | 关键 SQL 特征                                                                       | 分类原因                                                             |
| ------ | --------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `q4`   | SS + CS + WS          | 多渠道 year_total CTE、年度差异和比例比较                                                    | 不只是简单跨渠道统一，而是多层年度后聚合和客户级复杂对比，归复杂消费层。                             |
| `q5`   | SS/SR + CS/CR + WS/WR | 多渠道 sales/returns，`LEFT OUTER JOIN`，`ROLLUP`                                    | 同时有多渠道、多退货、outer join 和 rollup，高风险语义明显。                          |
| `q14a` | SS + CS + WS          | `INTERSECT`、跨渠道商品集合、`ROLLUP/HAVING`                                             | 集合交集和跨渠道销售叠加，属于复杂 set + 后聚合查询。                                   |
| `q14b` | SS + CS + WS          | `INTERSECT`、跨渠道商品集合、复杂 CTE                                                      | 与 `q14a` 同类，核心是跨渠道集合交集，不适合归普通 Batch-4。                           |
| `q16`  | CS + CR               | `EXISTS` + `NOT EXISTS`                                                         | catalog_sales 查询中使用 anti-return/anti-order 语义，属于高风险 anti join。   |
| `q36`  | SS                    | `ROLLUP/GROUPING` + `rank() over` + gross margin                                | 虽是单 SS，但层级聚合和排名叠加，是复杂后聚合分析。                                      |
| `q38`  | SS + CS + WS          | 多个 `INTERSECT`                                                                  | 三渠道 customer/date 集合交集，是典型 set 语义查询。                             |
| `q40`  | CS + CR               | `LEFT OUTER JOIN` catalog_returns，before/after 指标                               | sales + returns 且保留未退货销售，outer join 语义使其进入 Batch-5。              |
| `q44`  | SS                    | `rank() over` 做 best/worst item 对比                                              | 单事实域但排名和双向 top/bottom 后处理是主要复杂点。                                 |
| `q47`  | SS                    | CTE + window avg + `rank()`，月销售偏离                                               | 单 SS 但复杂窗口、排名和偏离分析叠加，属于复杂消费。                                     |
| `q49`  | SS/SR + CS/CR + WS/WR | 三渠道 sales/returns，return/currency ratio，`rank()`                                | 多渠道 + returns + ratio + rank，明显高风险。                              |
| `q51`  | SS + WS               | 累计 window + `FULL OUTER JOIN`                                                   | 跨渠道累计销售对比，并使用 full outer join 保留两侧，归 Batch-5。                    |
| `q57`  | CS                    | CTE + window avg + `rank()`                                                     | 单 CS 但窗口、排名、月度偏离分析是核心语义。                                         |
| `q61`  | SS                    | promotion sales / total sales ratio，两个派生子查询                                     | 虽是单 SS，但核心是两个不同过滤子计划的后聚合比例，归复杂消费层。                               |
| `q64`  | CS/CR + SS/SR         | catalog sales/returns 与 store sales/returns 组合，复杂 CTE/HAVING                    | 多事实 sales-return 混合，且 CTE 和筛选逻辑复杂。                               |
| `q67`  | SS                    | `ROLLUP` + `rank() over`                                                        | 单 SS 但层级聚合和排名叠加，属于高风险后聚合。                                        |
| `q69`  | SS + CS + WS          | 多渠道 `EXISTS` / `NOT EXISTS`                                                     | 跨渠道 sales presence 叠加 anti-channel 条件，因此不是普通 Batch-4，而是 Batch-5。 |
| `q70`  | SS                    | `ROLLUP/GROUPING` + `rank() over`                                               | 单 SS，但地理层级聚合和排名叠加，属于复杂消费。                                        |
| `q72`  | CS + INV + CR         | catalog_sales + inventory + catalog_returns，`LEFT OUTER JOIN` promotion/returns | 同时涉及 sales、inventory、returns 和 outer join，是复杂多事实混合。              |
| `q75`  | SS/SR + CS/CR + WS/WR | all_sales CTE，三渠道 sales 扣减 returns，`LEFT JOIN`                                  | 多渠道 sales + returns 混合，并且通过 left join 保留未退货销售。                   |
| `q77`  | SS/SR + CS/CR + WS/WR | 三渠道 sales/returns CTE + `UNION ALL` + `ROLLUP`                                  | 多渠道、多退货、rollup 同时存在，属于复杂消费。                                      |
| `q78`  | SS/SR + CS/CR + WS/WR | 三渠道 sales left join returns，再多 CTE 关联                                           | 多渠道 sales-return 混合，且 left join 和跨渠道 customer/item 对齐复杂。         |
| `q80`  | SS/SR + CS/CR + WS/WR | 三渠道 sales/returns，`LEFT OUTER JOIN`，`ROLLUP`                                    | 与 `q5/q77` 同类，outer join + rollup + 多渠道 returns。                 |
| `q83`  | SR + CR + WR          | 三个 returns 渠道 CTE 后组合比较                                                         | 虽然都是 returns，但跨三类退货事实分支组合，不是简单 returns 单域查询。                     |
| `q86`  | WS                    | `ROLLUP/GROUPING` + `rank() over`                                               | 单 WS 但层级聚合和排名叠加，是复杂后聚合。                                          |
| `q87`  | SS + CS + WS          | 多个 `EXCEPT`                                                                     | 三渠道集合差集，典型 set difference 高风险语义。                                 |
| `q93`  | SS + SR               | `LEFT OUTER JOIN` store_returns，计算扣退货后的销售                                       | sales + returns 且保留未退货销售，outer join 是核心语义。                       |
| `q94`  | WS + WR               | `EXISTS` + `NOT EXISTS` web_returns                                             | web sales 中要求存在多仓订单且不存在对应 return，anti-return 语义明显。               |
| `q97`  | SS + CS               | store/catalog customer-item 集合，`FULL OUTER JOIN`                                | 两个销售渠道之间做 full outer 对齐，空值保留语义使其进入 Batch-5。                      |

---

# Other：fallback

分类规则：**无明确事实表主干，或主要是纯维表/纯标量/低事实语义查询，不强行归入事实域 batch。**

| SQL   | 主要事实域         | 关键 SQL 特征              | 分类原因                                                             |
| ----- | ------------- | ---------------------- | ---------------------------------------------------------------- |
| `q41` | 无事实表，仅 `item` | `item` 自相关子查询、复杂维度属性过滤 | 没有 sales/returns/inventory 等事实表主干，本质是商品维表查询，因此放入 Other/fallback。 |
