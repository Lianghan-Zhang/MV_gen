# TPC-DS JOIN SQL 样例


## 1.概览

在这批 SQL 里，单看整条 SQL 的表共现关系：

- `store_sales + date_dim` 出现在 **62** 条 SQL
- `store_sales + date_dim + item` 出现在 **36** 条 SQL
- `store_sales + date_dim + store` 出现在 **35** 条 SQL
- `store_sales + date_dim + store + item` 出现在 **20** 条 SQL

同时：

- 如果只看**最外层主查询**，`store_sales + date_dim + item` 这个 exact skeleton 直接出现在 **5** 条 SQL：`q3, q42, q52, q55, q98`
- 如果按**query block / scope** 来看，而不是整条 SQL，这个 exact skeleton 实际出现在 **11 个查询、14 个 block** 里

这说明：

> **Candidate 生成，最好以 query block 为单位，而不是整条 SQL。**

因为 TPC-DS 里很多可复用结构都藏在 CTE / 子查询里，只看 outer query 会漏掉不少复用机会。

---

## 最适合的 3 组 JOIN 样例

### 1）最推荐：`store_sales ⋈ date_dim ⋈ item`

**覆盖链**大致是：

`store_sales + date_dim` 62  
→ `+ item` 36  
→ exact outer skeleton 5（`q3, q42, q52, q55, q98`）  

→ 似乎很符合要求的一对样例：`q42` 和 `q52`

两者：

- join skeleton 完全相同：`date_dim + store_sales + item`
- filter 也完全相同：
  - `item.i_manager_id = 1`
  - `dt.d_moy = 11`
  - `dt.d_year = 2000`
- measure 也一致：都是 `SUM(ss_ext_sales_price)`

但 group by 不同：

- `q42`：按 `dt.d_year, item.i_category_id, item.i_category`
- `q52`：按 `dt.d_year, item.i_brand_id, item.i_brand`

即：

> 同一个父 MV family，不需要为每条 SQL 单独建 MV。  
> 可以先围绕 `store_sales ⋈ date_dim ⋈ item` 构造一个较细粒度的父 MV，再让不同 SQL 在其上做 residual filter 和 re-aggregation。

这对你的研究特别好，因为它证明了：

- join family 是共享的
- filter 可以一致
- group by 可以分叉
- 一个候选 MV 可以服务多个 sibling query

#### 这一 JOIN 关系还能继续扩展

- `q3`：同 skeleton，但 filter 变成 `item.i_manufact_id = 128`，按 brand 聚合
- `q55`：同 skeleton，filter 变成 `i_manager_id = 28, d_moy = 11, d_year = 1999`，按 brand 聚合
- `q98`：同 skeleton，但用了日期区间和 `i_category IN (...)`，外层还有 window ratio，属于更复杂但仍可关联的变体


### 2）第二组：`store_sales ⋈ date_dim ⋈ store`


**覆盖链**：

`store_sales + date_dim` 62  
→ `+ store` 35  
→ exact outer skeleton 3（`q8, q43, q70`）  
→ 如果按 query block 看，会到 **4 个查询 / 5 个 block**（还包括 `q77` 里的一个 CTE）

#### 代表 SQL

- `q43`
  - skeleton：`date_dim + store_sales + store`
  - filter：`s_gmt_offset = -5`, `d_year = 2000`
  - group by：`s_store_name, s_store_id`
  - 指标：按 weekday 拆分的 `sum(ss_sales_price)`

- `q70`
  - skeleton 相同
  - filter：`d_month_seq BETWEEN ...`
  - group by：`ROLLUP(s_state, s_county)`
  - 指标：`sum(ss_net_profit)` + rank window

- `q8`
  - skeleton 相同
  - 但带了 zipcode 的半连接过滤
  - group by：`s_store_name`

#### 这组的意义

这组说明同一个 `store_sales ⋈ date_dim ⋈ store` family，可以服务：

- 店级聚合
- 州/县级 rollup
- 带地理过滤的门店分析

---

### 3）复杂样例：`store_sales ⋈ store_returns ⋈ catalog_sales ⋈ date_dim×3 ⋈ store ⋈ item`

代表查询：

- `q17`
- `q25`
- `q29`

这 3 条 SQL 的 outer join skeleton 是一样的：

- `store_sales`
- `store_returns`
- `catalog_sales`
- `date_dim` 三次别名
- `store`
- `item`

#### 其中最适合做对子的是 `q25` 和 `q29`

两者：

- join skeleton 完全相同
- group by 也相同：
  - `i_item_id, i_item_desc, s_store_id, s_store_name`
- 变化主要在：
  - 时间窗口不同
  - 指标不同（`net_profit` / `quantity`）

这就很适合写成：

> 一个较粗粒度的多事实 family 被多条查询复用，而 query-specific difference 主要体现在时间过滤和聚合 measure 上。

#### 为什么这组先别当第一批

因为它有这些复杂性：

- 多 fact join
- 多个 date_dim alias
- 事实表间关联更复杂
- `q17` 里还有 `count/avg/stddev_samp`

尤其 `stddev_samp` 这类聚合，要想从 MV 上可重写，通常要把统计量改写成可分解形式（比如 count / sum / sumsq）。这很适合做你后续“高级重写能力”的一部分，但不适合当第一版实验入口。

---