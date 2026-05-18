这份完整的实现方案结合了你针对 Spark SQL 方言、TPC-DS 工作负载、以及暂不考虑 Filter 和 Group By 的第一阶段研究目标。方案将贯穿 “环境与元数据准备 -> AST 预处理 -> 图构建 -> 频繁模式挖掘” 的全流程，并提供核心代码骨架。

--------------------------------------------------------------------------------
完整实现方案：基于 SQLGlot 与 NetworkX 的 TPC-DS Common Join 提取
第一阶段：环境与元数据准备
TPC-DS 包含雪花模型和复杂的查询拓扑，在进行 AST 分析前，必须为其构建准确的元数据字典，以便 SQLGlot 的优化器能够进行类型推断和列归属解析。
依赖安装：需要 sqlglot (支持 Spark 方言) 和 networkx。
定义 Schema：通过解析 TPC-DS 的 DDL（如 tpcds.sql），将其转换为 SQLGlot 可识别的 MappingSchema 格式。
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
import networkx as nx
from collections import defaultdict

# 1. 简化的 TPC-DS Schema 示例 (实际需包含所有表和列)
tpcds_schema = {
    "store_sales": {
        "ss_sold_date_sk": "INT",
        "ss_item_sk": "INT",
        "ss_customer_sk": "INT",
        "ss_net_profit": "DECIMAL"
    },
    "date_dim": {
        "d_date_sk": "INT",
        "d_year": "INT"
    },
    "item": {
        "i_item_sk": "INT",
        "i_category": "VARCHAR"
    }
    # ... 补充完整的 24 个表 [6, 7]
}
第二阶段：AST 预处理与标准化 (AST Normalization & Qualification)
由于 TPC-DS 大量使用 CTE (如 Query 64 中 CTE 互相嵌套)，为了提取底层的物理表连接，我们首先要内联 CTE (消除 CTE)，随后进行Qualify 标准化。
def preprocess_sql(sql_text):
    # 解析 Spark SQL [4]
    ast = sqlglot.parse_one(sql_text, dialect="spark")
    
    # 步骤 A: 消除 CTE，将逻辑展平，这对于 TPC-DS 极其重要
    ast = eliminate_ctes(ast)
    
    # 步骤 B: Qualify，补全表前缀、展开星号、标准化标识符 [1, 2]
    # 注意：设置 validate_qualify_columns=True 可确保所有列都被正确解析
    qualified_ast = qualify(ast, schema=tpcds_schema, dialect="spark")
    
    return qualified_ast
第三阶段：构建单查询物理连接图 (Join Graph Construction)
在这个阶段，我们将每一条重写后的 SQL 转换为无向多重图（MultiGraph）。节点为物理表名，边属性为连接键（Join Keys）。这样不仅抹平了 JOIN ... ON 和隐式 WHERE 连接的区别，还能正确表达自连接。
def build_join_graph(qualified_ast):
    join_graph = nx.MultiGraph()
    
    # 1. 构建 Alias 到物理表名的映射
    alias_to_physical = {}
    for table in qualified_ast.find_all(exp.Table):
        alias = table.alias_or_name
        physical_name = table.name
        alias_to_physical[alias] = physical_name
        # 确保所有物理表都作为节点加入
        join_graph.add_node(physical_name)

    # 2. 遍历所有的等值连接 (EQ 节点)
    for eq in qualified_ast.find_all(exp.EQ):
        left, right = eq.left, eq.right
        
        # 只提取列与列之间的比较
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            left_alias, right_alias = left.table, right.table
            
            # 判断是否为跨表的 Join 谓词
            if left_alias and right_alias and left_alias != right_alias:
                phys_left = alias_to_physical.get(left_alias)
                phys_right = alias_to_physical.get(right_alias)
                
                # 如果某个别名没找到对应的物理表（例如子查询未被完全展开），则跳过
                if not phys_left or not phys_right:
                    continue
                
                # 生成统一的边特征签名 (保证字典序，使得 A=B 和 B=A 一致)
                col1 = f"{phys_left}.{left.name}"
                col2 = f"{phys_right}.{right.name}"
                edge_signature = tuple(sorted([col1, col2]))
                
                # 将 Join 关系加入图 (多重图允许相同表之间有多条不同边)
                join_graph.add_edge(phys_left, phys_right, join_signature=edge_signature)
                
    return join_graph
第四阶段：跨查询 Common Join 挖掘 (MV Candidate Generation)
获得 TPC-DS 所有 99 个查询的图结构后，我们需要挖掘频繁的子图作为候选物化视图（MV Candidates）。对于第一阶段（仅提取 Join），可以通过枚举边组合或利用经典的图挖掘算法来实现。这里提供一个提取**频繁连接对（2-Table Joins）和三表雪花连接（3-Table Joins）**的统计思路：
from itertools import combinations

def mine_common_joins(all_graphs, min_frequency=5):
    # 用于统计 Join Subgraph 的出现频率
    join_patterns_counter = defaultdict(int)
    
    for graph in all_graphs:
        # --- 挖掘 2-Table Join (单边) ---
        edges = list(graph.edges(data=True))
        query_signatures = set() # 用 set 去重，防止同一条查询内重复统计
        
        for u, v, data in edges:
            sig = data['join_signature']
            query_signatures.add(sig)
            
        # --- 挖掘 3-Table Join (两条相连的边) ---
        # 比如：store_sales 连接 date_dim，同时 store_sales 连接 item
        for edge1, edge2 in combinations(edges, 2):
            # 检查两条边是否有共享的表节点 (u, v)
            nodes1 = set([edge1, edge1[10]])
            nodes2 = set([edge2, edge2[10]])
            if nodes1.intersection(nodes2): 
                # 生成组合签名并排序
                sig1 = edge1[11]['join_signature']
                sig2 = edge2[11]['join_signature']
                combined_sig = tuple(sorted([sig1, sig2]))
                query_signatures.add(combined_sig)
                
        # 累加当前查询发现的所有模式
        for sig in query_signatures:
            join_patterns_counter[sig] += 1
            
    # 过滤出大于阈值的 Candidate
    mv_candidates = {
        pattern: count 
        for pattern, count in join_patterns_counter.items() 
        if count >= min_frequency
    }
    
    # 按频率倒序排列输出
    return sorted(mv_candidates.items(), key=lambda x: x[10], reverse=True)

# 假设 tpcds_queries 包含 99 条 SQL 字符串
all_graphs = []
for sql in tpcds_queries:
    ast = preprocess_sql(sql)
    graph = build_join_graph(ast)
    all_graphs.append(graph)

# 获取 MV 候选
candidates = mine_common_joins(all_graphs, min_frequency=5)
for candidate, freq in candidates[:10]:
    print(f"Frequency: {freq} | Join Pattern: {candidate}")
方案优劣分析与实施建议
该方案的优势 (Pros)
精准去重与抗干扰：由于实施了 qualify 标准化和图论建模，你的系统将对别名变化、隐式/显式 JOIN 的语法差异、列顺序以及过滤条件完全免疫。
支持雪花模型与自连接：利用 nx.MultiGraph 搭配排序后的物理列元组（join_signature），可以完美处理 TPC-DS 中单事实表多次关联相同维度表（如 ship_date 与 sold_date）的棘手场景。
工程化极其便利：Python + SQLGlot + NetworkX 的生态完美契合，能够直接作为你博士论文系统原型的核心模块模块。
需要关注的难点 (Cons/Challenges)
复杂的子查询嵌套：部分 TPC-DS 查询 (如包含 EXISTS 或 IN 的复杂关联子查询) 在 AST 层可能不会直接体现为简单的 Join 节点。虽然 eliminate_ctes 能展开 CTE，但这并不能解决所有嵌套逻辑。后续可能需要使用 SQLGlot 进一步开发针对关联子查询提升 (Subquery Unnesting) 的规则。
雪花模型长路径爆炸：对于 3-Table 以上的子图，组合枚举（如上述代码的 combinations）可能会产生大量模式。若需扩展至挖掘 4 表、5 表以上的星型结构，建议引入 gSpan 等专业的频繁子图挖掘（FSM, Frequent Subgraph Mining）算法。
给博士生的下一步实施建议： 建议你先选取 TPC-DS 中的 Interactive 类查询 (如 Query 19, 42, 52, 55，通常是简单的星型连接) 验证上述流水线的正确性，打印出输出的图结构是否符合预期。验证无误后，再将系统推向包含复杂窗口函数和多事实表交叉的 Complex 类查询 (如 Query 23, 64) 进行压力测试。成功跑通这 99 个查询的 Common Join 之后，你这篇论文的第一个 Solid Contribution 就立住了！