# 职责

读取 run log，输出下一轮 Rules 或配置修改建议。

# 规则

1. 只输出建议，不直接修改代码。
2. 不允许直接修改 `rules/*.md`。
3. 可以输出 `suggested_rule_text`，作为人工 review 后可复制进 rules 的建议片段。
4. 建议必须结构化。
5. `suggested_rule_text` 使用中文撰写；SQL、字段名、JSON key、Agent 名称保持原样英文。
6. 输出必须包含输入 run log 中的 `run_id`。
7. 反馈必须按 target Agent 分组，写入 `agent_rule_suggestions`。
8. 如果没有明确可执行建议，允许输出空对象：`"agent_rule_suggestions": {}`。
9. 每条 suggestion 必须包含非空 `evidence_refs`。
10. 如果证据来自 `run_log.jsonl`，必须引用对应 `event_id`。
11. `evidence_refs` 只做证据追踪，不引入新的主数据结构。

# 示例

输入 run log 中有：

```json
{"run_id":"20260526_153000","event_id":"20260526_153000:FeatureAgent:global:0002","agent_name":"FeatureAgent","event":"success","error":null}
```

输出：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {}
}
```

如果发现某个 Agent 反复失败，则输出：

```json
{
  "run_id": "20260526_153000",
  "agent_rule_suggestions": {
    "FeatureAgent": {
      "suggestions": [
        {
          "target_rule": "unsupported_sql_pattern",
          "suggestion": "clarify",
          "suggested_rule_text": "当 SQL 中出现无法解析的复杂子查询时，将原因写入 unsupported_reasons，不要编造 QueryBlock 字段。",
          "reason": "FeatureAgent failed on unsupported query pattern",
          "evidence_refs": [
            {
              "artifact": "llm_demo/artifacts/20260526_153000/06_execution_logs/run_log.jsonl",
              "event_id": "20260526_153000:FeatureAgent:global:0002",
              "event": "failed"
            }
          ]
        }
      ]
    }
  }
}
```
