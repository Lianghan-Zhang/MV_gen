# QueryFamily Boundary Backup

备份时间：2026-06-13

备份目的：在按方案 A 取消 `QueryFamily` 作为 BatchMVAgent batch 内 MV 硬边界前，保存当前方案。

备份文件：`current_scheme.tgz`

包含范围：

- `llm_demo/src/agents/batch_mv_agent.py`
- `llm_demo/src/agents/batch_cluster_agent.py`
- `llm_demo/src/agents/executor_agent.py`
- `llm_demo/src/agents/family_agent.py`
- `llm_demo/src/core/batch_workflow_runner.py`
- `llm_demo/src/core/schemas.py`
- `llm_demo/src/core/family_candidate_builder.py`
- `llm_demo/rules/batch_mv_agent.md`
- `llm_demo/rules/batch_cluster_agent.md`
- `llm_demo/rules/family_agent.md`
- `llm_demo/rules/_prompt_template.md`
- `llm_demo/tests/test_agent_flow_q42_q52.py`
- `llm_demo/notebooks/etl_agent_flow_resume_prefail_artifacts.ipynb`
- `temp_decision.md`
- `suggestion.md`
