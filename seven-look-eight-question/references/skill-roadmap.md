# Skill 拆分路线图

## 已确认的架构知识

1. 七看八问不再只维护一个大 skill，而是拆成 15 个独立 rule skills。
2. 每条规则都要有自己独立的 SKILL.md、脚本、实测记录和 review 闭环。
3. 15 个 rule skills 逐个实现、逐个排查、逐个迭代。
4. 等 15 个 rule skills 稳定后，再补一个统一入口 skill 负责编排调用。
5. 统一入口不应该先做；否则排查粒度会过粗，迭代成本过高。

## 当前进度清单

- [x] look-01：拆分为独立 skill look-01-profit-quality，并保留可独立执行脚本
- [x] look-02：拆分为独立 skill look-02-cost-structure，并保留可独立执行脚本
- [x] look-03：拆分为独立 skill look-03-growth-trend，并保留可独立执行脚本
- [x] look-04：拆分为独立 skill look-04-business-market-distribution，并保留可独立执行脚本
- [x] look-05：拆分为独立 skill look-05-balance-sheet-health，并保留可独立执行脚本
- [x] look-06：拆分为独立 skill look-06-input-output-efficiency，并保留可独立执行脚本
- [x] look-07：拆分为独立 skill look-07-roe-capital-return，并保留可独立执行脚本
- [ ] question-01：独立 skill
- [ ] question-02：独立 skill
- [ ] question-03：独立 skill
- [ ] question-04：独立 skill
- [ ] question-05：独立 skill
- [ ] question-06：独立 skill
- [ ] question-07：独立 skill
- [ ] question-08：独立 skill
- [x] 统一入口 skill：`run_seven_looks.py` 已实现七看编排，支持 JSON/Markdown 输出、human-in-loop 汇总、质量评分和行动建议

## 单个 rule skill 的标准交付顺序

1. 明确规则业务定义
2. 输出 SQL 逻辑并 review 正确性
3. 固化为 Python 脚本
4. 对真实样本做实测
5. 做 code review
6. 更新注册状态与进度

## 当前建议的编排边界

- seven-look-eight-question：保留为总工作流/规划视角 skill
- look-01-profit-quality：规则1的独立执行 skill
- look-02-cost-structure：规则2的独立执行 skill
- look-03-growth-trend：规则3的独立执行 skill
- look-04-business-market-distribution：规则4的独立执行 skill
- 其余 14 条：按同样模式继续新增 sibling skills
- future entry skill：最后新增，专门负责按规则调用这些 sibling skills