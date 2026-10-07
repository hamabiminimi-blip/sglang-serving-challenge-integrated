# SGLang Serving Challenge 验收清单

依据群内提供的《第一关挑战》《第二关挑战》《第三关挑战》要求整理，供审查仓库和交付物时逐项勾验。第三关截止时间为 2026-10-07 22:00；第二关、第一关截止时间分别为 2026-10-03 22:00、2026-09-27 22:00。

## 第一关：单机 SGLang 在线推理

### 环境与运行
- [ ] 使用 SGLang 0.5.14、Ray 2.56.0、Qwen/Qwen3-0.6B。
- [ ] 本地启动 OpenAI-compatible 服务，`/v1/models` 可访问，并成功完成一次推理。
- [ ] 留存一次请求的运行截图。
- [ ] 从 Mooncake FAST’25 trace 采样 10–30 条请求，依据 input_length/output_length 构造合成 prompt，以 Poisson 到达过程生成请求时间并发送到 SGLang。
- [ ] 逐请求记录 input tokens、output tokens、status、TTFT 或 latency，并留存 workload 运行截图。

### 分析内容
- [ ] 提供一张说明 SGLang 在线推理请求路径的流程图。
- [ ] 不超过一页回答：RadixAttention/RadixCache 解决的问题；page-sized KV cache 与 prefix reuse 在 pipeline 中的位置；vLLM PagedAttention 的问题、与 SGLang RadixAttention/paged KV 的联系和差异。
- [ ] 简述第一次挑战的完成过程、困难与克服方式、科研工作启发。
- [ ] 记录 AI 模型、提示词、AI 如何帮助学习，以及是否被误导。
- [ ] 保留阅读文献笔记。

### 必须交付
- [ ] GitHub 公开仓库链接。
- [ ] `操作保存.pdf`：截图 1、2 合并在同一 PDF。
- [ ] `流程图.pdf`、`重点回答.pdf`、`作业感受.pdf`、`AI 使用说明情况.pdf`、`阅读文献笔记.pdf`。

## 第二关：前缀缓存测量与请求流程追踪

### 任务一：缓存对照实验
- [ ] 使用 SGLang 0.5.14、Ray 2.56.0、Qwen/Qwen3-0.6B，保持 Radix Cache 开启。
- [ ] 通过原生 `/generate` 接口发送 `input_ids`，使用流式响应；自行构造共享前缀与分散前缀两组负载，每组 32 条测量请求，最大并发数为 8。
- [ ] 固定 `temperature=0`、`max_new_tokens=16`、`ignore_eos=true`、`sampling_seed=2026`。
- [ ] 首次测量前先用短请求预热服务；每组测量前等待请求结束，调用 `POST /flush_cache` 并确认成功；共享前缀组按要求发送预热请求，预热不计入结果。
- [ ] 除前缀可否复用外，两组模型、输入/输出长度、请求顺序、并发数保持一致。
- [ ] 主表记录成功率、吞吐量、缓存命中率、实际执行 Prefill token 数，以及 TTFT、TPOT、端到端延迟的 p50/p95。
- [ ] 对比组全部请求成功且输入/输出长度一致；共享前缀组命中率更高、实际 Prefill token 更少。
- [ ] 从因果自注意力与因果掩码解释相同 token/位置的前缀为何可复用 K/V、命中后还需计算哪些 token，以及 TTFT 和 TPOT 为何变化不同。

### 任务二：追踪 SGLang 请求主流程
- [ ] 在 SGLang v0.5.14 源码中核对并追踪：`TokenizerManager.generate_request` → `Scheduler.event_loop_normal / waiting_queue` → `get_new_batch_prefill / match_prefix_for_req` → `RadixCache.match_prefix` → `PrefillAdder / ScheduleBatch` → model worker Prefill/Decode → `RadixCache.cache_finished_req / cache_unfinished_req` → TokenizerManager streaming output。
- [ ] 流程图覆盖请求接收、等待队列、前缀匹配、Prefill/Decode、缓存更新、流式返回，并标出关键函数所在文件。
- [ ] 不超过一页说明请求如何入队、前缀匹配如何减少本轮 Prefill token、新 KV 如何写回缓存、结果如何流式返回；流程图与说明合计不超过 2 页。
- [ ] 报告正文不超过 8 页，包含两个任务的实验、流程图和分析。
- [ ] 保存目标一逐请求结果与汇总，报告数据可对应到原始结果。
- [ ] 作业感受中的“最困难部分/如何克服”及“科研启发”两项禁止 AI 辅助撰写；另含 AI 使用说明。

### 必须交付
- [ ] GitHub 上上传 `HW2-姓名.zip`，解压后仅含同名根目录。
- [ ] 根目录含 `README.md`、`report.pdf`、`AI 使用说明情况（第二次挑战）.pdf`、`src/target1/`、`results/target1/shared_prefix/`、`results/target1/dispersed_prefix/`。
- [ ] README 写明 GPU/软件版本、安装、启动与回放命令、脚本用途，以及结果目录与报告表格对应关系。

## 第三关：比较并改进 Ray Serve 路由

### 拓扑与负载
- [ ] 在同一服务器启动四个同配置 SGLang 服务，建立 1 个 Ray head 与 4 个逻辑 worker；部署 4 个 Ray Serve Replica，每个在不同 worker 上且固定转发至一个独立 SGLang 后端。
- [ ] 所有实验经同一个 Ray Serve HTTP 入口回放，并保持模型、SGLang 参数、副本拓扑、压测端配置一致。
- [ ] 使用课程固定负载 `mooncake_prefix_workload_v2_seed2026.jsonl`；按材料中的命令校验，结果含 64 个 prefix family、64 条预热请求、2048 条测量请求。
- [ ] A–D 使用仓库发布的同一负载文件，不修改请求、到达时间或顺序；每轮开始前重新部署配置、清空四个后端缓存，再执行预热和测量。

### 四组路由与分析
- [ ] 完成 A 默认路由、B 参数候选对照并选定一个参数、C 前缀亲和、D 一个可运行的改进路由。
- [ ] D 只依赖当前请求与当前服务状态，不查看未来请求；描述使用的信号、规则、改善项和代价。
- [ ] 主表列出 A、两个 B 候选、C、D 的成功率、吞吐量、缓存命中率、实际 Prefill token 数、TTFT p95、端到端延迟 p95、四个 SGLang 后端请求分布。
- [ ] 用数据解释默认值与 B 选值差异、前缀亲和如何兼得缓存收益与热点偏斜、D 如何改进及付出何种代价；client_queue/dispatch_lag 用于确认压测端未成为主要瓶颈。
- [ ] 每轮 2048 条测量请求均成功且完整返回；若同一配置多轮，标清轮次并如实呈现波动。D 代码可复现，分析与数据相符；不要求所有指标最优。

### 必须交付
- [ ] GitHub 上上传 `HW3-姓名（或学号）.zip`，解压后仅含同名根目录。
- [ ] 根目录包含 `README.md`、`report.pdf`、`AI 使用说明情况（第三次挑战）.pdf`、`src/target1/`、`src/target3/`。
- [ ] 结果目录包含 `results/target1/shared_prefix/`、`results/target1/dispersed_prefix/`，以及 `results/target3/A_default/`、`B_candidates/`、`C_affinity/`、`D_improved/`；多轮可在组目录下建 `run-1/` 等子目录。
- [ ] README 写明 GPU/软件版本、安装、启动与回放命令、脚本用途、结果目录与报告表格的对应关系。
- [ ] `src/` 含负载脚本、Ray 集群与 Serve Deployment、D 路由代码、依赖及必要配置，并可按 README 运行。
- [ ] `results/` 保存每轮流量发生器输出的完整 CSV/JSON，报告数字可追溯至原始结果。
- [ ] 不提交模型权重、Python 环境、软件安装包或缓存目录。
- [ ] 作业感受中的“最困难部分/如何克服”及“科研启发”两项禁止 AI 辅助撰写；另含 AI 使用说明。
