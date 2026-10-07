# HW3 目标三指标口径与归档数据核对

本文说明 `src/target3/course_workload/run_workload.py` 如何采集请求明细并生成 `summary.json`，以及 `src/target3/make_main_table.py` 如何生成主表。目标三主负载为固定的 2,048 条测量请求；每轮另有 64 条顺序预热请求，预热不计入主表统计。五组配置的负载 SHA-256 均为 `f154ba954e28ca9612257edf5deb0d5dfc6bf27fd0115a701e1f49e52df75b03`，各组 `config.json` 中 `workload_verified=true`。

## 字段如何计算

- **请求成功率**：请求状态码为 200 且客户端未记录错误的请求数 ÷ 测量请求数。`failed` 是未满足该条件的请求数。`incomplete` 单独统计成功响应中实际生成 token 数不等于请求 token 数的请求；因此成功数与完整输出数是两个概念。本次五组均为 2,048/2,048 成功、0 失败、0 不完整。
- **吞吐（req/s）**：成功请求数 ÷ 测量阶段的实际墙钟耗时 `elapsed_s`。耗时从测量回放启动到所有测量任务结束；预热不计入。失败请求若有，会增加测量耗时但不进入成功数分子。`offered_rate_rps` 则是计划到达速率，按 `(测量请求数−1) / planned_duration_s` 计算，不是实际完成吞吐；本次计划速率约 69.30 req/s。
- **TTFT**：每个请求从客户端取得并发信号量、即将发出 HTTP 请求时，到客户端收到首个含输出 token 的 SSE 数据块之间的时间。它包含请求传输、Serve 排队/调度及首 token 等待，不包含计划到达前的等待和并发信号量前的客户端等待。
- **TPOT**：代码以首个和最后一个带非空 `output_ids` 的 SSE 数据块到达时间之差，除以 `实际输出 token 数−1`，作为单请求的平均 TPOT；输出不超过一个 token 或没有可用时间戳时不纳入 TPOT 分位数。它是每请求一个值，并非所有 token 间隔汇总的 p95；由于计时取自 SSE 数据块到达时间，不应解释为硬件层逐 token 计算时间。
- **端到端延迟**：同一客户端起点到完整响应结束的 `finished−started`，包含请求发出、服务端排队/推理和响应读完的时间；不含并发信号量之前的等待。
- **p50/p95/p99**：先从成功请求中筛选有该指标值的样本，升序排列后取 nearest-rank 样本，索引为 `ceil(q×样本数)−1`，不做线性插值。CSV 保留更高精度，主表字段按脚本设定取舍；PDF 报告再显示为两位小数。
- **缓存命中率**：成功请求中 SGLang `meta_info` 返回的 `cached_tokens` 总和 ÷ `prompt_tokens` 总和，是按 token 数加权的比率，不是逐请求命中率的简单平均。
- **实际 prefill tokens**：脚本字段 `computed_prefill_tokens_total` 的计算值，即成功请求的 `prompt_tokens_total−cached_tokens_total`。这是由服务端响应元数据推算的未命中 prompt token 数（prefill 工作量代理值），不是 GPU 计算时间或硬件计数器的直接测量。
- **后端请求分布**：只统计成功请求，依据 Replica 回写的 `X-SGLang-Backend` 响应头计数；它表示请求数分配，不等于各 GPU 的占用率、运行时负载或耗时分布。
- **dispatch_lag / client_queue**：前者为实际发出时刻减去计划到达时刻，包含客户端定时唤醒、构造 token 输入及信号量等待等时间；后者为客户端准备开始构造请求到取得信号量的时间，也包含本地构造和并发等待。因此二者不是纯服务端排队时间，也不能单独证明压测客户端绝无瓶颈。当前 README 中“毫秒级说明压测端不是瓶颈”应理解为本轮观测到的客户端调度等待较小，不应作为普遍因果结论。

具体实现见 [`run_workload.py`](src/target3/course_workload/run_workload.py#L85-L99) 的 nearest-rank 分位数、[`send_request`](src/target3/course_workload/run_workload.py#L188-L308) 的时间戳及字段采集、[`summarize`](src/target3/course_workload/run_workload.py#L353-L450) 的聚合公式，以及 [`make_main_table.py`](src/target3/make_main_table.py#L24-L49) 的表格映射。

## 归档结果核对

对 A、B1、B2、C、D 五组的 `requests.csv` 逐行重算后，以下字段均与各组 `summary.json` 对齐：测量请求数、成功/失败/不完整计数、prompt 与 cached token 总数、未命中 prompt token 总数、缓存命中率、TTFT/TPOT/端到端 p95，以及成功请求的后端分布。`throughput_rps` 与 `successful / elapsed_s` 一致；固定负载 hash 和 `workload_verified` 也一致，且五组 `validation_errors` 均为空。主表的成功数、prefill token、缓存命中率和 p95 与 summaries 在其显示精度内相符。

| 组 | 吞吐 (req/s) | 缓存命中率 | 未命中 prompt tokens | TTFT p95 (s) | 端到端 p95 (s) | 后端请求数（0/1/2/3） |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| A | 41.940 | 88.02% | 387,453 | 19.19 | 19.56 | 524 / 506 / 500 / 518 |
| B1 | 63.313 | 87.89% | 391,424 | 4.37 | 4.99 | 488 / 511 / 522 / 527 |
| B2 | 50.686 | 87.83% | 393,597 | 10.13 | 12.30 | 470 / 514 / 527 / 537 |
| C | 30.979 | 95.78% | 136,444 | 40.38 | 41.04 | 1,280 / 267 / 303 / 198 |
| D | 62.991 | 88.59% | 369,024 | 5.93 | 6.63 | 492 / 539 / 490 / 527 |

C 的归档结果显示更高的缓存命中率，同时后端 0 承接了 1,280/2,048 个成功请求；D 的归档后端请求数较均匀。这些是单轮的观察值，不能仅凭该表断言策略差异是结果变化的唯一原因。

## 解释边界

以上核对证明的是提交的配置、逐请求记录、汇总 JSON 和主表之间的算术一致性；它不独立证明这些记录确由 README 所写的四卡硬件和服务环境产生。本核查未在四卡环境重跑，每组也只有一轮，因此结果没有重复实验的方差或置信区间，细小差异不宜解释为稳定优势。

另外需要说明一处版本关系：组 D 的 `src/target3/routers.py` 在归档运行之后做过一次负载取值口径修正（`_load()` 改为取本地 in-flight 计数与副本上报值的较大者）。A、B1、B2、C、D 五组归档结果均由修正前的实现产生；该修正只影响「哪些副本会被判为热点、溢出往哪里走」的判定，不改变已归档逐请求记录与汇总数字的自洽性，但也意味着当前代码不能逐字节复现归档那一轮的路由决策序列。
