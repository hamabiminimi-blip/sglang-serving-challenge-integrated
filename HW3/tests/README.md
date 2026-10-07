# HW3 路由定向测试

从仓库根目录运行：

```bash
python3 -m unittest discover -s HW3/tests -v
```

测试仅使用 Python 标准库，无需安装 pytest、Ray Serve、SGLang、模型或 GPU。套件覆盖：

- 四卡交付版 A/B/C/D 与路由类的配置映射；D 的稳定前缀亲和、热阈值边界、最冷副本溢出顺序、空/单副本候选及 in-flight 生命周期钩子。
- 多 Proxy 场景下，两个各自只有本地计数的路由器必须能从同一副本的 `routing_stats.in_flight` 看到合并负载，不能把局部非零计数当作全局计数。
- 单卡交叉验证版 D 的队列探测、fallback rank 顺序、阈值、未知主副本队列的亲和保留，以及多个独立路由器在相同当前队列快照下选择一致的溢出副本。
- A/B 的默认 P2C、C 的 strict consistent-hash、D 的自定义路由映射。内置路由器的实际算法仍由 Ray Serve 执行，单测仅检查本项目如何配置它们。

## 未在本机验证的场景

当前环境没有安装 Ray Serve（仓库要求 `ray[serve]==2.56.0`）、SGLang 或 GPU；因此测试用最小 Ray 类型桩隔离导入，只验证本项目自定义 D 路由器的决策逻辑，不声称替代 Ray 集成测试。以下情形未运行：

1. Ray Serve 实际 `PowerOfTwoChoices` 与 `ConsistentHashRouter` 的选择行为，以及 `RequestRouterConfig` 在指定 Ray 版本中的实例化兼容性。
2. 多个真实 HTTP Proxy 并发路由时，`record_routing_stats` 的采样/传播延迟、队列探测 RPC、状态陈旧窗口与并发竞态。单测仅模拟各 Proxy 可读取相同副本负载报告。
3. 四个 SGLang 后端或 1GPU/4GPU 实验下的真实请求、HTTP/SSE 转发、吞吐/延迟及压力溢出效果。

这些需要安装项目要求的 Ray/SGLang 依赖，并启动相应 Serve Proxy 与后端后进行集成或压力测试。
