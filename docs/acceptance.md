# P0 验收清单

建议先关闭其他占内存的软件，启动平台，确认 `/api/health` 的 Ray、Redis 均正常。真实训练与短测试任务分开记录；每个场景结束后检查 `/api/cluster` 的资源账本。

新版先用 `backend/.auth/api-token` 登录网页；除health外所有API需Bearer令牌，脚本自动读取本机令牌。Ray/Redis不直接发布宿主端口。默认CPU预算2核、任务内存预算2048 MiB；检查回收时两者都应归零。

## 需求与实现映射

| 笔试要求 | 实现位置 | 验收方式 |
|---|---|---|
| 提交、分页、详情、取消、资源、增量日志 | `backend/app/main.py` | Swagger、前端、`scripts/smoke.py` |
| CPU/内存资源足够执行，不足排队 | `scheduler.py` 的联合事务预留和调度 | 总配额2核/2048 MiB，任一维度不足都排队 |
| 五种状态、真实退出码 | scheduler + worker 的 `result.json` | 成功任务0退出；测试任务7退出判失败 |
| worker 心跳与僵死检测 | worker写Redis、scheduler核对 | `app.demo --sleep 120 --freeze-heartbeat 3` |
| 最大运行时间 | scheduler + worker双侧检查 | 运行40秒任务，timeout设3秒 |
| 每任务日志文件、offset增量 | worker输出文件、API有界读取 | 使用17字节分块拼接中文日志，不能重复/缺失 |
| 可选SSE实时日志（已实现） | main.py log_stream、frontend/src/sse.ts | Bearer Header、完整事件字节游标、心跳、断线续读与end前尾排空 |
| 后端重启不丢任务和资源 | SQLite WAL、固定submission ID、启动对账 | 训练中仅重启API，核对同一Ray Job接管 |
| 集群总览 | `/overview` | 资源卡片、节点表、利用率进度条 |
| 任务列表 | `/jobs` | 五种标签、筛选、ID搜索、分页、查看和取消 |
| 任务提交 | `/submit` | 默认CPU训练命令、参数校验、成功跳转 |
| 任务详情和终端日志 | `/jobs/:id` | 状态/时间/原因、日志滚动、暂停滚动、复制 |

## 自动化短任务验收

```powershell
backend\.venv\Scripts\python.exe scripts\smoke.py
backend\.venv\Scripts\python.exe scripts\concurrency.py
```

脚本要求平台空闲，会保留其生成的任务记录，便于面试官在页面查看。异常退出时会尝试取消本脚本创建的未完成任务，不会取消用户已有任务。运行结果默认写 `docs/smoke-results.json`，失败以非零退出并写入具体原因。

## 真实训练与排队

1. 总配额设为 2，前端提交默认命令、申请2核、最大运行时长1800秒。
2. 页面应从 pending 进入 running；详情持续显示真实 YOLO 训练输出。
3. 再提交同样申请2核的任务，观察 pending，CPU可用量为0。
4. 第一个训练完成，应显示 succeeded、退出码0；第二个自动启动。
5. 取消第二个任务并等到 cancelled，CPU可用量恢复2。
6. 检查Docker任务 `workspace/metrics.jsonl` 和 `workspace/artifacts/train/weights/`；1 epoch只有一个指标点正常。资源曲线来自每2秒采样，与epoch指标分别展示。

首次模型/数据集下载时间属于首次运行开销，应提前准备并明确记录，不承诺固定训练分钟数。

## 多任务并发

总配额2，同时提交三个各申请1核的短任务：

```text
python -m app.demo --sleep 15 --exit-code 0
```

观察两个 running、一个 pending；资源释放后第三个启动。此场景验证真实执行器与资源调度并发，真实 YOLO 用前一场景单独验证，避免8GB机器并发训练导致内存压力。

`scripts/concurrency.py` 自动执行此场景，等待前两个任务均实际启动并收到心跳后，才判定并发成立；持续记录资源分配不超过2，结果保存 `docs/concurrency-results.json`。

## API 重启恢复

1. 提交 `python -m app.demo --sleep 90 --exit-code 0`，申请2核。
2. 等待任务实际有日志，记下任务ID、`ray_submission_id`、详情时间和CPU占用。
3. 再提交一个申请2核的任务，确认它在排队。
4. 默认Docker方案执行 `scripts/start-docker.ps1 -ApiOnly`，只重启API容器；也可运行自动化 `backend\.venv\Scripts\python.exe scripts\recovery.py --docker`。
5. 原任务应保留同一ID和Ray提交ID，CPU占用仍为2；日志继续，不重复启动训练。
6. 原任务结束后排队任务自动执行；两个任务结束，资源恢复。
7. 已结束的任务再次重启API后仍可分页查询并查看日志。

此项只重启API，不停止Ray或删除Redis/SQLite数据。完整机器断电后的训练续训不在本题P0范围。

Docker方案需确认API容器StartedAt变化、Ray容器ID与StartedAt不变、可信worker的PID/创建时间及独立训练容器ID保持。训练PID不属于API/Ray共享命名空间，不能用数字PID跨容器核对或终止。原生Windows脚本只作排查。

## 前端手动检查

- 集群总览：已分配核数与实际CPU使用率分别标注；服务异常有明确提示。
- 提交：空名称、不合法CPU和超时显示校验；提交成功跳转任务列表。
- 列表：切换所有状态标签，按ID搜索，换页、换每页数量，筛选后回到第一页。
- 取消：pending与running都可取消；终态不可误操作；停止请求后等确认再显示终态。
- 详情：重新进入任务能显示历史日志，滚动不被强制抢回；开启自动滚动后跟随新日志；复制结果与所见一致。
- 日志连接：显示“SSE已连接/重连中/读取完成”；暂停只停滚动；切换任务、登出和卸载会终止旧流，不混入旧任务日志。
- 切换不同任务时，不混入上一个任务的异步日志；日志文本中的 `<tag>` 以文本显示。
- 小窗口能阅读关键字段，按钮可通过键盘访问。

## 优先级加分项

按 `priority+floor(等待秒数/60)` 的有效优先级排序，同分FIFO，不封顶、不抢占。启动2核阻塞任务，提交P0旧任务，等待61秒后提交P1新任务；资源释放后旧任务有效优先级至少P1，并按FIFO先执行。

回填：先运行1核任务，再排2核高优先级任务，随后提交1核小任务；小任务可利用空闲1核运行并显示“回填”。所有被绕过任务的bypass_count随实际回填预留累加，默认达3次后停止继续绕过。轮询或重启本身不增加次数；边界由test_scheduling覆盖。

## 资源隔离与鉴权新增验收

```powershell
backend\.venv\Scripts\python.exe scripts\isolation_fairness.py --aging
# 保留历史P0证据，输出本轮独立记录：
backend\.venv\Scripts\python.exe scripts\smoke.py --output docs/isolation-smoke-results.json
backend\.venv\Scripts\python.exe scripts\recovery.py --docker --output docs/isolation-recovery-results.json
```

新增脚本验证匿名401、operator会话、非root UID1000、无Docker socket/平台数据库/令牌环境、仅loopback网络、实际CPU/内存/PID限制、资源字节游标、64 MiB真实OOM失败137及回收、回填和真实老化。脚本保留退出容器证据；检查Engine的OOMKilled，不能仅用手动exit137冒充内存超限。

手动查看资源详情的CPU/内存工作集曲线；新任务内存限制默认1024 MiB，可调整至平台总量以内。取消、心跳超时、运行超时、OOM、正常退出后均需确认训练容器停止并释放双配额。Docker检查失败必须保留预留，不能当作空闲。

## SSE实时日志新增验收

```powershell
# 平台空闲、前端5173和后端运行时；脚本自动读取本机Bearer令牌，不将令牌放URL。
backend\.venv\Scripts\python.exe scripts\sse_smoke.py --restart-api --restart-delay 20
```

脚本走Vite代理，验证匿名401、未知任务404、游标越界416、非法Last-Event-ID 422均在HTTP200前返回；排队期间10秒心跳，pending到running保持同一流；完整尾日志先log再空content end；断线用Header字节位置续传，无重复/丢失。API有活跃流时只重启API，确认Ray与执行ID不变，最终CPU/内存都回收。Last-Event-ID最多32个非负ASCII数字且优先query offset。

浏览器补测：观察SSE状态、启动后重启API并自动续接，确认请求URL只有offset、不含凭据；暂停滚动仍有新日志，恢复/复制可用；取消任务后收到end并显示读取完成。主动离线/网络异常按1/2/4/8/10秒退避；30秒无任何字节则重连。416不自动归零，用户明确选择“从头重新读取”。详情/资源仍1.5秒轮询、指标约6秒；这不是把所有页面请求都改成SSE。

最终证据：[sse-results.json](sse-results.json) 四组通过、API重启5.64秒；[sse-browser-results.json](sse-browser-results.json) 两次连接恢复、5004字符与持久日志清理后完全一致，任务只启动一次。测试/构建见 [sse-unit-results.json](sse-unit-results.json)。首轮worker Engine超时导致任务失败，保留 [首轮报告](sse-results-first-run.json)，不能把日志内容一致误称整轮训练通过。

## 记录规则

保存任务ID、时间、退出码、停止原因、资源回收结果；运行失败要保留失败记录与修复后的复测结果。`docs/verification.md` 只写实际执行的结果，未验证项标明“待验证”。
