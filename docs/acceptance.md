# P0 验收清单

建议先关闭其他占内存的软件，启动平台，确认 `/api/health` 的 Ray、Redis 均正常。真实训练与短测试任务分开记录；每个场景结束后检查 `/api/cluster` 的资源账本。

## 需求与实现映射

| 笔试要求 | 实现位置 | 验收方式 |
|---|---|---|
| 提交、分页、详情、取消、资源、增量日志 | `backend/app/main.py` | Swagger、前端、`scripts/smoke.py` |
| CPU 资源足够执行，不足排队 | `scheduler.py` 的事务预留和调度 | 总配额2，提交两个各申请2核任务，第二个等待 |
| 五种状态、真实退出码 | scheduler + worker 的 `result.json` | 成功任务0退出；测试任务7退出判失败 |
| worker 心跳与僵死检测 | worker写Redis、scheduler核对 | `app.demo --sleep 120 --freeze-heartbeat 3` |
| 最大运行时间 | scheduler + worker双侧检查 | 运行40秒任务，timeout设3秒 |
| 每任务日志文件、offset增量 | worker输出文件、API有界读取 | 使用17字节分块拼接中文日志，不能重复/缺失 |
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
6. 检查训练任务的 `metrics.jsonl` 和 `artifacts/train/weights/`；如果只有1 epoch，图表只有一个数据点是正常情况。希望展示趋势可在资源允许时训练3 epoch。

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

Docker方案需确认API容器的StartedAt变化、Ray容器ID与StartedAt不变，训练进程身份在共享PID命名空间内保持。原生Windows脚本只作兼容性排查，其历史短测通过不能替代Docker最终验收。

## 前端手动检查

- 集群总览：已分配核数与实际CPU使用率分别标注；服务异常有明确提示。
- 提交：空名称、不合法CPU和超时显示校验；提交成功跳转任务列表。
- 列表：切换所有状态标签，按ID搜索，换页、换每页数量，筛选后回到第一页。
- 取消：pending与running都可取消；终态不可误操作；停止请求后等确认再显示终态。
- 详情：重新进入任务能显示历史日志，滚动不被强制抢回；开启自动滚动后跟随新日志；复制结果与所见一致。
- 切换不同任务时，不混入上一个任务的异步日志；日志文本中的 `<tag>` 以文本显示。
- 小窗口能阅读关键字段，按钮可通过键盘访问。

## 优先级加分项

先启动一个占满配额的阻塞任务，然后依次提交低优先级任务和高优先级任务。取消阻塞任务后，高优先级任务应先开始；同优先级按提交时间FIFO执行。本实现不抢占已经运行的任务。

## 记录规则

保存任务ID、时间、退出码、停止原因、资源回收结果；运行失败要保留失败记录与修复后的复测结果。`docs/verification.md` 只写实际执行的结果，未验证项标明“待验证”。
