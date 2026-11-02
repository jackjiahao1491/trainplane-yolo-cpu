# 本机验证记录

日期：2026-10-31～2026-11-02（Asia/Shanghai）。本文将设计、自动化检查和实测结果分开记录；未运行的项目不记为通过。

## 2026-11-02 SSE实时日志升级

本轮只部署更新API容器；Ray与Redis未重启。日志由前端带Bearer Header的fetch流读取，原offset GET保留；详情、指标与资源仍轮询。以下为本次实际执行结果，不用此前隔离/公平性或训练历史替代SSE验收。

| 检查 | 实际结果与证据 |
|---|---|
| Linux全量测试 | 48 passed、无skip、52.68秒，包含真实Docker集成；见 [sse-unit-results.json](sse-unit-results.json) |
| 前端测试与构建 | npm test 13 passed，生产构建成功19.01秒；同上报告 |
| Vite代理SSE四组 | 鉴权与预响应码、10秒空闲心跳及pending→running同流与尾部、Last-Event-ID无重/漏续传及API重启、游标错误验证均通过；见 [sse-results.json](sse-results.json) |
| 活跃流API重启 | 5.64秒；Ray和执行ID保持，按已消费字节续读；结束后CPU预留0、可用2，内存预留0、可用2048 MiB |
| 浏览器真实交互 | 任务cfde268bf3984f4f建立两条SSE，URL仅有offset0/1035无凭据；API重启自动重连、暂停/复制成功、取消后end；5004字符与持久日志去ANSI/CR后一致、demo仅启动一次。见 [sse-browser-results.json](sse-browser-results.json) |
| 截图 | [实时流](screenshots/sse-live.png)、[续传完成](screenshots/sse-finished.png) |

首轮 [sse-results-first-run.json](sse-results-first-run.json) 未通过：worker的Docker SDK读取Engine发生5秒ReadTimeout，导致短任务失败。后续对失败任务的历史SSE与持久日志诊断一致（170字符）。未修改worker或调度逻辑掩盖该失败；第二轮四组完整通过，失败记录与diagnosis保留。

SSE响应前验证401/404/416/422；开始200后发生读错误发送error JSON并关闭。服务器0.5秒tail、10秒注释心跳，尾部先log后空content end；客户端完整帧消费后才推进字节游标，30秒无字节看门狗和有上限退避。Uvicorn优雅退出时限5秒防活跃流无限拖住API重启。浏览器证据验证了该次网络/重启恢复，不将它夸大为无限保留、跨文件轮转或全局恰好一次保证。

## 2026-11-01 资源隔离与公平调度升级

本节为新增版本的独立实测，后文保留首次P0交付历史。默认执行链路现在为API→Ray可信worker→独立Docker训练容器，使用CPU与内存联合预留、Bearer门禁、容器硬限制、聚合采样、优先级老化与受限回填。

| 检查 | 实际结果与证据 |
|---|---|
| Linux完整测试 | 40 passed、0 skipped、59.80秒，开启RUN_DOCKER_TESTS=1；含原process兼容与真实隔离/OOM/worker采样。见 [isolation-unit-results.json](isolation-unit-results.json) |
| 新增HTTP场景 | 五组通过：鉴权、隔离+采样游标、64 MiB真OOM137、CPU回填+取消回收、真实默认60秒老化。见 [isolation-fairness-results.json](isolation-fairness-results.json) |
| P0接口回归 | 新容器执行路径九项全部通过，见 [isolation-smoke-results.json](isolation-smoke-results.json) |
| API重启恢复 | 55.282秒；API重启、Ray不变、worker与训练容器身份不变、日志启动一次、队列接续，双配额释放。见 [isolation-recovery-results.json](isolation-recovery-results.json) |
| 真实YOLO | 网页提交5c168822dd054af1，CPU2、1024 MiB硬限、1 epoch/320/batch1，147.179秒成功退出0；66条采样，原始内存峰值718.551 MiB；best/last各6491626字节。见 [isolation-yolo-results.json](isolation-yolo-results.json) |
| 前端 | npm test及生产构建通过；网页登录、默认训练表单/跳转、真实资源曲线、指标/日志及退出门禁通过。见 [isolation-browser-results.json](isolation-browser-results.json) |
| 最终恢复 | health=ok，CPU和内存预留均0；临停的其他15个应用容器全部恢复运行，原有5项健康检查全部healthy。见 [isolation-final-health.json](isolation-final-health.json) |

Docker测试容器需要 `--init` 回收孤儿进程；首次临时测试命令遗漏该参数，使两个原process进程树用例因僵尸进程失败。生产Compose与训练容器本已启用init，补齐测试命令后40项完整通过。原有Starlette/httpx弃用提示和前端大包警告仍在，不影响本轮功能验收。

独立容器不是虚拟机。API/Ray持Docker管理能力，默认不发布Ray/Redis宿主端口；任务没有socket、令牌或其他任务挂载。当前只有单operator令牌，没有多账号RBAC。退出的任务容器保留用于OOM/退出检查，磁盘与保留期治理属于后续优化。

本次训练也经用户授权临停15个无关应用释放8GB机器内存，完成后恢复；147秒与历史107秒不是受控性能对照。coco128的一轮loss/mAP用于证明功能链路，不作为泛化精度基准。

## 检测到的环境

| 项目 | 检测结果 |
|---|---|
| 系统 | Windows 11 家庭中文版，64位，10.0.22631 |
| CPU | Intel Core i5-8265U，4物理核/8逻辑处理器 |
| 内存 | 约7.8GiB总量，初次检查可用约0.6GiB；随前台应用变化 |
| 磁盘 | D盘初次检查剩余约152.7GiB |
| 工具 | Python 3.12.10、Node.js 24.18.0、Git、Docker Desktop、WSL2 |

此表表示环境检测，不代表训练性能基准。

宿主原生排查阶段的主要依赖：Ray 2.40.0、FastAPI 0.162.0、redis-py 8.1.0、PyTorch 2.9.1+cpu、torchvision 0.24.1+cpu、ultralytics 8.3.221、numpy 2.2.6、opencv-python 4.12.0.88、polars 1.34.0、pywin32 311。默认setup现只在Windows安装requests、psutil用于预缓存和验收，不要求在宿主运行训练栈。Redis服务端使用7.4镜像，和Python客户端版本分别管理。原生全量锁定见 `backend/requirements.lock.txt`，Docker核心版本见 `backend/requirements.txt`，已运行镜像的完整依赖快照见 `backend/requirements-linux.lock.txt`。

训练与Docker最终验收期间，经用户明确授权临时停止其他15个Docker应用容器，操作前已保存原运行状态，验收结束后恢复。下方耗时以该验收环境为前提，不能表述成“所有原有应用同时运行也能达到此性能”。

## 当前交付方案：Docker Linux后端

原生Windows短测虽曾通过，但多轮运行后再次出现Ray提交请求挂起，不能作为稳定可运行结论。当前改为Windows宿主前端、Docker Linux的Ray/API/YOLO/Redis；服务仍在本机CPU上运行，不使用GPU。以下为Docker方案的实际运行结果，原生阶段的历史记录另列。

| 项目 | 状态 | 证据/说明 |
|---|---|---|
| Docker镜像与服务启动 | 通过 | Ray/API/Redis已真实运行，网页提交的YOLO任务完成；最终线程修正版镜像已重新构建并启动 |
| Linux单元与真实worker检查 | 通过 | Linux容器执行，14 passed、无skip，用时12.98秒 |
| Docker HTTP P0短任务 | 通过 | 最终容器执行smoke.py，9项全过，含实际子进程取消、心跳和运行超时；见smoke-results.json |
| Docker真实并发与排队 | 通过 | 两个1核任务实际并发、第三个pending，自动接续且分配不超过2，全部成功回收；见concurrency-results.json |
| Docker API重启恢复 | 通过 | recovery.py --docker完整通过；API容器StartedAt变化，Ray容器不变，同一worker/训练PID及submission ID、日志仅启动一次，原任务和队列成功且资源归零；见recovery-results.json |
| 真实YOLO CPU+coco128 | 通过 | 最终线程修正版993c67aa8de849fa succeeded exit0，107.378161秒，4次线程检查均为2，无资产下载，权重与指标生成，CPU回收；见yolo-results.json |
| 前端构建/基础交互 | 通过基础检查和真实提交 | 已完成网页提交真实YOLO并成功运行，四页与指标截图位于screenshots/；浏览器交互检查见下方记录 |

首次Docker真实训练使用 `python -m app.train --epochs 1 --imgsz 320 --batch 1`，申请2核、超时1800秒。2026-11-01 16:48:21.341开始、16:54:18.190结束（Asia/Shanghai），用时356.849秒；指标box_loss=1.69865、cls_loss=3.83949、dfl_loss=1.49486、mAP50=0.38557、mAP50-95=0.26747，结束后CPU配额全部释放。证据见 `yolo-first-run.json`。这是训练闭环和指标采集验收，不是模型泛化精度基准。

首轮发现Ultralytics的设备选择会重设线程数，最终版本增加训练/验证回调恢复PyTorch配额，并使用本地数据集YAML的绝对路径避免重复下载。

最终复验任务 `993c67aa8de849fa`：2026-11-01 16:57:02.033开始、16:58:49.411结束（Asia/Shanghai），用时107.378161秒，退出码0。日志记录训练/验证共4次 `torch_threads=2 quota=2`；无Downloading；best.pt与last.pt各6491754字节。指标box_loss=1.69865、cls_loss=3.8395、dfl_loss=1.49486、mAP50=0.38557、mAP50-95=0.26747。结束后allocated=0、available=2，证据见 `yolo-results.json`。

最终Docker完整复测：smoke9项全过，运行取消任务 `c086ad3b5b4948cc`、心跳故障任务 `e07e9b7c3aaa4d3e`；并发任务 `f50a440c46d24426` 与 `b9c895fac0204967` 同时实际运行，`63b27704a1904c33` 排队后成功。恢复测试任务 `c4460e029fd146af` 在API容器重启后保持worker PID2218、训练PID2220与同一创建时间、同一submission ID；API容器StartedAt变化，Ray容器ID/StartedAt不变；日志启动计数1，队列任务 `cd695a3ff5d347dc` 随后成功。恢复脚本用时51.515秒，最终allocated=0、available=2、pending=0、running=0。

最终浏览器补测也通过：实际点击“取消任务”及确认对话框，任务 `1a417a5aecdc42ee` 进入cancelled、退出码130、CPU回收；分页切换至第2页显示不同任务，选择“已成功”后回到第1页且只显示成功任务；ID搜索匹配真实YOLO任务。日志复制、暂停滚动、中文及ANSI清理显示均已检查，见 `browser-results.json`。

结束检查已恢复原有15个应用容器，全部running，原有5个健康检查均healthy；训练平台仍为health=ok，CPU配额2、已分配0、可用2，排队与运行任务均为0。Ray容器CPU时间上限确认为2核，见 `final-health.json`。页面及后端保持运行供演示。

## 原生Windows阶段历史结果（不作为稳定验收）

| 项目 | 状态 | 证据/说明 |
|---|---|---|
| PowerShell脚本语法 | 通过 | 对start.ps1、stop.ps1使用PowerShell Parser解析，无语法错误 |
| 验收/停止脚本Python编译 | 通过 | `python -m py_compile scripts/smoke.py scripts/stop_owned.py` |
| 停止脚本进程身份保护 | 通过 | 创建独立测试子进程：伪造旧启动时间不会被停止，正确启动时间可停止；未操作已有应用 |
| 后端确定性检查 | 通过 | 原生阶段9 passed，覆盖状态与边界回归；实际Ray端到端另行验证 |
| Redis服务 | 通过 | 真实Redis容器，HTTP健康检查redis_ok=true |
| Windows Ray提交/停止 | 短测曾通过，多轮不稳定 | Ray2.40任务成功exit0；第三轮取消、超时、心跳检查曾通过，后续再次挂起 |
| HTTP P0短任务 | 历史短测通过 | 第三轮9项曾通过，见native-smoke-results.json；不代表Windows执行器长期稳定 |
| 两任务真实并发与排队 | 历史短测通过 | 两个1核任务均实际启动且有心跳，第三个pending；见native-concurrency-results.json |
| 原生YOLO CPU+coco128 | 未执行 | 原生Ray多轮不稳定，未用该路径验收；真实训练已在Docker完成 |
| API重启恢复 | 历史短测通过 | 任务209ef506805b4103重启前后身份不变，原任务/队列exit0；见native-recovery-results.json |
| 前端TypeScript与构建 | 通过 | npm run build的vue-tsc和Vite通过；Element Plus整包有体积提示，无构建错误 |
| 四页浏览器交互 | 基础检查通过 | 浏览器实测资源/节点、列表ID搜索、失败详情、中文日志、暂停/恢复滚动、复制、空名称校验；真实YOLO表单提交在Docker阶段完成 |

原生阶段的失败和不稳定现象保留如下，未与最终Docker验收混算。

## 兼容性排查

- Ray 2.59：本机隐藏后台启动Dashboard时，EventHead子进程出现C++ `bad fd (-1 vs -1)`，未将此版本记为运行通过。
- 原生阶段尝试Ray 2.40.0、ASCII临时目录、pywin32与旧GCS客户端兼容路径；这些措施曾帮助短测通过，但多轮后仍出现POST键值写入挂起。最终转向Docker Linux后端，不再声称原生方案稳定。只停止本项目持有的进程，不修改第三方Ray源码。
- 首轮HTTP验收暴露Ray SDK `stop_job` 无HTTP超时，导致心跳超时任务的训练进程虽已停止，调度线程却阻塞、业务状态未收尾。使用项目内 `BoundedJobClient` 设置连接/读取超时后，仅重启API，旧任务自动收尾为failed（exit15，worker心跳超时）并释放资源。第一轮失败记录保存在 `smoke-results-first-run.json`，修复后的完整复测另见最终 `smoke-results.json`。
- 第二轮：API调度未再次无限阻塞，但Ray `/api/jobs/` 提交路径连续读取超时，submission `08e39c3d9eb44d2e-1` 始终404，未实际创建。脚本取消该未启动业务任务后资源归零；未把这轮记为通过。记录保存在 `smoke-results-second-run.json`；后续第三轮完整复测通过。
- 最终修复给worker的Redis取消信号提供4秒优雅收尾宽限，仍存活再强杀；新Ray服务显式 `--num-gpus=0` 且设置 `CUDA_VISIBLE_DEVICES=-1`。第三轮完整smoke通过9项，包括真实子进程运行后取消、非零退出码7、运行超时、心跳超时、UTF-8增量日志、分页筛选和资源回收。训练取消任务ID `0b3115227aeb427f`，心跳故障任务ID `582adebf14cf4b27`。
- 前端浏览器检查发现并修复默认timeout的HTML步长校验冲突（min1与step60），重新TypeScript检查和Vite构建通过，浏览器控制台无运行错误。
- 首次自动恢复检查发现Windows PowerShell5在反序列化进程数组后保留包装信息，manifest出现 `{value:[...],Count:2}` 嵌套。已改为显式重建普通进程记录，并恢复原有Ray/Web/API归属；API实际停启后原任务仍在运行，但该轮验收因读取记录失败未记通过。
- 第二次恢复检查在2026-10-31 22:29提交，执行跨过主机长时间暂停，恢复操作继续时已是2026-11-01 16:18。原训练因超时收尾；此轮不作为正常短时API重启恢复证据，需在连续运行环境下重新验证。
- 恢复验收helper曾使用 `capture_output=True` 启动会后台创建API进程的PowerShell脚本；Windows继承的PIPE句柄导致runner等不到EOF，尽管API和原训练已经正常运行。已改为将辅助脚本输出重定向到 `.runtime/recovery-*.log` 文件，只等待PowerShell进程退出。
- 2026-11-01连续环境最终恢复验收通过：仅重启API，Ray和Redis保持；Ray提交ID与训练PID/创建时间保持一致，日志仅启动一次，原任务与排队任务自然成功，CPU预留归零。
