<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api, dateText, durationText, memoryText, session, terminal, waitText, type Job, type Metric, type ResourcePage, type ResourceUsage } from '../api'
import { followJobLogs, type LogStreamState } from '../sse'
import AppIcon from '../components/AppIcon.vue'
import MetricChart from '../components/MetricChart.vue'
import ResourceChart from '../components/ResourceChart.vue'
import StatusBadge from '../components/StatusBadge.vue'

const route = useRoute()
const job = ref<Job | null>(null)
const error = ref('')
const logError = ref('')
const metrics = ref<Metric[]>([])
const logs = ref('')
const displayLogs = computed(() => logs.value
  .replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, '')
  .replace(/\r\n?/g, '\n'))
const logContainer = ref<HTMLElement>()
const follow = ref(true)
const copied = ref(false)
const cancelling = ref(false)
const loading = ref(true)
const logsFinished = ref(false)
const truncated = ref(false)
const offset = ref(0)
const logState = ref<LogStreamState>({ phase: 'connecting' })
const streamLabels = { connecting: 'SSE连接中', connected: 'SSE已连接', reconnecting: 'SSE重连中', finished: '读取完成', error: '连接已停止' }
let logController: AbortController | undefined
const resourceItems = ref<ResourceUsage[]>([])
const resourceLatest = ref<ResourceUsage | null>(null)
const resourceOffset = ref(0)
const resourcesFinished = ref(false)
const resourceError = ref('')
const latestUsage = computed(() => {
  const snapshot = job.value?.resource_usage
  return snapshot && (!resourceLatest.value || snapshot.timestamp > resourceLatest.value.timestamp) ? snapshot : resourceLatest.value
})
let generation = 0
let busyGeneration = -1
let timer: number | undefined
let copyTimer: number | undefined
let round = 0
let disposed = false
const canCancel = computed(() => job.value && !terminal(job.value.status) && !job.value.cancel_requested && !job.value.stop_reason)

async function poll() {
  const current = generation
  if (busyGeneration === current || disposed) return
  busyGeneration = current
  const id = String(route.params.id)
  const getMetrics = round++ % 4 === 0
  try {
    const [detail, metricResult, resourceResult] = await Promise.allSettled([
      api<Job>(`/jobs/${id}`),
      getMetrics ? api<{ items: Metric[] }>(`/jobs/${id}/metrics`) : Promise.resolve(null),
      !resourcesFinished.value ? api<ResourcePage>(`/jobs/${id}/resources?after=${resourceOffset.value}&limit=300`) : Promise.resolve(null),
    ])
    if (disposed || generation !== current) return
    if (detail?.status === 'fulfilled') {
      job.value = detail.value as Job
      error.value = ''
    } else if (detail?.status === 'rejected') error.value = detail.reason instanceof Error ? detail.reason.message : '任务信息读取失败'
    if (metricResult.status === 'fulfilled' && metricResult.value) metrics.value = metricResult.value.items
    if (resourceResult.status === 'fulfilled') {
      const chunk = resourceResult.value
      if (chunk) {
        resourceOffset.value = chunk.next_offset
        resourceItems.value = [...resourceItems.value, ...chunk.items].slice(-600)
        const latest = chunk.latest ?? chunk.items.at(-1) ?? null
        if (latest && (!resourceLatest.value || latest.timestamp >= resourceLatest.value.timestamp)) resourceLatest.value = latest
        resourcesFinished.value = chunk.finished && chunk.eof
        resourceError.value = ''
      }
    } else {
      resourceError.value = resourceResult.reason instanceof Error ? resourceResult.reason.message : '资源采样读取失败'
    }
  } finally {
    if (!disposed && generation === current) loading.value = false
    if (busyGeneration === current) busyGeneration = -1
  }
}
function connectLogs(fromStart = false) {
  logController?.abort()
  if (fromStart) {
    logs.value = ''
    offset.value = 0
    truncated.value = false
  }
  logsFinished.value = false
  logError.value = ''
  const controller = new AbortController()
  logController = controller
  const current = generation
  const isCurrent = () => !disposed && generation === current && logController === controller && !controller.signal.aborted
  void followJobLogs(String(route.params.id), {
    signal: controller.signal,
    offset: offset.value,
    onChunk: async chunk => {
      if (!isCurrent()) return
      // ponytail: keep the most recent 500k raw characters; complete logs stay on disk.
      if (logs.value.length + chunk.content.length > 500000) truncated.value = true
      logs.value = (logs.value + chunk.content).slice(-500000)
      offset.value = chunk.next_offset
      if (follow.value && chunk.content) {
        await nextTick()
        if (isCurrent() && logContainer.value) logContainer.value.scrollTop = logContainer.value.scrollHeight
      }
    },
    onState: state => {
      if (!isCurrent()) return
      logState.value = state
      logError.value = state.message || ''
      logsFinished.value = state.phase === 'finished'
    },
  })
}
async function cancel() {
  if (!job.value) return
  try {
    await ElMessageBox.confirm(`确认取消任务“${job.value.name}”？运行中的训练将被终止。`, '取消任务', { type: 'warning', confirmButtonText: '确认取消任务', cancelButtonText: '继续训练' })
    cancelling.value = true
    const current = generation
    const result = await api<Job>(`/jobs/${job.value.id}/cancel`, { method: 'POST' })
    if (current === generation) job.value = result
    ElMessage.success('已提交取消请求，正在确认任务停止')
  } catch (e) {
    if (e !== 'cancel' && e !== 'close') ElMessage.error(e instanceof Error ? e.message : '取消失败')
  } finally { cancelling.value = false }
}
async function copyLogs() {
  try {
    await navigator.clipboard.writeText(displayLogs.value)
    copied.value = true
    window.clearTimeout(copyTimer)
    copyTimer = window.setTimeout(() => { copied.value = false }, 2000)
  } catch { ElMessage.error('复制失败，请在日志面板内选择文本后复制') }
}
async function toggleFollow() {
  follow.value = !follow.value
  if (follow.value) {
    await nextTick()
    if (logContainer.value) logContainer.value.scrollTop = logContainer.value.scrollHeight
  }
}
watch(() => route.params.id, () => {
  logController?.abort()
  ++generation
  job.value = null
  logs.value = ''
  metrics.value = []
  error.value = ''
  logError.value = ''
  offset.value = 0
  resourceItems.value = []
  resourceLatest.value = null
  resourceOffset.value = 0
  resourcesFinished.value = false
  resourceError.value = ''
  logsFinished.value = false
  truncated.value = false
  loading.value = true
  follow.value = true
  round = 0
  connectLogs()
  void poll()
}, { immediate: true, flush: 'sync' })
watch(() => session.authenticated, authenticated => { if (!authenticated) logController?.abort() }, { flush: 'sync' })
onMounted(async () => {
  timer = window.setInterval(poll, 1500)
  if (route.hash === '#logs') { await nextTick(); document.getElementById('logs')?.scrollIntoView({ behavior: 'smooth', block: 'start' }) }
})
onUnmounted(() => { disposed = true; ++generation; logController?.abort(); window.clearInterval(timer); window.clearTimeout(copyTimer) })
</script>

<template>
  <div class="view-page">
    <div class="page-heading detail-heading"><div><RouterLink to="/jobs" class="back-link"><AppIcon name="back" :size="16" />任务管理</RouterLink><h1>{{ job?.name || (loading ? '正在加载任务…' : '任务详情') }}</h1><p class="mono">{{ route.params.id }}</p></div><div class="detail-actions"><StatusBadge v-if="job" :status="job.status" :cancelling="!!job.cancel_requested || (!!job.stop_reason && !terminal(job.status))" /><el-button v-if="job && !terminal(job.status)" type="danger" plain :disabled="!canCancel" :loading="cancelling" @click="cancel">{{ canCancel ? '取消任务' : '正在停止' }}</el-button></div></div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon class="page-alert" />
    <section class="panel detail-panel" v-loading="loading"><div class="panel-heading"><div><h2>任务信息</h2><p>运行配置与生命周期记录</p></div><span class="subtle-badge">CPU TRAINING</span></div><template v-if="job"><div class="detail-grid"><div><span>CPU 配额</span><strong>{{ job.cpu }} <small>核</small></strong></div><div><span>内存限制</span><strong>{{ memoryText(job.memory_mb) }}</strong></div><div><span>运行时长</span><strong>{{ durationText(job) }}</strong></div><div><span>最大运行时长</span><strong>{{ job.timeout_seconds }} <small>秒</small></strong></div><div><span>初始 / 有效优先级</span><strong>P{{ job.priority }} / P{{ job.effective_priority ?? job.priority }}</strong></div><div><span>排队等待</span><strong>{{ waitText(job.queue_wait_seconds) }}</strong><span v-if="job.backfilled" class="backfill-badge detail-backfill">回填执行</span></div><div><span>提交时间</span><strong>{{ dateText(job.created_at) }}</strong></div><div><span>开始时间</span><strong>{{ dateText(job.started_at) }}</strong></div><div><span>结束时间</span><strong>{{ dateText(job.finished_at) }}</strong></div><div><span>最近心跳</span><strong>{{ dateText(job.heartbeat_at) }}</strong></div></div><div class="command-display"><span>执行命令</span><code>{{ job.command }}</code></div><div class="execution-meta"><span>退出码 <strong>{{ job.exit_code ?? '—' }}</strong></span><span>Ray 提交 ID <code>{{ job.ray_submission_id || '等待提交' }}</code></span></div><el-alert v-if="job.error" :title="job.error" :type="job.status === 'cancelled' ? 'info' : 'error'" :closable="false" show-icon class="job-error" /></template></section>
    <ResourceChart v-if="job" :items="resourceItems" :latest="latestUsage" :cpu-limit="job.cpu" :memory-limit="job.memory_mb" :finished="resourcesFinished" :error="resourceError" />
    <section id="logs" class="terminal-panel">
      <div class="terminal-header"><div class="terminal-title"><AppIcon name="terminal" :size="19" /><h2>训练日志</h2><span :class="['log-live', { ended: logsFinished || logState.phase === 'error' }]" role="status">{{ streamLabels[logState.phase] }}</span></div><div class="terminal-tools"><button v-if="!logsFinished" type="button" @click="connectLogs()"><AppIcon name="refresh" :size="15" />重连</button><button type="button" :aria-pressed="follow" @click="toggleFollow"><AppIcon :name="follow ? 'pause' : 'play'" :size="15" />{{ follow ? '暂停滚动' : '继续滚动' }}</button><button type="button" :disabled="!displayLogs" @click="copyLogs"><AppIcon :name="copied ? 'check' : 'copy'" :size="15" />{{ copied ? '已复制' : '复制日志' }}</button></div></div>
      <div v-if="logError" class="terminal-error" role="alert">{{ logError }}<span v-if="logState.retryInMs">；{{ logState.retryInMs / 1000 }} 秒后重试。</span><span v-else-if="logState.phase === 'error'">（{{ logState.code }}）已停止自动重连。</span><button v-if="logState.phase === 'error' && ['log_truncated', 'http_416', 'stream_protocol'].includes(logState.code || '')" type="button" class="log-recovery-button" @click="connectLogs(true)">从头重新读取</button></div>
      <div v-if="truncated" class="terminal-notice">面板保留最近 500,000 个字符；完整日志仍保存在服务器。复制仅包含当前显示内容。</div><div ref="logContainer" class="terminal-body" tabindex="0" aria-label="训练日志内容"><pre v-if="displayLogs">{{ displayLogs }}</pre><div v-else class="terminal-empty"><span class="terminal-prompt">›</span>{{ logsFinished ? '任务已结束，没有日志输出。' : '等待训练日志输出…' }}<span v-if="!logsFinished && logState.phase !== 'error'" class="terminal-cursor" /></div></div>
      <div class="terminal-footer"><span><i :class="['service-dot', { healthy: logState.phase === 'connected' }]" />{{ logsFinished ? '历史日志已加载' : 'SSE 实时日志 · 字节游标断线接续' }}</span><span>{{ offset.toLocaleString() }} bytes 已读取<span v-if="!follow" class="paused-label"> · 自动滚动已暂停</span></span></div>
    </section>
    <MetricChart :items="metrics" />
  </div>
</template>
