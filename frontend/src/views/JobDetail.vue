<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api, dateText, durationText, terminal, type Job, type LogChunk, type Metric } from '../api'
import AppIcon from '../components/AppIcon.vue'
import MetricChart from '../components/MetricChart.vue'
import StatusBadge from '../components/StatusBadge.vue'

const route = useRoute()
const job = ref<Job | null>(null)
const error = ref('')
const logError = ref('')
const metrics = ref<Metric[]>([])
const logs = ref('')
const logContainer = ref<HTMLElement>()
const follow = ref(true)
const copied = ref(false)
const cancelling = ref(false)
const loading = ref(true)
const logsFinished = ref(false)
const truncated = ref(false)
const offset = ref(0)
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
  const requests = [api<Job>(`/jobs/${id}`)] as Promise<unknown>[]
  if (!logsFinished.value) requests.push(api<LogChunk>(`/jobs/${id}/logs?offset=${offset.value}&limit=65536`))
  if (getMetrics) requests.push(api<{ items: Metric[] }>(`/jobs/${id}/metrics`))
  const hadLogsRequest = !logsFinished.value
  try {
    const results = await Promise.allSettled(requests)
    if (disposed || generation !== current) return
    const detail = results[0]
    if (detail?.status === 'fulfilled') {
      job.value = detail.value as Job
      error.value = ''
    } else if (detail?.status === 'rejected') error.value = detail.reason instanceof Error ? detail.reason.message : '任务信息读取失败'
    if (hadLogsRequest) {
      const result = results[1]
      if (result?.status === 'fulfilled') {
        const chunk = result.value as LogChunk
        offset.value = chunk.next_offset
        // ponytail: keep the most recent 500k characters in the browser; persistent logs stay on disk.
        if (logs.value.length + chunk.content.length > 500000) truncated.value = true
        logs.value = (logs.value + chunk.content).slice(-500000)
        logsFinished.value = chunk.finished && chunk.eof
        logError.value = ''
        if (follow.value && chunk.content) {
          await nextTick()
          if (generation === current && logContainer.value) logContainer.value.scrollTop = logContainer.value.scrollHeight
        }
      } else if (result?.status === 'rejected') {
        logError.value = result.reason instanceof Error ? result.reason.message : '日志读取失败'
      }
    }
    if (getMetrics && generation === current) {
      const result = results.at(-1)
      if (result?.status === 'fulfilled') metrics.value = (result.value as { items: Metric[] }).items
    }
  } finally {
    if (!disposed && generation === current) loading.value = false
    if (busyGeneration === current) busyGeneration = -1
  }
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
    await navigator.clipboard.writeText(logs.value)
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
  ++generation
  job.value = null
  logs.value = ''
  metrics.value = []
  error.value = ''
  logError.value = ''
  offset.value = 0
  logsFinished.value = false
  truncated.value = false
  loading.value = true
  follow.value = true
  round = 0
  void poll()
}, { immediate: true })
onMounted(async () => {
  timer = window.setInterval(poll, 1500)
  if (route.hash === '#logs') { await nextTick(); document.getElementById('logs')?.scrollIntoView({ behavior: 'smooth', block: 'start' }) }
})
onUnmounted(() => { disposed = true; ++generation; window.clearInterval(timer); window.clearTimeout(copyTimer) })
</script>

<template>
  <div class="view-page">
    <div class="page-heading detail-heading"><div><RouterLink to="/jobs" class="back-link"><AppIcon name="back" :size="16" />任务管理</RouterLink><h1>{{ job?.name || (loading ? '正在加载任务…' : '任务详情') }}</h1><p class="mono">{{ route.params.id }}</p></div><div class="detail-actions"><StatusBadge v-if="job" :status="job.status" :cancelling="!!job.cancel_requested || (!!job.stop_reason && !terminal(job.status))" /><el-button v-if="job && !terminal(job.status)" type="danger" plain :disabled="!canCancel" :loading="cancelling" @click="cancel">{{ canCancel ? '取消任务' : '正在停止' }}</el-button></div></div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon class="page-alert" />
    <section class="panel detail-panel" v-loading="loading"><div class="panel-heading"><div><h2>任务信息</h2><p>运行配置与生命周期记录</p></div><span class="subtle-badge">CPU TRAINING</span></div><template v-if="job"><div class="detail-grid"><div><span>CPU 配额</span><strong>{{ job.cpu }} <small>核</small></strong></div><div><span>运行时长</span><strong>{{ durationText(job) }}</strong></div><div><span>最大运行时长</span><strong>{{ job.timeout_seconds }} <small>秒</small></strong></div><div><span>优先级</span><strong>P{{ job.priority }}</strong></div><div><span>提交时间</span><strong>{{ dateText(job.created_at) }}</strong></div><div><span>开始时间</span><strong>{{ dateText(job.started_at) }}</strong></div><div><span>结束时间</span><strong>{{ dateText(job.finished_at) }}</strong></div><div><span>最近心跳</span><strong>{{ dateText(job.heartbeat_at) }}</strong></div></div><div class="command-display"><span>执行命令</span><code>{{ job.command }}</code></div><div class="execution-meta"><span>退出码 <strong>{{ job.exit_code ?? '—' }}</strong></span><span>Ray 提交 ID <code>{{ job.ray_submission_id || '等待提交' }}</code></span></div><el-alert v-if="job.error" :title="job.error" :type="job.status === 'cancelled' ? 'info' : 'error'" :closable="false" show-icon class="job-error" /></template></section>
    <section id="logs" class="terminal-panel"><div class="terminal-header"><div class="terminal-title"><AppIcon name="terminal" :size="19" /><h2>训练日志</h2><span :class="['log-live', { ended: logsFinished }]">{{ logsFinished ? '读取完成' : '持续读取' }}</span></div><div class="terminal-tools"><button type="button" :aria-pressed="follow" @click="toggleFollow"><AppIcon :name="follow ? 'pause' : 'play'" :size="15" />{{ follow ? '暂停滚动' : '继续滚动' }}</button><button type="button" :disabled="!logs" @click="copyLogs"><AppIcon :name="copied ? 'check' : 'copy'" :size="15" />{{ copied ? '已复制' : '复制日志' }}</button></div></div><div v-if="logError" class="terminal-error">{{ logError }}，将自动重试。</div><div v-if="truncated" class="terminal-notice">面板保留最近 500,000 个字符；完整日志仍保存在服务器。复制仅包含当前显示内容。</div><div ref="logContainer" class="terminal-body" tabindex="0" aria-label="训练日志内容"><pre v-if="logs">{{ logs }}</pre><div v-else class="terminal-empty"><span class="terminal-prompt">›</span>{{ logsFinished ? '任务已结束，没有日志输出。' : '等待训练日志输出…' }}<span v-if="!logsFinished" class="terminal-cursor" /></div></div><div class="terminal-footer"><span><i :class="['service-dot', { healthy: !logsFinished && !logError }]" />{{ logsFinished ? '历史日志已加载' : '每 1.5 秒增量读取' }}</span><span>{{ offset.toLocaleString() }} bytes 已读取<span v-if="!follow" class="paused-label"> · 自动滚动已暂停</span></span></div></section>
    <MetricChart :items="metrics" />
  </div>
</template>
