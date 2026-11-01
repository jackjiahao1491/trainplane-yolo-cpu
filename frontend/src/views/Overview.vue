<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { api, dateText, memoryText, terminal, type Cluster, type Job, type JobPage } from '../api'
import AppIcon from '../components/AppIcon.vue'
import StatusBadge from '../components/StatusBadge.vue'

const cluster = ref<Cluster | null>(null)
const jobs = ref<Job[]>([])
const error = ref('')
const loading = ref(true)
const updated = ref('')
let busy = false
let disposed = false
let timer: number | undefined
const percent = computed(() => cluster.value?.total_cpus ? Math.round(cluster.value.allocated_cpus / cluster.value.total_cpus * 100) : 0)
const memoryPercent = computed(() => cluster.value?.total_memory_mb ? Math.min(100, Math.round(cluster.value.allocated_memory_mb / cluster.value.total_memory_mb * 100)) : 0)
const resourceCards = computed(() => [
  { label: 'CPU 总配额', value: cluster.value?.total_cpus, unit: '核', icon: 'cpu', note: '用于任务调度的逻辑配额', color: 'teal' },
  { label: '已分配 CPU', value: cluster.value?.allocated_cpus, unit: '核', icon: 'play', note: '运行及启动中任务的资源预留', color: 'blue' },
  { label: '可用 CPU', value: cluster.value?.available_cpus, unit: '核', icon: 'grid', note: '可分配给新任务的资源', color: 'green' },
  { label: '排队任务', value: cluster.value?.pending_jobs, unit: '个', icon: 'clock', note: '资源就绪后自动开始执行', color: 'orange' },
])

async function load() {
  if (busy) return
  busy = true
  try {
    const [resources, recent] = await Promise.allSettled([
      api<Cluster>('/cluster'), api<JobPage>('/jobs?page=1&page_size=5'),
    ])
    if (disposed) return
    if (resources.status === 'fulfilled') {
      cluster.value = resources.value
      error.value = ''
      updated.value = new Date().toLocaleTimeString('zh-CN', { hour12: false })
    } else error.value = resources.reason instanceof Error ? resources.reason.message : '资源信息读取失败'
    if (recent.status === 'fulfilled') jobs.value = recent.value.items
    else if (!error.value) error.value = recent.reason instanceof Error ? recent.reason.message : '任务读取失败'
  } finally {
    if (!disposed) loading.value = false
    busy = false
  }
}
onMounted(() => { void load(); timer = window.setInterval(load, 3000) })
onUnmounted(() => { disposed = true; window.clearInterval(timer) })
</script>

<template>
  <div class="view-page">
    <div class="page-heading">
      <div><div class="eyebrow">CLUSTER OVERVIEW</div><h1>集群总览</h1><p>掌握资源分配，观察每一次训练的进展。</p></div>
      <RouterLink to="/submit" class="primary-link"><AppIcon name="plus" :size="18" />提交训练任务</RouterLink>
    </div>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon class="page-alert" />
    <section class="overview-banner">
      <div class="banner-copy"><span class="banner-label"><span class="tiny-light" />本地 CPU 工作空间</span><h2>从一次提交，到训练完成。</h2><p>任务自动排队，资源按需分配，训练过程清晰可见。</p><div class="banner-meta"><span>执行模式 <strong>CPU</strong></span><span>节点数量 <strong>{{ cluster?.nodes.length ?? '—' }}</strong></span><span>运行任务 <strong>{{ cluster?.running_jobs ?? '—' }}</strong></span></div></div>
      <div class="allocation-chart"><svg viewBox="0 0 120 120" aria-hidden="true"><circle cx="60" cy="60" r="49" class="ring-track" /><circle cx="60" cy="60" r="49" class="ring-value" :stroke-dasharray="`${percent * 3.079} 307.9`" /></svg><div><strong>{{ cluster ? percent : '—' }}<small v-if="cluster">%</small></strong><span>CPU 配额分配率</span></div></div>
    </section>
    <section class="stat-grid" aria-label="CPU 资源统计">
      <article v-for="card in resourceCards" :key="card.label" class="stat-card"><div class="stat-top"><span>{{ card.label }}</span><span :class="['stat-icon', card.color]"><AppIcon :name="card.icon" :size="19" /></span></div><div class="stat-number">{{ card.value ?? '—' }}<span>{{ card.unit }}</span></div><p>{{ card.note }}</p></article>
    </section>
    <section class="panel">
      <div class="panel-heading"><div><h2>节点资源</h2><p>资源分配与机器实际负载分别展示</p></div><div class="refresh-meta"><span v-if="updated">更新于 {{ updated }}</span><el-button text :loading="loading" aria-label="刷新资源" @click="load"><AppIcon name="refresh" :size="17" /></el-button></div></div>
      <div class="memory-quota-strip"><div><span>内存总配额</span><strong>{{ memoryText(cluster?.total_memory_mb) }}</strong></div><div><span>已分配内存</span><strong>{{ memoryText(cluster?.allocated_memory_mb) }}</strong></div><div><span>可用内存</span><strong>{{ memoryText(cluster?.available_memory_mb) }}</strong></div><div class="memory-quota-progress"><div class="progress-label"><span>内存配额分配率</span><span>{{ cluster ? memoryPercent : '—' }}%</span></div><el-progress :percentage="memoryPercent" :show-text="false" :stroke-width="6" /></div></div>
      <el-table :data="cluster?.nodes || []" v-loading="loading" empty-text="暂无节点信息">
        <el-table-column label="节点" min-width="210"><template #default="{ row }"><div class="node-cell"><span class="node-icon"><AppIcon name="cpu" :size="21" /></span><div><strong>{{ row.name }}</strong><small>{{ row.id }}</small></div></div></template></el-table-column>
        <el-table-column label="CPU 配额" min-width="180"><template #default="{ row }"><div class="progress-label"><span>{{ row.allocated_cpus }} / {{ row.total_cpus }} 核</span><span>{{ row.total_cpus ? Math.round(row.allocated_cpus / row.total_cpus * 100) : 0 }}%</span></div><el-progress :percentage="row.total_cpus ? Math.min(100, Math.round(row.allocated_cpus / row.total_cpus * 100)) : 0" :show-text="false" :stroke-width="6" /></template></el-table-column>
        <el-table-column label="实际 CPU 使用率" min-width="180"><template #default="{ row }"><div class="progress-label"><span>整机负载</span><span>{{ row.cpu_percent.toFixed(1) }}%</span></div><el-progress :percentage="Math.min(100, row.cpu_percent)" :show-text="false" color="#6c8cce" :stroke-width="6" /></template></el-table-column>
        <el-table-column label="内存使用率" min-width="180"><template #default="{ row }"><div class="progress-label"><span>整机内存</span><span>{{ row.memory_percent.toFixed(1) }}%</span></div><el-progress :percentage="Math.min(100, row.memory_percent)" :show-text="false" :color="row.memory_percent > 90 ? '#d7954c' : '#98a4b8'" :stroke-width="6" /></template></el-table-column>
      </el-table>
      <div class="service-status"><span><i :class="['service-dot', { healthy: cluster?.ray_ok }]" />Ray {{ cluster ? cluster.ray_ok ? '已连接' : '未连接' : '待检测' }}</span><span><i :class="['service-dot', { healthy: cluster?.redis_ok }]" />Redis {{ cluster ? cluster.redis_ok ? '已连接' : '未连接' : '待检测' }}</span><span class="muted">CPU 配额用于调度，不等同于机器实际使用率</span></div>
    </section>
    <div class="scheduling-note"><AppIcon name="clock" :size="18" /><div><strong>公平调度</strong><p>等待任务按有效优先级调度，老化间隔 {{ cluster?.aging_interval_seconds ?? '—' }} 秒；资源空余时允许回填，队首最多被越过 {{ cluster?.backfill_max_bypass ?? '—' }} 次。运行中的任务不被抢占。</p></div></div>
    <section class="panel recent-panel"><div class="panel-heading"><div><h2>最近任务 <span class="heading-count">RECENT ACTIVITY</span></h2><p>最近提交的 5 个训练任务</p></div><RouterLink to="/jobs" class="text-link">全部任务<AppIcon name="arrow" :size="16" /></RouterLink></div>
      <el-table :data="jobs" empty-text="还没有训练任务，提交第一个任务开始吧">
        <el-table-column label="任务名称" min-width="210"><template #default="{ row }"><RouterLink :to="`/jobs/${row.id}`" class="job-name">{{ row.name }}</RouterLink><span v-if="row.backfilled" class="backfill-badge">回填</span><div class="mono secondary-id">{{ row.id }}</div></template></el-table-column>
        <el-table-column label="状态" min-width="120"><template #default="{ row }"><StatusBadge :status="row.status" :cancelling="!!row.stop_reason && !terminal(row.status)" /></template></el-table-column>
        <el-table-column label="资源配额" min-width="135"><template #default="{ row }"><span>{{ row.cpu }} 核 CPU</span><div class="cell-secondary">{{ memoryText(row.memory_mb) }}</div></template></el-table-column>
        <el-table-column label="提交时间" min-width="160"><template #default="{ row }">{{ dateText(row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" width="90" align="right"><template #default="{ row }"><RouterLink :to="`/jobs/${row.id}`" class="text-link">详情<AppIcon name="arrow" :size="14" /></RouterLink></template></el-table-column>
      </el-table>
    </section>
  </div>
</template>
