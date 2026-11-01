<script setup lang="ts">
import { computed } from 'vue'
import { memoryText, type ResourceUsage } from '../api'

const props = defineProps<{ items: ResourceUsage[]; latest: ResourceUsage | null; cpuLimit: number; memoryLimit: number; finished: boolean; error: string }>()
const timeText = (timestamp: number) => new Date(timestamp * 1000).toLocaleTimeString('zh-CN', { hour12: false })
const plots = computed(() => {
  const items = props.items.filter(item => Number.isFinite(item.timestamp))
  const first = items[0]?.timestamp ?? 0
  const last = items.at(-1)?.timestamp ?? 0
  return [
    { key: 'cpu_percent' as const, label: '训练进程 CPU', unit: '%', color: '#158778', limit: props.cpuLimit * 100 },
    { key: 'memory_mb' as const, label: '容器内存工作集', unit: 'MiB', color: '#6c8cce', limit: props.memoryLimit },
  ].map(plot => {
    const values = items.filter(item => Number.isFinite(item[plot.key]))
    const max = Math.max(plot.limit, ...values.map(item => item[plot.key]), 1) * 1.12
    return {
      ...plot, max, first, last,
      limitY: 146 - plot.limit / max * 112,
      points: values.map(item => ({ x: last === first ? 230 : 48 + (item.timestamp - first) / (last - first) * 354, y: 146 - item[plot.key] / max * 112, value: item[plot.key], timestamp: item.timestamp })),
    }
  })
})
</script>

<template>
  <section class="panel resource-sampling-panel">
    <div class="panel-heading"><div><h2>进程资源采样</h2><p>采集任务容器内所有进程；内存为扣除可回收文件缓存的工作集，CPU 单核满载为 100%</p></div><span class="subtle-badge">{{ finished ? '历史采样' : '持续采样' }}</span></div>
    <el-alert v-if="error" :title="error" type="warning" :closable="false" show-icon class="resource-sampling-error" />
    <div class="resource-live-grid"><div><span>最新 CPU 使用率</span><strong>{{ latest ? latest.cpu_percent.toFixed(1) + '%' : '—' }}</strong><small>{{ cpuLimit }} 核配额，对应 {{ cpuLimit * 100 }}%</small></div><div><span>最新内存占用</span><strong>{{ memoryText(latest?.memory_mb) }}</strong><small>限制 {{ memoryText(latest?.memory_limit_mb ?? memoryLimit) }}</small></div><div><span>进程 / 线程数</span><strong>{{ latest?.pids ?? '—' }}</strong><small>容器内进程与线程</small></div><div><span>最近采样</span><strong>{{ latest ? timeText(latest.timestamp) : '—' }}</strong><small>{{ finished ? '任务结束，采样读取完成' : '等待并增量读取执行侧采样' }}</small></div></div>
    <div v-if="items.length" class="resource-chart-grid"><div v-for="plot in plots" :key="plot.key" class="resource-chart"><div class="resource-chart-heading"><span><i :style="{ background: plot.color }" />{{ plot.label }}</span><small>虚线：{{ plot.limit }} {{ plot.unit }} 配额</small></div><svg viewBox="0 0 440 185" role="img" :aria-label="`${plot.label}变化曲线`"><g v-for="index in [0, 1, 2]" :key="index"><line x1="48" :y1="34 + index * 56" x2="402" :y2="34 + index * 56" stroke="#e7ecee" /><text x="39" :y="38 + index * 56" text-anchor="end">{{ (plot.max * (2 - index) / 2).toFixed(0) }}</text></g><line x1="48" :y1="plot.limitY" x2="402" :y2="plot.limitY" stroke="#a8b7bd" stroke-dasharray="4 4" /><polyline :points="plot.points.map(point => `${point.x},${point.y}`).join(' ')" fill="none" :stroke="plot.color" stroke-width="2" stroke-linejoin="round" /><circle v-for="(point, index) in plot.points" :key="index" :cx="point.x" :cy="point.y" r="2.5" :fill="plot.color"><title>{{ timeText(point.timestamp) }} · {{ point.value.toFixed(1) }} {{ plot.unit }}</title></circle><text x="48" y="173">{{ timeText(plot.first) }}</text><text x="402" y="173" text-anchor="end">{{ timeText(plot.last) }}</text></svg></div></div>
    <div v-else class="metrics-empty">{{ finished ? '此任务没有可用的资源采样。' : '暂无资源采样，等待训练进程启动。' }}</div>
    <div class="resource-chart-foot">曲线保留最近 600 条采样；CPU 使用率可超过 100%，例如双核满载约为 200%。</div>
  </section>
</template>
