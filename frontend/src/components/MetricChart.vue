<script setup lang="ts">
import { computed, ref } from 'vue'
import type { Metric } from '../api'

const props = defineProps<{ items: Metric[] }>()
const mode = ref('loss')
const colors = ['#158778', '#6c8cce', '#d99a50']
const series = computed(() => mode.value === 'loss'
  ? [{ key: 'box_loss', label: 'box loss' }, { key: 'cls_loss', label: 'cls loss' }, { key: 'dfl_loss', label: 'dfl loss' }]
  : [{ key: 'map50', label: 'mAP50' }, { key: 'map50_95', label: 'mAP50–95' }])
const plotted = computed(() => {
  const values = props.items.flatMap(item => series.value.map(s => item[s.key as keyof Metric])).filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  const max = values.length ? Math.max(...values, 0.01) * 1.1 : 1
  const epochs = props.items.map(item => item.epoch)
  const first = epochs.length ? Math.min(...epochs) : 0
  const last = epochs.length ? Math.max(...epochs) : 1
  return {
    max, first, last,
    lines: series.value.map((s, index) => ({
      ...s, color: colors[index],
      points: props.items.flatMap(item => {
        const value = item[s.key as keyof Metric]
        return typeof value === 'number' && Number.isFinite(value) ? [{ x: last === first ? 360 : 58 + (item.epoch - first) / (last - first) * 620, y: 188 - value / max * 150, value }] : []
      }),
    })),
    hasValues: values.length > 0,
  }
})
</script>

<template>
  <section class="panel metrics-panel"><div class="panel-heading"><div><h2>训练指标</h2><p>每个 epoch 完成后更新</p></div><el-radio-group v-model="mode" size="small"><el-radio-button value="loss">Loss</el-radio-button><el-radio-button value="map">mAP</el-radio-button></el-radio-group></div>
    <template v-if="plotted.hasValues"><div class="chart-legend"><span v-for="line in plotted.lines" :key="line.key"><i :style="{ background: line.color }" />{{ line.label }}<strong>{{ line.points.at(-1)?.value.toFixed(4) ?? '—' }}</strong></span></div><svg viewBox="0 0 720 225" class="metric-chart" role="img" :aria-label="mode === 'loss' ? '训练损失曲线' : '检测精度曲线'"><g v-for="index in [0, 1, 2, 3]" :key="index"><line x1="58" :y1="38 + index * 50" x2="678" :y2="38 + index * 50" stroke="#e7ecee" stroke-dasharray="4 5" /><text x="48" :y="42 + index * 50" text-anchor="end">{{ (plotted.max * (3 - index) / 3).toFixed(2) }}</text></g><text x="58" y="215">{{ plotted.first }}</text><text x="678" y="215" text-anchor="end">{{ plotted.last }}</text><text x="360" y="215" text-anchor="middle">Epoch</text><g v-for="line in plotted.lines" :key="line.key"><polyline :points="line.points.map(point => `${point.x},${point.y}`).join(' ')" fill="none" :stroke="line.color" stroke-width="2.3" stroke-linejoin="round" /><circle v-for="(point, index) in line.points" :key="index" :cx="point.x" :cy="point.y" r="3.5" :fill="line.color"><title>{{ line.label }}: {{ point.value }}</title></circle></g></svg></template>
    <div v-else class="metrics-empty">{{ items.length ? '当前暂无该类指标' : '暂无训练指标，等待首个 epoch 完成。' }}</div>
  </section>
</template>
