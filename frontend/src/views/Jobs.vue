<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api, dateText, durationText, statusLabels, terminal, type Job, type JobPage, type Status } from '../api'
import AppIcon from '../components/AppIcon.vue'
import StatusBadge from '../components/StatusBadge.vue'

const items = ref<Job[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(10)
const status = ref('')
const search = ref('')
const loading = ref(true)
const error = ref('')
const cancelling = ref<string[]>([])
let request = 0
let disposed = false
let timer: number | undefined
async function load(quiet = false) {
  const current = ++request
  if (!quiet) loading.value = true
  const params = new URLSearchParams({ page: String(page.value), page_size: String(pageSize.value) })
  if (status.value) params.set('status', status.value)
  if (search.value.trim()) params.set('search', search.value.trim())
  try {
    const result = await api<JobPage>(`/jobs?${params}`)
    if (disposed || current !== request) return
    items.value = result.items
    total.value = result.total
    error.value = ''
    if (!items.value.length && page.value > 1 && total.value <= (page.value - 1) * pageSize.value) {
      page.value = Math.max(1, Math.ceil(total.value / pageSize.value))
      void load()
    }
  } catch (e) {
    if (!disposed && current === request) error.value = e instanceof Error ? e.message : '任务查询失败'
  } finally {
    if (!disposed && current === request) loading.value = false
  }
}
function filter() { page.value = 1; void load() }
async function cancel(job: Job) {
  try {
    await ElMessageBox.confirm(job.status === 'pending' ? `取消排队任务“${job.name}”？` : `停止正在运行的任务“${job.name}”？确认后将终止训练。`, '取消任务', { confirmButtonText: '确认取消任务', cancelButtonText: '继续训练', type: 'warning' })
    cancelling.value.push(job.id)
    await api<Job>(`/jobs/${job.id}/cancel`, { method: 'POST' })
    ElMessage.success('已提交取消请求')
    await load(true)
  } catch (e) {
    if (e !== 'cancel' && e !== 'close') ElMessage.error(e instanceof Error ? e.message : '取消失败')
  } finally { cancelling.value = cancelling.value.filter(id => id !== job.id) }
}
onMounted(() => { void load(); timer = window.setInterval(() => load(true), 3000) })
onUnmounted(() => { disposed = true; ++request; window.clearInterval(timer) })
</script>

<template>
  <div class="view-page">
    <div class="page-heading"><div><div class="eyebrow">TRAINING JOBS</div><h1>任务管理 <span class="title-count">{{ total }}</span></h1><p>管理训练生命周期，从排队到完成。</p></div><RouterLink to="/submit" class="primary-link"><AppIcon name="plus" :size="18" />提交训练任务</RouterLink></div>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" class="page-alert" />
    <section class="panel jobs-panel">
      <div class="filter-bar"><div class="filter-fields"><el-input v-model="search" placeholder="搜索任务 ID" clearable class="search-input" aria-label="搜索任务 ID" @keyup.enter="filter" @clear="filter"><template #prefix><AppIcon name="search" :size="17" /></template></el-input><el-select v-model="status" placeholder="全部状态" clearable aria-label="任务状态筛选" style="width: 155px" @change="filter"><el-option v-for="(label, value) in statusLabels" :key="value" :label="label" :value="value as Status" /></el-select><el-button @click="filter">查询</el-button></div><el-button :loading="loading" @click="load()"><AppIcon name="refresh" :size="16" /><span>刷新</span></el-button></div>
      <el-table :data="items" v-loading="loading" empty-text="没有符合条件的任务" class="jobs-table">
        <el-table-column label="任务" min-width="225"><template #default="{ row }"><RouterLink :to="`/jobs/${row.id}`" class="job-name">{{ row.name }}</RouterLink><div class="mono secondary-id">{{ row.id }}</div></template></el-table-column>
        <el-table-column label="状态" min-width="120"><template #default="{ row }"><StatusBadge :status="row.status" :cancelling="!!row.stop_reason && !terminal(row.status)" /></template></el-table-column>
        <el-table-column label="CPU / 优先级" min-width="115"><template #default="{ row }"><span>{{ row.cpu }} 核</span><span class="priority-label">P{{ row.priority }}</span></template></el-table-column>
        <el-table-column label="运行时长" min-width="120"><template #default="{ row }">{{ durationText(row) }}</template></el-table-column>
        <el-table-column label="提交时间" min-width="155"><template #default="{ row }">{{ dateText(row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" min-width="185" fixed="right"><template #default="{ row }"><div class="row-actions"><RouterLink :to="`/jobs/${row.id}`" class="text-link">详情</RouterLink><RouterLink :to="`/jobs/${row.id}#logs`" class="text-link">日志</RouterLink><el-button link type="danger" :disabled="terminal(row.status) || !!row.stop_reason" :loading="cancelling.includes(row.id)" @click="cancel(row)">取消</el-button></div></template></el-table-column>
      </el-table>
      <div class="table-bottom"><span class="muted">共 {{ total }} 个任务 · 每 3 秒自动刷新</span><el-pagination v-model:current-page="page" v-model:page-size="pageSize" :total="total" :page-sizes="[10, 20, 50]" layout="sizes, prev, pager, next" background @current-change="load()" @size-change="filter" /></div>
    </section>
    <div class="inline-tip"><AppIcon name="clock" :size="17" /><span>资源不足时任务自动排队；运行任务结束后，平台将自动分配释放的 CPU。</span></div>
  </div>
</template>
