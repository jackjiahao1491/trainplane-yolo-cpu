<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { api, defaultCommand, memoryText, type Cluster, type Job } from '../api'
import AppIcon from '../components/AppIcon.vue'

const router = useRouter()
const formRef = ref<FormInstance>()
const submitting = ref(false)
const cluster = ref<Cluster | null>(null)
const environmentError = ref('')
const error = ref('')
const form = reactive({ name: '', command: defaultCommand, cpu: 2, memory_mb: 1024, timeout_seconds: 1800, priority: 0 })
const rules: FormRules = {
  name: [{ required: true, whitespace: true, message: '请输入任务名称', trigger: 'blur' }, { max: 100, message: '任务名称不能超过 100 个字符', trigger: 'blur' }],
  command: [{ required: true, whitespace: true, message: '请输入训练命令', trigger: 'blur' }, { max: 4096, message: '命令不能超过 4096 个字符', trigger: 'blur' }],
  cpu: [{ required: true, type: 'number', min: 1, message: '至少申请 1 核 CPU', trigger: 'change' }],
  memory_mb: [{ required: true, type: 'number', min: 64, message: '内存限制至少为 64 MiB', trigger: 'change' }, { validator: (_rule, value, callback) => {
    if (!Number.isInteger(value)) return callback(new Error('内存限制必须为整数'))
    if (cluster.value && value > cluster.value.total_memory_mb) return callback(new Error('内存限制不能超过集群总配额'))
    callback()
  }, trigger: 'change' }],
  timeout_seconds: [{ required: true, type: 'number', min: 1, max: 86400, message: '运行时限应为 1～86400 秒', trigger: 'change' }],
}
const willQueue = computed(() => cluster.value && (form.cpu > cluster.value.available_cpus || form.memory_mb > cluster.value.available_memory_mb))
async function submit() {
  if (!await formRef.value?.validate().catch(() => false)) return
  submitting.value = true
  error.value = ''
  try {
    await api<Job>('/jobs', { method: 'POST', body: JSON.stringify({ ...form, name: form.name.trim(), command: form.command.trim() }) })
    ElMessage.success('训练任务已提交')
    await router.push('/jobs')
  } catch (e) { error.value = e instanceof Error ? e.message : '提交失败，请重试' }
  finally { submitting.value = false }
}
onMounted(async () => {
  try {
    cluster.value = await api<Cluster>('/cluster')
    form.cpu = Math.min(form.cpu, cluster.value.total_cpus)
    form.memory_mb = Math.min(form.memory_mb, cluster.value.total_memory_mb)
  } catch (e) { environmentError.value = e instanceof Error ? e.message : '无法读取资源信息' }
})
</script>

<template>
  <div class="view-page">
    <div class="page-heading"><div><div class="eyebrow">NEW TRAINING JOB</div><h1>提交训练任务</h1><p>配置训练参数，其余交给平台。</p></div><RouterLink to="/jobs" class="secondary-link"><AppIcon name="back" :size="17" />返回任务列表</RouterLink></div>
    <el-alert v-if="environmentError" :title="environmentError" type="warning" show-icon :closable="false" class="page-alert" />
    <div class="submit-layout">
      <section class="panel submit-form-panel"><div class="panel-heading"><div><h2>任务配置</h2><p>默认使用 YOLO 小模型和 coco128，适合 CPU 演示</p></div><span class="subtle-badge">CPU ONLY</span></div>
        <el-form ref="formRef" :model="form" :rules="rules" label-position="top" class="training-form" novalidate @submit.prevent="submit">
          <el-form-item label="任务名称" prop="name"><el-input v-model="form.name" placeholder="例如：YOLO CPU 基线训练" maxlength="100" show-word-limit /></el-form-item>
          <el-form-item prop="command"><template #label><div class="form-label-row"><span>训练命令</span><el-button link type="primary" @click="form.command = defaultCommand">恢复默认命令</el-button></div></template><el-input v-model="form.command" type="textarea" :rows="4" resize="vertical" class="command-input" aria-label="训练命令" /><div class="field-help">默认入口固定使用 CPU 和 coco128；可调整 epoch、图片尺寸与 batch。</div></el-form-item>
          <div class="form-grid"><el-form-item label="申请 CPU" prop="cpu"><div class="number-with-unit"><el-input-number v-model="form.cpu" :min="1" :max="cluster?.total_cpus || 64" :step="1" :precision="0" controls-position="right" /><span>核</span></div><div class="field-help">平台总配额 {{ cluster?.total_cpus ?? '—' }} 核</div></el-form-item><el-form-item label="最大运行时长" prop="timeout_seconds"><div class="number-with-unit"><el-input-number v-model="form.timeout_seconds" :min="1" :max="86400" :step="1" :precision="0" controls-position="right" /><span>秒</span></div><div class="field-help">从实际开始运行计时，超时自动终止</div></el-form-item></div>
          <el-form-item label="内存限制" prop="memory_mb"><div class="number-with-unit"><el-input-number v-model="form.memory_mb" :min="64" :max="cluster?.total_memory_mb || 65536" :step="64" :precision="0" controls-position="right" /><span>MiB</span></div><div class="field-help">同时用于调度预留与训练进程内存限制；平台总配额 {{ memoryText(cluster?.total_memory_mb) }}。</div></el-form-item>
          <el-form-item label="任务优先级" prop="priority"><div class="priority-form"><el-input-number v-model="form.priority" :min="0" :max="10" :precision="0" controls-position="right" /><span class="field-help">0～10，数值越高越优先；等待老化会提高有效优先级，不抢占运行任务。</span></div></el-form-item>
          <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" class="page-alert" />
          <div class="form-actions"><span><AppIcon name="check" :size="16" />任务配置与日志将持久保存</span><el-button type="primary" size="large" native-type="submit" :loading="submitting"><AppIcon name="play" :size="17" />提交训练任务</el-button></div>
        </el-form>
      </section>
      <aside class="submit-aside"><section class="panel resource-snapshot"><span class="aside-label">RESOURCE SNAPSHOT</span><h2>当前可用资源</h2><div class="snapshot-number">{{ cluster?.available_cpus ?? '—' }}<span>/ {{ cluster?.total_cpus ?? '—' }} 核</span></div><el-progress :percentage="cluster?.total_cpus ? Math.round(cluster.available_cpus / cluster.total_cpus * 100) : 0" :show-text="false" :stroke-width="7" /><div class="snapshot-memory"><span>可用内存配额</span><strong>{{ memoryText(cluster?.available_memory_mb) }}</strong><small>共 {{ memoryText(cluster?.total_memory_mb) }}</small></div><div :class="['availability-note', { waiting: willQueue }]"><span class="status-dot" />{{ cluster ? willQueue ? '当前资源不足，提交后将进入排队' : '当前资源可满足本次申请' : '等待获取资源信息' }}</div><p>资源情况可能随其他任务变化，最终以实际调度结果为准。</p></section>
        <section class="training-guide"><span class="guide-icon"><AppIcon name="book" :size="22" /></span><h3>轻量训练建议</h3><p>首次运行建议从小规模训练开始，验证完整流程后再增加训练轮数。</p><dl><div><dt>数据集</dt><dd>coco128</dd></div><div><dt>默认轮数</dt><dd>1 epoch</dd></div><div><dt>图片尺寸</dt><dd>320 × 320</dd></div><div><dt>Batch size</dt><dd>1</dd></div></dl><div class="guide-foot">模型与数据提前准备，可减少演示时的下载等待。</div></section>
      </aside>
    </div>
  </div>
</template>
