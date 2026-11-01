import { reactive } from 'vue'

export type Status = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled'
export interface ResourceUsage {
  timestamp: number
  cpu_percent: number
  memory_mb: number
  memory_limit_mb: number
  pids: number
}
export interface ResourcePage { items: ResourceUsage[]; next_offset: number; finished: boolean; eof: boolean; latest: ResourceUsage | null }
export interface Job {
  id: string
  name: string
  command: string
  cpu: number
  memory_mb: number
  timeout_seconds: number
  priority: number
  effective_priority: number
  queue_wait_seconds: number
  backfilled: boolean
  resource_usage: ResourceUsage | null
  status: Status
  created_at: string
  started_at: string | null
  finished_at: string | null
  heartbeat_at: string | null
  exit_code: number | null
  error: string | null
  ray_submission_id: string | null
  cancel_requested?: boolean
  stop_reason?: string | null
}
export interface Cluster {
  total_cpus: number
  allocated_cpus: number
  available_cpus: number
  total_memory_mb: number
  allocated_memory_mb: number
  available_memory_mb: number
  aging_interval_seconds: number
  backfill_max_bypass: number
  pending_jobs: number
  running_jobs: number
  redis_ok: boolean
  ray_ok: boolean
  nodes: { id: string; name: string; total_cpus: number; allocated_cpus: number; cpu_percent: number; memory_percent: number }[]
}
export interface JobPage { items: Job[]; total: number; page: number; page_size: number }
export interface LogChunk { content: string; next_offset: number; eof: boolean; finished: boolean }
export interface Metric { epoch: number; box_loss?: number | null; cls_loss?: number | null; dfl_loss?: number | null; map50?: number | null; map50_95?: number | null }

export const defaultCommand = 'python -m app.train --epochs 1 --imgsz 320 --batch 1'
export const statusLabels: Record<Status, string> = {
  pending: '排队中', running: '运行中', succeeded: '已成功', failed: '已失败', cancelled: '已取消',
}
export const terminal = (status: Status) => ['succeeded', 'failed', 'cancelled'].includes(status)

const tokenKey = 'trainplane.access-token'
let accessToken = sessionStorage.getItem(tokenKey) || ''
export const session = reactive({ authenticated: false, username: '', checking: Boolean(accessToken) })

export function signOut() {
  accessToken = ''
  sessionStorage.removeItem(tokenKey)
  session.authenticated = false
  session.username = ''
}

interface SessionResult { authenticated: true; username: string }

export async function signIn(token: string) {
  const value = token.trim()
  if (!value) throw new Error('请输入访问令牌')
  const result = await api<SessionResult>('/auth/session', { headers: { Authorization: `Bearer ${value}` } })
  accessToken = value
  sessionStorage.setItem(tokenKey, value)
  session.username = result.username
  session.authenticated = true
}

export async function restoreSession() {
  if (!accessToken) return
  session.checking = true
  try {
    const result = await api<SessionResult>('/auth/session')
    session.username = result.username
    session.authenticated = true
  } finally { session.checking = false }
}

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), 15000)
  try {
    const headers = new Headers(options.headers)
    headers.set('Content-Type', 'application/json')
    if (accessToken && !headers.has('Authorization')) headers.set('Authorization', `Bearer ${accessToken}`)
    const response = await fetch(`/api${path}`, {
      ...options,
      signal: controller.signal,
      headers,
    })
    if (response.status === 401) {
      // A late response from an older session must not clear a newly verified token.
      if (!session.authenticated || headers.get('Authorization') === `Bearer ${accessToken}`) signOut()
      throw new Error('访问令牌无效或已失效，请重新登录')
    }
    if (!response.ok) {
      const data = await response.json().catch(() => null)
      const detail = data?.detail
      const message = typeof detail === 'string' ? detail
        : Array.isArray(detail) ? detail.map(item => item.msg).join('；') : `请求失败（${response.status}）`
      throw new Error(message)
    }
    return await response.json() as T
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw new Error('请求超时，请检查后端服务')
    if (error instanceof TypeError) throw new Error('无法连接后端，请检查服务是否启动')
    throw error
  } finally {
    window.clearTimeout(timer)
  }
}

export function dateText(value?: string | null) {
  if (!value) return '—'
  const normalized = /(?:Z|[+-]\d\d:\d\d)$/i.test(value) ? value : `${value}Z`
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(date)
}

export function durationText(job: Job) {
  if (!job.started_at) return '—'
  const start = new Date(/(?:Z|[+-]\d\d:\d\d)$/i.test(job.started_at) ? job.started_at : `${job.started_at}Z`).getTime()
  const end = job.finished_at ? new Date(/(?:Z|[+-]\d\d:\d\d)$/i.test(job.finished_at) ? job.finished_at : `${job.finished_at}Z`).getTime() : Date.now()
  const seconds = Math.max(0, Math.floor((end - start) / 1000))
  if (!Number.isFinite(seconds)) return '—'
  return seconds < 60 ? `${seconds} 秒` : `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`
}

export function waitText(seconds: number) {
  const value = Math.max(0, Math.floor(seconds || 0))
  if (value < 60) return `${value} 秒`
  if (value < 3600) return `${Math.floor(value / 60)} 分 ${value % 60} 秒`
  return `${Math.floor(value / 3600)} 时 ${Math.floor(value % 3600 / 60)} 分`
}

export function memoryText(value?: number | null) {
  return typeof value === 'number' && Number.isFinite(value) ? `${value.toLocaleString('zh-CN', { maximumFractionDigits: 1 })} MiB` : '—'
}
