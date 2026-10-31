export type Status = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled'
export interface Job {
  id: string
  name: string
  command: string
  cpu: number
  timeout_seconds: number
  priority: number
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

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), 15000)
  try {
    const response = await fetch(`/api${path}`, {
      ...options,
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json', ...options.headers },
    })
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
