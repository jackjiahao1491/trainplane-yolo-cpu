import { ApiError, checkResponse, requestHeaders, type LogChunk } from './api'

export interface SSEFrame { event: string; data: string; id?: string }
export interface LogStreamState {
  phase: 'connecting' | 'connected' | 'reconnecting' | 'finished' | 'error'
  message?: string
  code?: string
  retryInMs?: number
}

class LogStreamError extends Error {
  constructor(public code: string, message: string) { super(message) }
}

/** A frame is returned only after its blank line, never at an interrupted EOF. */
export class SSEParser {
  retry = 1000
  private line = ''
  private skipLF = false
  private firstCharacter = true
  private event = ''
  private id: string | undefined
  private data: string[] = []
  private dataLength = 0

  push(text: string): SSEFrame[] {
    const frames: SSEFrame[] = []
    for (const character of text) {
      if (this.firstCharacter) {
        this.firstCharacter = false
        if (character === '\uFEFF') continue
      }
      if (this.skipLF) {
        this.skipLF = false
        if (character === '\n') continue
      }
      if (character === '\r' || character === '\n') {
        this.readLine(this.line, frames)
        this.line = ''
        this.skipLF = character === '\r'
      } else {
        this.line += character
        if (this.line.length + this.dataLength > 1048576) throw new LogStreamError('stream_protocol', '日志流事件超过大小限制，请手动重连')
      }
    }
    return frames
  }

  private readLine(line: string, frames: SSEFrame[]) {
    if (!line) {
      if (this.data.length) frames.push({ event: this.event || 'message', data: this.data.join('\n'), id: this.id })
      this.event = ''
      this.id = undefined
      this.data = []
      this.dataLength = 0
      return
    }
    if (line.startsWith(':')) return
    const colon = line.indexOf(':')
    const field = colon < 0 ? line : line.slice(0, colon)
    let value = colon < 0 ? '' : line.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'event') this.event = value
    else if (field === 'id' && !value.includes('\0')) this.id = value
    else if (field === 'data') { this.data.push(value); this.dataLength += value.length + 1 }
    else if (field === 'retry' && /^\d+$/.test(value)) this.retry = Math.max(1000, Math.min(10000, Number(value)))
  }
}

function logChunk(frame: SSEFrame): LogChunk {
  let chunk: LogChunk
  try { chunk = JSON.parse(frame.data) }
  catch { throw new LogStreamError('stream_protocol', '日志流数据格式错误，请手动重连') }
  if (!chunk || typeof chunk.content !== 'string' || !Number.isSafeInteger(chunk.next_offset) || chunk.next_offset < 0
      || typeof chunk.eof !== 'boolean' || typeof chunk.finished !== 'boolean'
      || (frame.id !== undefined && (!/^\d+$/.test(frame.id) || Number(frame.id) !== chunk.next_offset))) {
    throw new LogStreamError('stream_protocol', '日志流游标或字段无效，请手动重连')
  }
  if (frame.event === 'end' && !(chunk.finished && chunk.eof)) throw new LogStreamError('stream_protocol', '日志流结束标志无效，请手动重连')
  return chunk
}

function waitToRetry(delay: number, signal: AbortSignal) {
  return new Promise<void>(resolve => {
    if (signal.aborted) { resolve(); return }
    const done = () => { clearTimeout(timer); signal.removeEventListener('abort', done); resolve() }
    const timer = setTimeout(done, delay)
    signal.addEventListener('abort', done, { once: true })
  })
}

/** Authenticated, abortable stream. Only consumed complete frames advance cursor. */
export async function followJobLogs(jobId: string, options: {
  signal: AbortSignal
  offset?: number
  onChunk: (chunk: LogChunk) => void | Promise<void>
  onState: (state: LogStreamState) => void
}) {
  let cursor = options.offset ?? 0
  let failures = 0
  let retry = 1000
  let hasConnected = false
  while (!options.signal.aborted) {
    options.onState({ phase: hasConnected || failures ? 'reconnecting' : 'connecting' })
    const connection = new AbortController()
    const abort = () => connection.abort()
    options.signal.addEventListener('abort', abort, { once: true })
    let idle = false
    let watchdog: ReturnType<typeof setTimeout> | undefined
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
    let delay = 0
    const connectedAt = Date.now()
    const resetWatchdog = () => {
      clearTimeout(watchdog)
      watchdog = setTimeout(() => { idle = true; connection.abort() }, 30000)
    }
    resetWatchdog()
    try {
      if (options.signal.aborted) return
      const headers = requestHeaders({ Accept: 'text/event-stream' })
      if (cursor > 0) headers.set('Last-Event-ID', String(cursor))
      const response = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/logs/stream?offset=${cursor}`, { headers, signal: connection.signal, cache: 'no-store' })
      await checkResponse(response, headers)
      if (options.signal.aborted) return
      if (!response.body || response.headers.get('Content-Type')?.split(';')[0]?.trim().toLowerCase() !== 'text/event-stream') {
        throw new LogStreamError('stream_protocol', '服务器未返回日志事件流，请检查服务后手动重连')
      }
      reader = response.body.getReader()
      const parser = new SSEParser()
      const decoder = new TextDecoder('utf-8', { fatal: true })
      hasConnected = true
      options.onState({ phase: 'connected' })
      while (!options.signal.aborted) {
        const { value, done } = await reader.read()
        if (options.signal.aborted) return
        if (done) throw new Error('日志流意外断开')
        if (!value?.byteLength) continue
        resetWatchdog()
        let text: string
        try { text = decoder.decode(value, { stream: true }) }
        catch { throw new LogStreamError('stream_protocol', '日志流包含无效UTF-8数据，请检查服务后手动重连') }
        const frames = parser.push(text)
        retry = parser.retry
        for (const frame of frames) {
          if (options.signal.aborted) return
          if (frame.event === 'error') {
            let problem: { code?: string; message?: string }
            try { problem = JSON.parse(frame.data) }
            catch { throw new LogStreamError('stream_protocol', '日志流错误事件格式无效，请手动重连') }
            throw new LogStreamError(typeof problem?.code === 'string' ? problem.code : 'stream_error', typeof problem?.message === 'string' ? problem.message : '日志流已停止，请检查任务后手动重连')
          }
          if (frame.event !== 'log' && frame.event !== 'end') continue
          const chunk = logChunk(frame)
          if (chunk.next_offset < cursor) throw new LogStreamError('stream_protocol', '日志游标发生回退，请从头重新读取')
          if (chunk.next_offset > cursor) {
            await options.onChunk(chunk)
            cursor = chunk.next_offset
            failures = 0
          }
          if (frame.event === 'end') {
            options.onState({ phase: 'finished' })
            return
          }
        }
      }
    } catch (error) {
      if (options.signal.aborted) return
      if (error instanceof LogStreamError || (error instanceof ApiError && error.status >= 400 && error.status < 500)) {
        options.onState({ phase: 'error', code: error instanceof LogStreamError ? error.code : `http_${error.status}`, message: error.message })
        return
      }
      if (Date.now() - connectedAt >= 10000) failures = 0
      delay = Math.min(10000, retry * 2 ** Math.min(failures++, 4))
      options.onState({ phase: 'reconnecting', retryInMs: delay, message: idle ? '30秒未收到日志流数据，连接可能已失效' : '日志连接中断，正在从已读取位置重连' })
    } finally {
      clearTimeout(watchdog)
      options.signal.removeEventListener('abort', abort)
      connection.abort()
      if (reader) { try { await reader.cancel() } catch { /* Already aborted or closed. */ } }
    }
    if (delay) await waitToRetry(delay, options.signal)
  }
}
