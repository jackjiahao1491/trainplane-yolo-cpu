import assert from 'node:assert/strict'
import test from 'node:test'
import { frontendLoader } from './load-typescript.mjs'

const encoder = new TextEncoder()
const event = (name, content, offset, finished = false, newline = '\n') => [
  `event: ${name}`, `id: ${offset}`, `data: ${JSON.stringify({ content, next_offset: offset, eof: finished, finished })}`, '', '',
].join(newline)

function fixture(t) {
  const storage = new Map()
  Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
    getItem: key => storage.get(key) ?? null,
    setItem: (key, value) => storage.set(key, value),
    removeItem: key => storage.delete(key),
  } })
  const previousWindow = globalThis.window
  globalThis.window = { setTimeout, clearTimeout }
  const previousFetch = globalThis.fetch
  t.after(() => { globalThis.fetch = previousFetch; globalThis.window = previousWindow })
  const load = frontendLoader()
  return { ...load('../src/sse.ts'), storage, api: load('../src/api.ts') }
}

function sseResponse(chunks, signal, keepOpen = false) {
  let source
  const body = new ReadableStream({
    start(controller) {
      source = controller
      for (const chunk of chunks) controller.enqueue(typeof chunk === 'string' ? encoder.encode(chunk) : chunk)
      if (!keepOpen) controller.close()
      else signal?.addEventListener('abort', () => controller.error(new DOMException('Aborted', 'AbortError')), { once: true })
    },
  })
  return { response: new Response(body, { headers: { 'Content-Type': 'text/event-stream; charset=utf-8' } }), source }
}

async function settle() { for (let index = 0; index < 25; index++) await Promise.resolve() }

test('SSE frames support BOM, CRLF split across chunks, comments, retry and multiline data', t => {
  const { SSEParser } = fixture(t)
  const parser = new SSEParser()
  const text = '\uFEFFretry:1000\r\n\r\n: heartbeat\r\nevent: log\r\nid: 6\r\ndata: {"content":"中文",\r\ndata: "next_offset":6,"eof":false,"finished":false}\r\n\r\n'
  const frames = [...text].flatMap(character => parser.push(character))
  assert.equal(parser.retry, 1000)
  assert.equal(frames.length, 1)
  assert.equal(frames[0].event, 'log')
  assert.equal(frames[0].id, '6')
  assert.equal(JSON.parse(frames[0].data).content, '中文')
  assert.deepEqual(parser.push('event: log\nid: 99\ndata: {"content":"incomplete"}'), [])
})

test('fetch SSE decodes each UTF-8 byte and consumes final end content exactly once', async t => {
  const { followJobLogs } = fixture(t)
  const text = ': pulse\r\n\r\n' + event('log', '中文🙂', 10, false, '\r\n') + event('end', '完成', 16, true)
  const bytes = encoder.encode(text)
  const chunks = [...bytes].map(byte => new Uint8Array([byte]))
  globalThis.fetch = async () => sseResponse(chunks).response
  const content = [], states = []
  await followJobLogs('job', { signal: new AbortController().signal, onChunk: value => content.push(value), onState: value => states.push(value) })
  assert.equal(content.map(value => value.content).join(''), '中文🙂完成')
  assert.deepEqual(content.map(value => value.next_offset), [10, 16])
  assert.equal(states.at(-1).phase, 'finished')
})

test('disconnect resumes consumed byte cursor, discards partial frames and deduplicates replay', async t => {
  const { followJobLogs } = fixture(t)
  const calls = [], content = [], headers = []
  globalThis.fetch = async (url, options) => {
    calls.push(url); headers.push(options.headers)
    if (calls.length === 1) {
      const partial = encoder.encode('event: log\nid: 12\ndata: {"content":"世')
      return sseResponse([event('log', '你好', 6), partial.slice(0, -1)]).response
    }
    return sseResponse([event('log', '你好', 6), event('log', '世界', 12), event('end', '', 12, true)]).response
  }
  await followJobLogs('job', { signal: new AbortController().signal, onChunk: chunk => content.push(chunk.content), onState() {} })
  assert.deepEqual(calls, ['/api/jobs/job/logs/stream?offset=0', '/api/jobs/job/logs/stream?offset=6'])
  assert.deepEqual(content, ['你好', '世界'])
  assert.equal(headers[0].has('Last-Event-ID'), false)
  assert.equal(headers[1].get('Last-Event-ID'), '6')
})

test('server error and invalid event cursors stop without blind retries', async t => {
  const { followJobLogs } = fixture(t)
  let calls = 0
  globalThis.fetch = async () => { calls++; return sseResponse(['event: error\ndata: {"code":"log_truncated","message":"日志已截断，请从头读取"}\n\n']).response }
  const states = []
  await followJobLogs('job', { signal: new AbortController().signal, offset: 42, onChunk() { assert.fail('No log should be consumed') }, onState: state => states.push(state) })
  assert.equal(calls, 1)
  assert.equal(states.at(-1).phase, 'error')
  assert.equal(states.at(-1).code, 'log_truncated')
  globalThis.fetch = async () => sseResponse(['event: log\nid: 10\ndata: {"content":"bad","next_offset":11,"eof":false,"finished":false}\n\n']).response
  await followJobLogs('job', { signal: new AbortController().signal, onChunk() { assert.fail('Bad cursor must not be consumed') }, onState: state => states.push(state) })
  assert.equal(states.at(-1).code, 'stream_protocol')
})

test('empty end completes without reading a later event', async t => {
  const { followJobLogs } = fixture(t)
  globalThis.fetch = async () => sseResponse([event('end', '', 0, true) + event('log', 'late', 4)]).response
  const states = []
  await followJobLogs('job', { signal: new AbortController().signal, onChunk() { assert.fail('End is empty') }, onState: state => states.push(state) })
  assert.equal(states.at(-1).phase, 'finished')
})

test('invalid wire UTF-8 stops as a protocol error instead of retrying corrupt data', async t => {
  const { followJobLogs } = fixture(t)
  let calls = 0
  globalThis.fetch = async () => { calls++; return sseResponse([new Uint8Array([0xff])]).response }
  const states = []
  await followJobLogs('job', { signal: new AbortController().signal, onChunk() { assert.fail('Corrupt frame must not be consumed') }, onState: state => states.push(state) })
  assert.equal(calls, 1)
  assert.equal(states.at(-1).code, 'stream_protocol')
})

test('route/unmount abort closes the pending reader and suppresses retries and late chunks', async t => {
  const { followJobLogs } = fixture(t)
  const route = new AbortController()
  let connectionSignal, calls = 0
  globalThis.fetch = async (_url, options) => {
    calls++; connectionSignal = options.signal
    return sseResponse([], options.signal, true).response
  }
  const states = []
  const done = followJobLogs('old-job', { signal: route.signal, onChunk() { assert.fail('Old route must not receive chunks') }, onState: state => states.push(state) })
  await settle()
  assert.equal(states.at(-1).phase, 'connected')
  route.abort()
  await done
  assert.equal(connectionSignal.aborted, true)
  assert.equal(calls, 1)
  assert.equal(states.some(state => state.phase === 'reconnecting'), false)
})

test('transient failures back off 1/2/4/8/10 seconds and abort cancels the retry timer', async t => {
  const { followJobLogs } = fixture(t)
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const controller = new AbortController(), delays = []
  let calls = 0
  globalThis.fetch = async () => { calls++; throw new TypeError('network down') }
  const done = followJobLogs('job', { signal: controller.signal, onChunk() {}, onState: state => { if (state.retryInMs) delays.push(state.retryInMs) } })
  await settle()
  for (const delay of [1000, 2000, 4000, 8000]) { t.mock.timers.tick(delay); await settle() }
  assert.deepEqual(delays, [1000, 2000, 4000, 8000, 10000])
  controller.abort()
  await done
  t.mock.timers.tick(100000)
  await settle()
  assert.equal(calls, 5)
})

test('any incoming bytes including comment heartbeats reset the 30-second idle watchdog', async t => {
  const { followJobLogs } = fixture(t)
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const controller = new AbortController(), states = []
  let source, connectionSignal
  globalThis.fetch = async (_url, options) => {
    connectionSignal = options.signal
    const stream = sseResponse([], options.signal, true)
    source = stream.source
    return stream.response
  }
  const done = followJobLogs('job', { signal: controller.signal, onChunk() {}, onState: state => states.push(state) })
  await settle()
  t.mock.timers.tick(20000)
  source.enqueue(encoder.encode(': heartbeat\n\n'))
  await settle()
  t.mock.timers.tick(20000)
  await settle()
  assert.equal(connectionSignal.aborted, false)
  t.mock.timers.tick(10000)
  await settle()
  assert.equal(connectionSignal.aborted, true)
  assert.equal(states.at(-1).phase, 'reconnecting')
  assert.match(states.at(-1).message, /30秒/)
  controller.abort()
  await done
})

test('SSE sends Bearer only in headers and a 401 clears the same API session', async t => {
  const { followJobLogs, api: apiModule } = fixture(t)
  globalThis.fetch = async () => new Response(JSON.stringify({ authenticated: true, username: 'operator' }), { status: 200 })
  await apiModule.signIn('sse-unit-test-token')
  let request
  globalThis.fetch = async (url, options) => { request = { url, options }; return new Response('{}', { status: 401 }) }
  const states = []
  await followJobLogs('job', { signal: new AbortController().signal, onChunk() {}, onState: state => states.push(state) })
  assert.equal(request.options.headers.get('Authorization'), 'Bearer sse-unit-test-token')
  assert.equal(request.url, '/api/jobs/job/logs/stream?offset=0')
  assert.equal(apiModule.session.authenticated, false)
  assert.equal(sessionStorage.getItem('trainplane.access-token'), null)
  assert.equal(states.at(-1).code, 'http_401')
})

test('HTTP416 reports a recoverable cursor error without automatic retries', async t => {
  const { followJobLogs } = fixture(t)
  let calls = 0
  const states = []
  globalThis.fetch = async () => { calls++; return new Response(JSON.stringify({ detail: 'offset 超过日志大小，请从0读取' }), { status: 416 }) }
  await followJobLogs('job', { signal: new AbortController().signal, offset: 123, onChunk() { assert.fail('Invalid offset must not consume logs') }, onState: state => states.push(state) })
  assert.equal(calls, 1)
  assert.equal(states.at(-1).code, 'http_416')
  assert.equal(states.at(-1).phase, 'error')
})
