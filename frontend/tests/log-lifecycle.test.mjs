import assert from 'node:assert/strict'
import test from 'node:test'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { parse, compileScript } from '@vue/compiler-sfc'
import ts from 'typescript'

test('JobDetail aborts its SSE on task change, logout and unmount', async () => {
  const source = readFileSync(new URL('../src/views/JobDetail.vue', import.meta.url), 'utf8')
  const { descriptor } = parse(source)
  const script = compileScript(descriptor, { id: 'log-lifecycle-check' })
  const compiled = ts.transpileModule(script.content, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText
  const require = createRequire(import.meta.url)
  const vue = require('vue')
  const watchers = [], unmounts = [], streams = []
  const route = { params: { id: 'first-task' }, hash: '' }
  const session = { authenticated: true }
  const previousWindow = globalThis.window
  globalThis.window = { setTimeout, clearTimeout, setInterval, clearInterval }
  const module = { exports: {} }
  const fakeApi = {
    session,
    api: async path => path.includes('/resources') ? { items: [], next_offset: 0, latest: null, finished: false, eof: true }
      : path.includes('/metrics') ? { items: [] } : { status: 'running', resource_usage: null },
    terminal: status => ['succeeded', 'failed', 'cancelled'].includes(status),
  }
  function resolve(specifier) {
    if (specifier === 'vue') return {
      ...vue,
      onMounted() {},
      onUnmounted: callback => unmounts.push(callback),
      watch: (getter, callback, options) => {
        watchers.push({ getter, callback, options })
        if (options?.immediate) callback(getter())
      },
    }
    if (specifier === 'vue-router') return { useRoute: () => route }
    if (specifier === '../api') return fakeApi
    if (specifier === '../sse') return { followJobLogs: (id, options) => { streams.push({ id, ...options }); return Promise.resolve() } }
    if (specifier === 'element-plus' || specifier.endsWith('.vue')) return {}
    return require(specifier)
  }
  try {
    new Function('require', 'module', 'exports', compiled)(resolve, module, module.exports)
    module.exports.default.setup({}, { expose() {} })
    assert.equal(streams.length, 1)
    assert.equal(streams[0].id, 'first-task')
    route.params.id = 'second-task'
    watchers[0].callback(route.params.id)
    assert.equal(streams[0].signal.aborted, true)
    assert.equal(streams[1].id, 'second-task')
    session.authenticated = false
    watchers[1].callback(false)
    assert.equal(streams[1].signal.aborted, true)
    assert.equal(watchers[1].options.flush, 'sync')
    session.authenticated = true
    route.params.id = 'third-task'
    watchers[0].callback(route.params.id)
    unmounts.forEach(callback => callback())
    assert.equal(streams[2].signal.aborted, true)
    await Promise.resolve()
  } finally { globalThis.window = previousWindow }
})
